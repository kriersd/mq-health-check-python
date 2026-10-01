#!/usr/bin/env python3
"""
mq-health-agent: per-host health aggregator for IBM MQ behind an F5 BIG-IP.

Runs on each MQ host. A background loop runs a set of checks against the
local queue manager (mqweb REST API, local files, and optionally a real MQ
client connection) and caches the verdict. The F5 polls GET /health:

    200  UP        all member-scope checks pass
    200  DEGRADED  a WARN, or a FAIL in a pool-scope check (alert, don't fail over)
    503  DOWN      a member-scope check failed -> F5 marks this pool member down
    503  STALE     the check loop has not produced a fresh result

Run `python3 mq_health_agent.py --config config.yaml --once` to evaluate once
and print the result (exit 0 = UP, 1 = DEGRADED, 2 = DOWN).
"""
import argparse
import ipaddress
import json
import logging
import os
import re
import shutil
import socket
import ssl
import subprocess
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import quote

import requests
import yaml

try:
    import pymqi  # only needed for the client_connect check
except ImportError:  # pragma: no cover
    pymqi = None

OK, WARN, FAIL, SKIP = "OK", "WARN", "FAIL", "SKIP"
MEMBER, POOL = "member", "pool"
UP, DEGRADED, DOWN, STALE = "UP", "DEGRADED", "DOWN", "STALE"

# Default scope per check. "member" failures take this host out of the F5 pool;
# "pool" failures usually affect every member alike, so they only alert.
DEFAULT_SCOPE = {
    "qmgr_status": MEMBER,
    "listener": MEMBER,
    "svrconn_channel": MEMBER,
    "max_channels": MEMBER,
    "chlauth": MEMBER,
    "log_usage": MEMBER,
    "put_get": MEMBER,
    "disk_space": MEMBER,
    "tls_cert": MEMBER,
    "client_connect": MEMBER,
    "app_queues": POOL,
    "ldap": POOL,
}

# MQSC reason codes that mean "nothing to display" rather than an error.
NOT_FOUND_REASONS = {2085, 3065, 4031}

log = logging.getLogger("mq-health-agent")


class RestUnavailable(Exception):
    """mqweb could not be reached or rejected the monitor credentials."""


class CheckError(Exception):
    """A check could not be evaluated for a reason other than REST availability."""


def up(value):
    return str(value).strip().upper()


def as_int(value, default=None):
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return default


# --------------------------------------------------------------------------- REST


class MQRest:
    def __init__(self, rest_cfg, qmgr):
        self.base = rest_cfg["url"].rstrip("/") + "/ibmmq/rest/" + rest_cfg.get("api_version", "v2")
        self.qm = qmgr
        self.timeout = rest_cfg.get("timeout_seconds", 5)
        self.session = requests.Session()
        password = os.environ.get(rest_cfg.get("password_env", "MQHEALTH_REST_PASSWORD"), "")
        self.session.auth = (rest_cfg["user"], password)
        self.session.verify = rest_cfg.get("ca_bundle", True)
        # Required by mqweb on POST/PATCH/DELETE; any value is accepted.
        self.session.headers["ibm-mq-rest-csrf-token"] = "mq-health-agent"
        self._lock = threading.Lock()

    def _request(self, method, path, **kwargs):
        try:
            resp = self.session.request(method, self.base + path, timeout=self.timeout, **kwargs)
        except requests.RequestException as exc:
            raise RestUnavailable(f"{type(exc).__name__}: {exc}") from exc
        if resp.status_code in (401, 403):
            raise RestUnavailable(f"HTTP {resp.status_code} from mqweb - check the monitor user's roles")
        if resp.status_code >= 500 and resp.status_code != 503:
            raise RestUnavailable(f"HTTP {resp.status_code} from mqweb")
        return resp

    def qmgr_state(self):
        resp = self._request("GET", f"/admin/qmgr/{quote(self.qm)}")
        if resp.status_code == 404:
            return "not found"
        if resp.status_code != 200:
            raise RestUnavailable(f"HTTP {resp.status_code} from mqweb: {resp.text[:200]}")
        return resp.json()["qmgr"][0].get("state", "unknown")

    def mqsc(self, qualifier, name=None, params=None, response=("all",)):
        """Run a DISPLAY command via runCommandJSON. Returns a list of parameter
        dicts (lower-cased keys). Raises CheckError on a real command failure."""
        body = {
            "type": "runCommandJSON",
            "command": "display",
            "qualifier": qualifier,
            "responseParameters": list(response),
        }
        if name is not None:
            body["name"] = name
        if params:
            body["parameters"] = params
        resp = self._request("POST", f"/admin/action/qmgr/{quote(self.qm)}/mqsc", json=body)
        if resp.status_code == 503:
            raise RestUnavailable("mqweb reports the queue manager is unavailable (HTTP 503)")
        if resp.status_code != 200:
            raise CheckError(f"DISPLAY {qualifier.upper()} returned HTTP {resp.status_code}: {resp.text[:200]}")
        records, errors = [], []
        for item in resp.json().get("commandResponse", []):
            if item.get("completionCode", 0) == 0:
                if "parameters" in item:
                    records.append({k.lower(): v for k, v in item["parameters"].items()})
            elif as_int(item.get("reasonCode")) not in NOT_FOUND_REASONS:
                errors.append(item)
        if errors and not records:
            first = errors[0]
            msg = " ".join(first.get("message", [])) or f"reason {first.get('reasonCode')}"
            raise CheckError(f"DISPLAY {qualifier.upper()} failed: {msg}")
        return records

    def put_get(self, queue, expiry_ms=60000):
        path = f"/messaging/qmgr/{quote(self.qm)}/queue/{quote(queue)}/message"
        headers = {
            "Content-Type": "text/plain;charset=utf-8",
            "ibm-mq-md-persistence": "persistent",
            "ibm-mq-md-expiry": str(expiry_ms),
        }
        put = self._request("POST", path, data=f"mq-health-agent {time.time()}", headers=headers)
        if put.status_code == 503:
            raise RestUnavailable("mqweb reports the queue manager is unavailable (HTTP 503)")
        if put.status_code != 201:
            raise CheckError(f"persistent put failed: HTTP {put.status_code} {_mq_error(put)}")
        msg_id = put.headers.get("ibm-mq-md-messageId")
        params = {"messageId": msg_id} if msg_id else None
        get = self._request("DELETE", path, params=params)
        if get.status_code == 204:
            raise CheckError("put succeeded but the message could not be retrieved")
        if get.status_code != 200:
            raise CheckError(f"destructive get failed: HTTP {get.status_code} {_mq_error(get)}")


def _mq_error(resp):
    try:
        err = resp.json()["error"][0]
        return f"{err.get('msgId', '')} reason {err.get('reasonCode', '?')}: {err.get('message', '')}"[:300]
    except Exception:
        return resp.text[:300]


# --------------------------------------------------------------------------- checks
# Each check takes (agent, cfg) and returns (state, detail).


def check_qmgr_status(agent, cfg):
    state = agent.rest.qmgr_state()
    if state.lower() == "running":
        return OK, "queue manager is running"
    return FAIL, f"queue manager state is '{state}'"


def check_listener(agent, cfg):
    port = cfg.get("port", 1414)
    recs = agent.rest.mqsc("lsstatus", "*", response=("port", "status"))
    on_port = [r for r in recs if as_int(r.get("port")) == port]
    if not on_port:
        return FAIL, f"no started listener on port {port}"
    bad = [r for r in on_port if up(r.get("status")) != "RUNNING"]
    if bad:
        return FAIL, f"listener on port {port} status {up(bad[0].get('status'))}"
    return OK, f"listener running on port {port}"


def check_svrconn_channel(agent, cfg):
    warn_pct = cfg.get("warn_pct", 85)
    problems, warnings, notes = [], [], []
    for chl in cfg.get("channels", []):
        defs = agent.rest.mqsc("channel", chl, response=("chltype", "maxinst", "maxinstc"))
        if not defs:
            problems.append(f"{chl}: not defined")
            continue
        d = defs[0]
        if up(d.get("chltype")) != "SVRCONN":
            problems.append(f"{chl}: is {up(d.get('chltype'))}, not SVRCONN")
            continue
        insts = agent.rest.mqsc("chstatus", chl, response=("status", "conname"))
        if any(up(s.get("status")) == "STOPPED" for s in insts):
            problems.append(f"{chl}: STOPPED - new client connections are refused")
            continue
        maxinst = as_int(d.get("maxinst"), 999999999)
        maxinstc = as_int(d.get("maxinstc"), 999999999)
        count = len(insts)
        if count >= maxinst:
            problems.append(f"{chl}: {count}/{maxinst} instances (MAXINST reached)")
        elif maxinst < 999999999 and count * 100 >= maxinst * warn_pct:
            warnings.append(f"{chl}: {count}/{maxinst} instances")
        per_client = {}
        for s in insts:
            host = str(s.get("conname", "")).split("(")[0]
            per_client[host] = per_client.get(host, 0) + 1
        for host, n in per_client.items():
            if host and n >= maxinstc:
                warnings.append(f"{chl}: client {host} at MAXINSTC ({n}/{maxinstc})")
        notes.append(f"{chl}: {count} instances")
    if problems:
        return FAIL, "; ".join(problems + warnings)
    if warnings:
        return WARN, "; ".join(warnings)
    return OK, "; ".join(notes) or "no channels configured"


def _qm_ini_limits(path):
    """Return (MaxChannels, MaxActiveChannels) from qm.ini; MQ defaults are 100 / MaxChannels."""
    max_ch, max_active, in_channels = 100, None, False
    with open(path, encoding="utf-8", errors="replace") as fh:
        for line in fh:
            s = line.strip()
            if re.match(r"^[A-Za-z]+:\s*$", s):
                in_channels = s.lower().startswith("channels:")
                continue
            if in_channels and "=" in s:
                key, val = (p.strip() for p in s.split("=", 1))
                if key.lower() == "maxchannels":
                    max_ch = as_int(val, max_ch)
                elif key.lower() == "maxactivechannels":
                    max_active = as_int(val)
    return max_ch, (max_active if max_active is not None else max_ch)


def check_max_channels(agent, cfg):
    qm_ini = cfg.get("qm_ini") or os.path.join(agent.qmgr_data_dir, "qm.ini")
    if not os.path.exists(qm_ini):
        raise CheckError(f"{qm_ini} not found")
    max_ch, max_active = _qm_ini_limits(qm_ini)
    limit = min(max_ch, max_active)
    count = len(agent.rest.mqsc("chstatus", "*", response=("status",)))
    pct = count * 100 / limit if limit else 0
    detail = f"{count}/{limit} channel instances ({pct:.0f}% of MaxChannels/MaxActiveChannels)"
    if pct >= cfg.get("fail_pct", 98):
        return FAIL, detail
    if pct >= cfg.get("warn_pct", 85):
        return WARN, detail
    return OK, detail


def check_chlauth(agent, cfg):
    blocked, notes = [], []
    for probe in cfg.get("probes", []):
        params = {"match": "runcheck", "address": probe["address"]}
        if probe.get("clntuser"):
            params["clntuser"] = probe["clntuser"]
        if probe.get("sslpeer"):
            params["sslpeer"] = probe["sslpeer"]
        recs = agent.rest.mqsc("chlauth", probe["channel"], params=params)
        label = f"{probe['channel']} from {probe['address']}" + (f" as {probe['clntuser']}" if probe.get("clntuser") else "")
        rule = next((r for r in recs if up(r.get("type")) in ("BLOCKADDR", "BLOCKUSER")
                     or up(r.get("usersrc")) == "NOACCESS"), None)
        if rule:
            blocked.append(f"{label}: blocked by {up(rule.get('type'))} rule")
        else:
            notes.append(f"{label}: allowed")
    if blocked:
        return FAIL, "; ".join(blocked)
    return OK, "; ".join(notes) or "no probes configured"


def check_log_usage(agent, cfg):
    recs = agent.rest.mqsc("qmstatus", response=("all",))
    if not recs:
        raise CheckError("DISPLAY QMSTATUS returned nothing")
    loginuse = as_int(recs[0].get("loginuse"))
    if loginuse is None:
        return SKIP, "LOGINUSE not reported at this MQ level - rely on the put_get check"
    detail = f"recovery log {loginuse}% in use"
    if loginuse >= cfg.get("fail_pct", 95):
        return FAIL, detail
    if loginuse >= cfg.get("warn_pct", 80):
        return WARN, detail
    return OK, detail


def check_put_get(agent, cfg):
    agent.rest.put_get(cfg["queue"], cfg.get("expiry_ms", 60000))
    return OK, f"persistent put/get on {cfg['queue']} succeeded"


def check_app_queues(agent, cfg):
    problems, warnings, notes = [], [], []
    warn_pct = cfg.get("warn_pct", 80)
    for q in cfg.get("queues", []):
        recs = agent.rest.mqsc("qlocal", q, response=("curdepth", "maxdepth", "put", "get"))
        if not recs:
            problems.append(f"{q}: not defined")
            continue
        r = recs[0]
        cur, mx = as_int(r.get("curdepth"), 0), as_int(r.get("maxdepth"), 0)
        if up(r.get("put")) == "DISABLED":
            problems.append(f"{q}: PUT inhibited")
        if up(r.get("get")) == "DISABLED":
            problems.append(f"{q}: GET inhibited")
        if mx and cur >= mx:
            problems.append(f"{q}: full ({cur}/{mx})")
        elif mx and cur * 100 >= mx * warn_pct:
            warnings.append(f"{q}: {cur}/{mx}")
        else:
            notes.append(f"{q}: {cur}/{mx}")
    if problems:
        return FAIL, "; ".join(problems + warnings)
    if warnings:
        return WARN, "; ".join(warnings)
    return OK, "; ".join(notes) or "no queues configured"


def check_disk_space(agent, cfg):
    paths = cfg.get("paths") or [agent.qmgr_data_dir]
    worst_state, parts = OK, []
    for path in paths:
        usage = shutil.disk_usage(path)
        pct = usage.used * 100 / usage.total
        parts.append(f"{path} {pct:.0f}% used")
        if pct >= cfg.get("fail_pct", 95):
            worst_state = FAIL
        elif pct >= cfg.get("warn_pct", 85) and worst_state != FAIL:
            worst_state = WARN
    return worst_state, "; ".join(parts)


def _cert_not_after(cfg):
    pem = cfg.get("pem")
    tmpdir = None
    try:
        if not pem:
            tmpdir = tempfile.mkdtemp(prefix="mqhealth-")
            pem = os.path.join(tmpdir, "cert.pem")
            cmd = [cfg.get("runmqakm", "runmqakm"), "-cert", "-extract", "-db", cfg["kdb"], "-stashed",
                   "-label", cfg["label"], "-target", pem, "-format", "ascii"]
            out = subprocess.run(cmd, capture_output=True, text=True, timeout=20)
            if out.returncode != 0:
                raise CheckError(f"runmqakm failed: {(out.stderr or out.stdout).strip()[:200]}")
        out = subprocess.run(["openssl", "x509", "-enddate", "-noout", "-in", pem],
                             capture_output=True, text=True, timeout=10)
        if out.returncode != 0:
            raise CheckError(f"openssl failed: {out.stderr.strip()[:200]}")
        raw = " ".join(out.stdout.strip().split("=", 1)[1].split())  # 'Jan 1 00:00:00 2027 GMT'
        return datetime.strptime(raw, "%b %d %H:%M:%S %Y %Z").replace(tzinfo=timezone.utc)
    finally:
        if tmpdir:
            shutil.rmtree(tmpdir, ignore_errors=True)


def check_tls_cert(agent, cfg):
    not_after = _cert_not_after(cfg)
    days = (not_after - datetime.now(timezone.utc)).days
    detail = f"queue manager certificate expires {not_after:%Y-%m-%d} ({days} days)"
    if days < 0:
        return FAIL, detail
    if days < cfg.get("warn_days", 30):
        return WARN, detail
    return OK, detail


def check_ldap(agent, cfg):
    up_hosts, down_hosts = [], []
    for server in cfg.get("servers", []):
        host, _, port = server.rpartition(":")
        try:
            socket.create_connection((host, int(port)), timeout=cfg.get("timeout_seconds", 3)).close()
            up_hosts.append(server)
        except OSError as exc:
            down_hosts.append(f"{server} ({exc.__class__.__name__})")
    if not up_hosts and down_hosts:
        return FAIL, "no LDAP server reachable: " + ", ".join(down_hosts)
    if down_hosts:
        return WARN, "unreachable: " + ", ".join(down_hosts)
    return OK, "reachable: " + ", ".join(up_hosts)


def check_client_connect(agent, cfg):
    """Real MQ client connection through the same SVRCONN / TLS / CONNAUTH / CHLAUTH
    path the applications use, made directly to this host's listener (not the VIP)."""
    if pymqi is None:
        raise CheckError("pymqi is not installed")
    cd = pymqi.CD()
    cd.ChannelName = cfg["channel"].encode()
    cd.ConnectionName = f"{cfg.get('host', '127.0.0.1')}({cfg.get('port', 1414)})".encode()
    cd.ChannelType = pymqi.CMQXC.MQCHT_CLNTCONN
    cd.TransportType = pymqi.CMQXC.MQXPT_TCP
    sco = None
    if cfg.get("cipher"):
        cd.SSLCipherSpec = cfg["cipher"].encode()
        sco = pymqi.SCO()
        sco.KeyRepository = cfg["key_repository"].encode()
    user = cfg.get("user")
    password = os.environ.get(cfg.get("password_env", "MQHEALTH_CLIENT_PASSWORD")) if user else None
    qmgr = pymqi.QueueManager(None)
    try:
        qmgr.connect_with_options(agent.qmgr_name, user=user, password=password, cd=cd, sco=sco)
    except pymqi.MQMIError as exc:
        return FAIL, f"MQCONNX failed: {exc}"
    try:
        qmgr.inquire(pymqi.CMQC.MQCA_Q_MGR_NAME)
        if cfg.get("queue"):
            q = pymqi.Queue(qmgr, cfg["queue"])
            md = pymqi.MD()
            md.Persistence = pymqi.CMQC.MQPER_PERSISTENT
            md.Expiry = 600  # tenths of a second
            q.put(b"mq-health-agent client check", md)
            gmo = pymqi.GMO()
            gmo.Options = pymqi.CMQC.MQGMO_NO_WAIT
            gmo.MatchOptions = pymqi.CMQC.MQMO_MATCH_MSG_ID
            q.get(None, pymqi.MD(MsgId=md.MsgId), gmo)
            q.close()
        return OK, "client connect" + (" and put/get" if cfg.get("queue") else "") + " succeeded"
    except pymqi.MQMIError as exc:
        return FAIL, f"connected but MQ call failed: {exc}"
    finally:
        try:
            qmgr.disconnect()
        except Exception:
            pass


CHECKS = {
    "qmgr_status": check_qmgr_status,
    "listener": check_listener,
    "svrconn_channel": check_svrconn_channel,
    "max_channels": check_max_channels,
    "chlauth": check_chlauth,
    "log_usage": check_log_usage,
    "put_get": check_put_get,
    "app_queues": check_app_queues,
    "disk_space": check_disk_space,
    "tls_cert": check_tls_cert,
    "ldap": check_ldap,
    "client_connect": check_client_connect,
}


# --------------------------------------------------------------------------- agent


class Agent:
    def __init__(self, config):
        self.cfg = config
        self.qmgr_name = config["queue_manager"]
        self.qmgr_data_dir = config.get("qmgr_data_dir") or os.path.join(
            "/var/mqm/qmgrs", self.qmgr_name.replace(".", "!"))
        self.rest = MQRest(config["rest"], self.qmgr_name)
        self.interval = config.get("interval_seconds", 10)
        self.check_timeout = config.get("check_timeout_seconds", 8)
        # A check listed with no options (None or {}) is enabled with defaults.
        checks = {name: (c or {}) for name, c in (config.get("checks") or {}).items()}
        self.enabled = {name: c for name, c in checks.items() if c.get("enabled", True)}
        unknown = set(self.enabled) - set(CHECKS)
        if unknown:
            raise SystemExit(f"unknown checks in config: {', '.join(sorted(unknown))}")
        if "client_connect" in self.enabled and pymqi is None:
            raise SystemExit("client_connect is enabled but pymqi is not installed (pip install pymqi)")
        self.pool = ThreadPoolExecutor(max_workers=max(4, len(self.enabled)))
        self.latest = None
        self.latest_at = 0.0
        self._prev_states = {}

    def _run_one(self, name):
        cfg = self.enabled[name]
        try:
            state, detail = CHECKS[name](self, cfg)
            return {"state": state, "detail": detail}
        except RestUnavailable as exc:
            return {"state": FAIL, "detail": f"mqweb unavailable: {exc}", "rest_unavailable": True}
        except CheckError as exc:
            return {"state": FAIL, "detail": str(exc)}
        except Exception as exc:  # defensive: never let one check kill the loop
            log.exception("check %s raised", name)
            return {"state": FAIL, "detail": f"{type(exc).__name__}: {exc}"}

    def evaluate(self):
        started = time.time()
        futures = {name: self.pool.submit(self._run_one, name) for name in self.enabled}
        results = {}
        for name, fut in futures.items():
            remaining = max(0.1, self.check_timeout - (time.time() - started))
            try:
                results[name] = fut.result(timeout=remaining)
            except FutureTimeout:
                results[name] = {"state": FAIL, "detail": f"timed out after {self.check_timeout}s (queue manager hung?)"}
            results[name]["scope"] = self.enabled[name].get("scope", DEFAULT_SCOPE[name])

        # An mqweb outage is not a queue manager outage. If the real client path
        # works, report REST-dependent checks as WARN instead of failing the member.
        client_ok = results.get("client_connect", {}).get("state") == OK
        for name, r in results.items():
            if r.pop("rest_unavailable", False):
                if name == "qmgr_status" and not client_ok:
                    continue  # stays FAIL: no evidence the queue manager is serving
                r["state"] = WARN
                if name == "qmgr_status":
                    r["detail"] += " (client connect check passed; not failing the member)"

        member_fail = [n for n, r in results.items() if r["state"] == FAIL and r["scope"] == MEMBER]
        any_issue = any(r["state"] in (FAIL, WARN) for r in results.values())
        status = DOWN if member_fail else DEGRADED if any_issue else UP
        report = {
            "status": status,
            "queue_manager": self.qmgr_name,
            "host": socket.gethostname(),
            "evaluated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "duration_ms": int((time.time() - started) * 1000),
            "failing_member_checks": member_fail,
            "checks": results,
        }
        self._log_transitions(report)
        self.latest, self.latest_at = report, time.time()
        return report

    def _log_transitions(self, report):
        current = {n: r["state"] for n, r in report["checks"].items()}
        current["_overall"] = report["status"]
        for name, state in current.items():
            prev = self._prev_states.get(name)
            if prev is not None and prev != state:
                detail = report["checks"].get(name, {}).get("detail", "")
                level = logging.INFO if state in (OK, UP) else logging.WARNING
                log.log(level, json.dumps({"event": "state_change", "check": name, "from": prev,
                                           "to": state, "detail": detail}))
        if not self._prev_states:
            log.info(json.dumps({"event": "initial_state", "status": report["status"],
                                 "failing": report["failing_member_checks"]}))
        self._prev_states = current

    def loop(self):
        while True:
            try:
                self.evaluate()
            except Exception:
                log.exception("evaluation failed")
            time.sleep(self.interval)

    def current(self):
        if self.latest is None or time.time() - self.latest_at > 3 * self.interval + self.check_timeout:
            return {"status": STALE, "queue_manager": self.qmgr_name,
                    "detail": "no recent evaluation - agent check loop is stuck or starting"}
        return self.latest


# --------------------------------------------------------------------------- HTTP


def make_handler(agent, allow_from):
    networks = [ipaddress.ip_network(n, strict=False) for n in allow_from]

    class Handler(BaseHTTPRequestHandler):
        server_version = "mq-health-agent"

        def _allowed(self):
            if not networks:
                return True
            addr = ipaddress.ip_address(self.client_address[0])
            return any(addr in n for n in networks)

        def _send(self, code, body, head_only=False):
            payload = json.dumps(body, indent=2).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("Connection", "close")
            self.end_headers()
            if not head_only:
                self.wfile.write(payload)

        def _handle(self, head_only):
            if not self._allowed():
                return self._send(403, {"error": "forbidden"}, head_only)
            path = self.path.split("?", 1)[0]
            if path == "/health":
                report = agent.current()
                code = 200 if report["status"] in (UP, DEGRADED) else 503
                return self._send(code, report, head_only)
            if path == "/livez":
                return self._send(200, {"status": "alive"}, head_only)
            return self._send(404, {"error": "not found"}, head_only)

        def do_GET(self):
            self._handle(False)

        def do_HEAD(self):
            self._handle(True)

        def log_message(self, fmt, *args):  # F5 probes every few seconds; keep the log quiet
            log.debug("%s %s", self.client_address[0], fmt % args)

    return Handler


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default="/etc/mq-health-agent/config.yaml")
    ap.add_argument("--once", action="store_true", help="evaluate once, print JSON, and exit")
    ap.add_argument("--log-level", default="INFO")
    args = ap.parse_args()
    logging.basicConfig(level=args.log_level.upper(), format="%(asctime)s %(levelname)s %(message)s")

    with open(args.config, encoding="utf-8") as fh:
        config = yaml.safe_load(fh)
    agent = Agent(config)

    if args.once:
        report = agent.evaluate()
        print(json.dumps(report, indent=2))
        sys.exit({UP: 0, DEGRADED: 1}.get(report["status"], 2))

    http_cfg = config.get("http", {})
    server = ThreadingHTTPServer((http_cfg.get("bind", "0.0.0.0"), http_cfg.get("port", 9444)),
                                 make_handler(agent, http_cfg.get("allow_from", [])))
    tls = http_cfg.get("tls") or {}
    if tls.get("cert"):
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.minimum_version = ssl.TLSVersion.TLSv1_2
        ctx.load_cert_chain(tls["cert"], tls.get("key"))
        server.socket = ctx.wrap_socket(server.socket, server_side=True)

    threading.Thread(target=agent.loop, name="checks", daemon=True).start()
    log.info("listening on %s:%s for queue manager %s", *server.server_address[:2], agent.qmgr_name)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
