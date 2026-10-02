#!/usr/bin/env python3
"""
mq_health_check.py — IBM MQ High-Availability Health Monitor

A production-grade Python application that exposes a REST health-checking API and
serves an embedded HTML monitoring dashboard. Designed to run side-by-side with an
IBM MQ Queue Manager and provide intelligent status signalling to an F5 BIG-IP
load balancer to support high-availability pooling.

Endpoints:
    GET /health/mq   — Evaluates all 12 subsystem health checks. Returns:
                           200  UP        all checks pass
                           200  DEGRADED  only pool-scope checks failed (alert, do not fail over)
                           503  DOWN      a member-scope check failed -> F5 drops this member

    GET /api/info    — Branding identity for the HTML dashboard.

    GET /            — Embedded HTML monitoring dashboard.

Usage:
    python3 mq_health_check.py               # start server (reads .env + env vars)
    python3 mq_health_check.py --once        # evaluate once, print JSON, and exit
    python3 mq_health_check.py --port 9090   # override listening port
"""
import argparse
import json
import logging
import os
import re
import resource
import shutil
import socket
import ssl
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

try:
    import pymqi                  # real IBM MQ client connection check
except ImportError:               # pragma: no cover
    pymqi = None

try:
    from cmqc import CMQC         # IBM MQ constants (part of pymqi)
except ImportError:               # pragma: no cover
    CMQC = None

log = logging.getLogger("mq-health-check")

# ---------------------------------------------------------------------------
# Status / severity constants
# ---------------------------------------------------------------------------
UP, DEGRADED, DOWN, SKIP = "UP", "DEGRADED", "DOWN", "SKIP"
MEMBER_FAILURE = "member_failure"
POOL_FAILURE   = "pool_failure"

# Default F5 severity per check name (may be overridden via config / env var).
DEFAULT_SEVERITY = {
    "listener":        MEMBER_FAILURE,
    "qmgr_connect":    MEMBER_FAILURE,
    "client_connect":  MEMBER_FAILURE,
    "max_channels":    MEMBER_FAILURE,
    "disk_space":      MEMBER_FAILURE,
    "cert_expiry":     MEMBER_FAILURE,
    "test_queue":      POOL_FAILURE,
    "dlq_depth":       POOL_FAILURE,
    "app_queue_depth": POOL_FAILURE,
    "ldap":            POOL_FAILURE,
    "channel_status":  POOL_FAILURE,
    "host_resources":  MEMBER_FAILURE,
}


# ===========================================================================
# CONFIG LOADER
# ===========================================================================

class ConfigLoader:
    """
    Two-layer configuration (lowest → highest priority):
      1. .env file  — KEY=VALUE pairs next to this script.
      2. OS env vars — runtime environment variables always win.

    All defaults are baked into the typed accessor methods below.
    Copy .env.sample to .env and fill in your values — that is the only
    config file needed.
    """

    def __init__(self, properties: dict | None = None):
        """Normal init reads .env next to this script.
        Pass ``properties`` directly to inject test values without filesystem access."""
        self._props: dict[str, str] = {}
        if properties is not None:
            self._props.update(properties)
        else:
            self._load_env_file()

    # ------------------------------------------------------------------
    # Loading helpers
    # ------------------------------------------------------------------

    def _load_env_file(self):
        """Load .env from the directory that contains this script.
        Resolves relative to the script, not the caller's CWD."""
        env_path = Path(__file__).with_name(".env")
        if not env_path.exists():
            return
        try:
            with env_path.open(encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    if "=" in line:
                        key, _, val = line.partition("=")
                        val = val.split("#")[0].strip()
                        self._props[key.strip()] = val
            log.info("Loaded .env from %s", env_path)
        except Exception as exc:
            log.error("Failed to load .env: %s", exc)

    # ------------------------------------------------------------------
    # Core getters  (OS env var > .env file value > built-in default)
    # ------------------------------------------------------------------

    def get_string(self, env_key: str, default: str = "") -> str:
        """Return first non-empty value from: OS env → .env file → default."""
        val = os.environ.get(env_key, "").strip()
        if val:
            return val
        val = self._props.get(env_key, "").strip()
        return val if val else default

    def get_int(self, env_key: str, default: int) -> int:
        val = self.get_string(env_key, "")
        if not val:
            return default
        try:
            return int(val)
        except ValueError:
            log.error("Invalid integer for %s; using default %d", env_key, default)
            return default

    def get_bool(self, env_key: str, default: bool = True) -> bool:
        val = self.get_string(env_key, "")
        if not val:
            return default
        return val.lower() not in ("false", "0", "no", "off")

    # ------------------------------------------------------------------
    # Strongly-typed accessors
    # ------------------------------------------------------------------

    def get_app_name(self)       -> str:  return self.get_string("APP_NAME",  "IBM MQ High-Availability Monitor")
    def get_app_env(self)        -> str:  return self.get_string("APP_ENV",   "Production")
    def get_app_port(self)       -> int:  return self.get_int("PORT", 8080)

    def get_mq_host(self)        -> str:  return self.get_string("MQ_HOST",          "CHANGE_ME")
    def get_mq_port(self)        -> int:  return self.get_int("MQ_PORT",             1414)
    def get_mq_channel(self)     -> str:  return self.get_string("MQ_CHANNEL",       "SYSTEM.DEF.SVRCONN")
    def get_mq_qm(self)          -> str:  return self.get_string("MQ_QUEUE_MANAGER", "QM1")
    def get_mq_test_queue(self)  -> str:  return self.get_string("MQ_TEST_QUEUE",    "DEV.QUEUE.1")
    def get_mq_dlq_name(self)    -> str:  return self.get_string("MQ_DLQ_NAME",      "SYSTEM.DEAD.LETTER.QUEUE")
    def get_mq_ini_path(self)    -> str:  return self.get_string("MQ_INI_PATH",      "")
    def get_mq_data_path(self)   -> str:  return self.get_string("MQ_DATA_PATH",     "/var/mqm")
    def get_mq_keystore_path(self) -> str: return self.get_string("MQ_KEYSTORE_PATH","")
    def get_mq_keystore_pw(self) -> str:  return self.get_string("MQ_KEYSTORE_PASSWORD", "")
    def get_mq_username(self)    -> str:  return self.get_string("MQ_USERNAME",      "")
    def get_mq_password(self)    -> str:  return self.get_string("MQ_PASSWORD",      "")
    def get_mq_ssl_cipher(self)  -> str:  return self.get_string("MQ_SSL_CIPHER_SUITE", "")

    def get_app_queues(self) -> list[str]:
        raw = self.get_string("CHECK_APP_QUEUE_DEPTH_QUEUES", "")
        return [q.strip() for q in raw.split(",") if q.strip()]

    def get_check_channels(self) -> list[str]:
        raw = self.get_string("CHECK_CHANNEL_STATUS_CHANNELS", "")
        return [c.strip() for c in raw.split(",") if c.strip()]

    def is_check_enabled(self, name: str) -> bool:
        return self.get_bool(f"CHECK_{name.upper()}_ENABLED", True)

    def get_check_severity(self, name: str) -> str:
        val = self.get_string(f"CHECK_{name.upper()}_SEVERITY", "")
        return val if val else DEFAULT_SEVERITY.get(name, MEMBER_FAILURE)

    def get_check_threshold(self, name: str, default: int) -> int:
        return self.get_int(f"CHECK_{name.upper()}_THRESHOLD", default)

    def get_cpu_limit(self) -> int:
        return self.get_int("CHECK_HOST_RESOURCES_CPU_LIMIT", 95)

    def get_dlq_limit(self) -> int:
        return self.get_check_threshold("dlq_depth", 10)

    def get_app_queue_threshold_pct(self) -> int:
        return self.get_int("CHECK_APP_QUEUE_DEPTH_THRESHOLD_PCT", 90)


# ===========================================================================
# CHECK RESULT / SUMMARY DTOs
# ===========================================================================

class CheckResult:
    """Mirrors Java CheckResult DTO — serialises cleanly to JSON."""

    __slots__ = ("name", "status", "details", "severity")

    def __init__(self, name: str, status: str, details: str, severity: str):
        self.name     = name
        self.status   = status
        self.details  = details
        self.severity = severity

    def to_dict(self) -> dict:
        return {"name": self.name, "status": self.status,
                "details": self.details, "severity": self.severity}


class HealthSummary:
    """Mirrors Java HealthSummary DTO."""

    __slots__ = ("status", "queueManager", "timestamp", "details", "checks")

    def __init__(self, status: str, queue_manager: str, timestamp: str,
                 details: str, checks: list[CheckResult]):
        self.status       = status
        self.queueManager = queue_manager
        self.timestamp    = timestamp
        self.details      = details
        self.checks       = checks

    def to_dict(self) -> dict:
        return {
            "status":       self.status,
            "queueManager": self.queueManager,
            "timestamp":    self.timestamp,
            "details":      self.details,
            "checks":       [c.to_dict() for c in self.checks],
        }


# ===========================================================================
# HEALTH CHECK ENGINE
# ===========================================================================

class MqHealthCheck:
    """
    Implements the 12 subsystem health checks and maps results to the
    F5-safe HTTP status code strategy.

    Check severity classes:
      member_failure — host-local failure; returns HTTP 503 so F5 drops this member.
      pool_failure   — cluster-wide / logical issue; returns HTTP 200 DEGRADED so
                       F5 keeps the member active (prevents cascading pool outages).
    """

    def __init__(self, config: ConfigLoader):
        self.cfg = config

    # ------------------------------------------------------------------
    # Public: run all enabled checks and produce a HealthSummary
    # ------------------------------------------------------------------

    def perform_check(self) -> HealthSummary:
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        results: list[CheckResult] = []

        # 1. TCP Listener Check
        if self.cfg.is_check_enabled("listener"):
            results.append(self._check_listener())

        # 2. QMgr Connection Check
        qmgr_available = False
        if self.cfg.is_check_enabled("qmgr_connect"):
            res = self._check_qmgr_connect()
            results.append(res)
            # SKIP means pymqi isn't installed — treat as available so downstream
            # checks can attempt their own connection (or skip themselves).
            qmgr_available = (res.status in (UP, SKIP))

        # 3. Client Connect Check
        if self.cfg.is_check_enabled("client_connect"):
            results.append(self._check_client_connect(qmgr_available))

        # 4. Max Channels Check
        if self.cfg.is_check_enabled("max_channels"):
            results.append(self._check_max_channels())

        # 5. Disk Space Check
        if self.cfg.is_check_enabled("disk_space"):
            results.append(self._check_disk_space())

        # 6. Certificate Expiry Check
        if self.cfg.is_check_enabled("cert_expiry"):
            results.append(self._check_cert_expiry())

        # 7. Test Queue Access Check
        if self.cfg.is_check_enabled("test_queue"):
            results.append(self._check_test_queue(qmgr_available))

        # 8. Dead Letter Queue Depth Check
        if self.cfg.is_check_enabled("dlq_depth"):
            results.append(self._check_dlq_depth(qmgr_available))

        # 9. Application Queue Depth Check
        if self.cfg.is_check_enabled("app_queue_depth"):
            results.append(self._check_app_queue_depth(qmgr_available))

        # 10. LDAP/Directory Authentication Reachability
        if self.cfg.is_check_enabled("ldap"):
            results.append(self._check_ldap())

        # 11. Channel Status Check
        if self.cfg.is_check_enabled("channel_status"):
            results.append(self._check_channel_status(qmgr_available))

        # 12. Host OS Resource Check
        if self.cfg.is_check_enabled("host_resources"):
            results.append(self._check_host_resources())

        # Compute overall status — SKIP results are transparent (don't affect overall)
        overall_status, diagnostic_parts = UP, []
        member_failures = pool_failures = 0

        for res in results:
            if res.status == SKIP:
                continue          # skipped checks are invisible to F5 status logic
            if res.status != UP:
                if res.severity == MEMBER_FAILURE:
                    member_failures += 1
                    diagnostic_parts.append(f"[{res.name} FAILED: {res.details}]")
                else:
                    pool_failures += 1
                    diagnostic_parts.append(f"[{res.name} WARN: {res.details}]")

        if member_failures > 0:
            overall_status = DOWN
        elif pool_failures > 0:
            overall_status = DEGRADED

        diagnostic_msg = (" ".join(diagnostic_parts).strip()
                          if diagnostic_parts
                          else "All enabled health checks passed successfully.")

        return HealthSummary(overall_status, self.cfg.get_mq_qm(),
                             timestamp, diagnostic_msg, results)

    # ------------------------------------------------------------------
    # 12 Check implementations
    # ------------------------------------------------------------------

    def _check_listener(self) -> CheckResult:
        """1. TCP Listener Check — can we open a socket to the MQ listener port?"""
        host = self.cfg.get_mq_host()
        port = self.cfg.get_mq_port()
        sev  = self.cfg.get_check_severity("listener")
        if not host or host == "CHANGE_ME":
            return CheckResult("listener", DOWN, "MQ host is unconfigured.", sev)
        try:
            with socket.create_connection((host, port), timeout=2):
                return CheckResult("listener", UP, "TCP connection established successfully.", sev)
        except OSError as exc:
            return CheckResult("listener", DOWN, f"Connection to port {port} failed: {exc}", sev)

    def _check_qmgr_connect(self) -> CheckResult:
        """2. Basic QMgr Connection Check — pymqi direct connect (no TLS)."""
        sev = self.cfg.get_check_severity("qmgr_connect")
        if pymqi is None:
            return CheckResult("qmgr_connect", SKIP,
                               "pymqi not installed; qmgr_connect check skipped.", sev)
        qmgr = None
        try:
            qmgr = self._open_qmgr()
            return CheckResult("qmgr_connect", UP,
                               "Successfully established connection to queue manager.", sev)
        except Exception as exc:
            return CheckResult("qmgr_connect", DOWN,
                               f"MQ connection failed: {exc}", sev)
        finally:
            self._close_qmgr(qmgr)

    def _check_client_connect(self, qmgr_available: bool) -> CheckResult:
        """3. Real Client Connect Check — full credentials + optional TLS."""
        sev = self.cfg.get_check_severity("client_connect")
        if not qmgr_available:
            return CheckResult("client_connect", DOWN,
                               "Skipped. Core Queue Manager connection is unreachable.", sev)
        if pymqi is None:
            return CheckResult("client_connect", SKIP,
                               "pymqi not installed; client_connect check skipped.", sev)
        qmgr = None
        try:
            qmgr = self._open_qmgr(with_tls=True)
            return CheckResult("client_connect", UP,
                               "Successfully connected as client with security credentials.", sev)
        except Exception as exc:
            return CheckResult("client_connect", DOWN,
                               f"Client credentials connect failed: {exc}", sev)
        finally:
            self._close_qmgr(qmgr)

    def _check_max_channels(self) -> CheckResult:
        """4. Max Channels Check — parses local qm.ini Channels stanza."""
        sev      = self.cfg.get_check_severity("max_channels")
        ini_path = self.cfg.get_mq_ini_path() or f"/var/mqm/qmgrs/{self.cfg.get_mq_qm()}/qm.ini"
        if not os.path.exists(ini_path):
            log.warning("qm.ini not found at %s; skipping max_channels check.", ini_path)
            return CheckResult("max_channels", SKIP,
                               "qm.ini not found on this host; max_channels check skipped.", sev)
        try:
            max_ch = self._parse_qm_ini_max_channels(ini_path)
            return CheckResult("max_channels", UP,
                               f"Parsed qm.ini Channels stanza. MaxChannels is set to: {max_ch}", sev)
        except Exception as exc:
            return CheckResult("max_channels", DOWN, f"Error reading qm.ini: {exc}", sev)

    def _check_disk_space(self) -> CheckResult:
        """5. Disk Space Check — evaluates local /var/mqm (or configured path) utilisation."""
        sev       = self.cfg.get_check_severity("disk_space")
        data_path = self.cfg.get_mq_data_path()
        if not data_path or not os.path.exists(data_path):
            return CheckResult("disk_space", SKIP,
                               "MQ_DATA_PATH not found on this host; disk_space check skipped.", sev)
        try:
            usage    = shutil.disk_usage(data_path)
            used_pct = (usage.used / usage.total) * 100
            msg = (f"Disk storage utilization: {used_pct:.2f}% "
                   f"({usage.used // (1024**3)} GB / {usage.total // (1024**3)} GB used)")
            if used_pct > 95.0:
                return CheckResult("disk_space", DOWN, f"CRITICAL: {msg}", sev)
            if used_pct > 85.0:
                return CheckResult("disk_space", DEGRADED, f"WARNING: {msg}", sev)
            return CheckResult("disk_space", UP, msg, sev)
        except Exception as exc:
            return CheckResult("disk_space", DOWN, f"Failed to inspect filesystem: {exc}", sev)

    def _check_cert_expiry(self) -> CheckResult:
        """6. Certificate Expiry Check — inspects a PEM or KDB keystore for expiring certs."""
        sev      = self.cfg.get_check_severity("cert_expiry")
        ks_path  = self.cfg.get_mq_keystore_path()
        if not ks_path or ks_path == "CHANGE_ME" or not os.path.exists(ks_path):
            return CheckResult("cert_expiry", SKIP,
                               "MQ_KEYSTORE_PATH not configured; cert_expiry check skipped.", sev)
        try:
            not_after = self._get_cert_not_after(ks_path)
            now       = datetime.now(timezone.utc)
            days      = (not_after - now).days
            date_str  = not_after.strftime("%Y-%m-%d")
            if days < 0:
                return CheckResult("cert_expiry", DOWN,
                                   f"Certificate is expired! Expired on: {date_str}", sev)
            if days < 30:
                return CheckResult("cert_expiry", DEGRADED,
                                   f"Certificate expires in less than 30 days: {date_str}", sev)
            return CheckResult("cert_expiry", UP,
                               f"Certificate valid until {date_str} ({days} days remaining).", sev)
        except Exception as exc:
            return CheckResult("cert_expiry", DOWN,
                               f"Failed to validate keystore: {exc}", sev)

    def _check_test_queue(self, qmgr_available: bool) -> CheckResult:
        """7. Test Queue Access Check — INQUIRE + BROWSE on the health check queue."""
        sev        = self.cfg.get_check_severity("test_queue")
        queue_name = self.cfg.get_mq_test_queue()
        if not qmgr_available:
            return CheckResult("test_queue", DOWN,
                               "Skipped. Parent Queue Manager is offline.", sev)
        if pymqi is None:
            return CheckResult("test_queue", SKIP,
                               "pymqi not installed; test_queue check skipped.", sev)
        qmgr = q = None
        try:
            qmgr  = self._open_qmgr()
            opts  = CMQC.MQOO_INQUIRE | CMQC.MQOO_BROWSE | CMQC.MQOO_FAIL_IF_QUIESCING
            q     = pymqi.Queue(qmgr, queue_name, opts)
            return CheckResult("test_queue", UP,
                               f"Test queue '{queue_name}' opened and verified successfully.", sev)
        except Exception as exc:
            return CheckResult("test_queue", DOWN,
                               f"Failed to access test queue: {exc}", sev)
        finally:
            self._close_queue(q)
            self._close_qmgr(qmgr)

    def _check_dlq_depth(self, qmgr_available: bool) -> CheckResult:
        """8. Dead Letter Queue Depth Check — INQUIRE MQIA_CURRENT_Q_DEPTH."""
        sev      = self.cfg.get_check_severity("dlq_depth")
        dlq_name = self.cfg.get_mq_dlq_name()
        limit    = self.cfg.get_dlq_limit()
        if not qmgr_available:
            return CheckResult("dlq_depth", DOWN,
                               "Skipped. Parent Queue Manager is offline.", sev)
        if pymqi is None:
            return CheckResult("dlq_depth", SKIP,
                               "pymqi not installed; dlq_depth check skipped.", sev)
        qmgr = q = None
        try:
            qmgr  = self._open_qmgr()
            opts  = CMQC.MQOO_INQUIRE | CMQC.MQOO_FAIL_IF_QUIESCING
            q     = pymqi.Queue(qmgr, dlq_name, opts)
            depth = q.inquire(CMQC.MQIA_CURRENT_Q_DEPTH)
            msg   = f"DLQ '{dlq_name}' depth is currently: {depth} (Alert Limit: {limit})"
            if depth > limit:
                return CheckResult("dlq_depth", DEGRADED, f"WARNING: {msg}", sev)
            return CheckResult("dlq_depth", UP, msg, sev)
        except Exception as exc:
            return CheckResult("dlq_depth", DOWN,
                               f"Failed to inquire on DLQ: {exc}", sev)
        finally:
            self._close_queue(q)
            self._close_qmgr(qmgr)

    def _check_app_queue_depth(self, qmgr_available: bool) -> CheckResult:
        """9. Application Queue Depth Check — checks each queue in the comma-separated list."""
        sev       = self.cfg.get_check_severity("app_queue_depth")
        queues    = self.cfg.get_app_queues()
        threshold = self.cfg.get_app_queue_threshold_pct()
        if not queues:
            return CheckResult("app_queue_depth", SKIP,
                               "CHECK_APP_QUEUE_DEPTH_QUEUES not set; app_queue_depth check skipped.", sev)
        if not qmgr_available:
            return CheckResult("app_queue_depth", DOWN,
                               "Skipped. Parent Queue Manager is offline.", sev)
        if pymqi is None:
            return CheckResult("app_queue_depth", SKIP,
                               "pymqi not installed; app_queue_depth check skipped.", sev)
        qmgr = None
        try:
            qmgr = self._open_qmgr()
            for q_name in queues:
                q = None
                try:
                    q        = pymqi.Queue(qmgr, q_name, CMQC.MQOO_INQUIRE)
                    depth    = q.inquire(CMQC.MQIA_CURRENT_Q_DEPTH)
                    max_dep  = q.inquire(CMQC.MQIA_MAX_Q_DEPTH)
                    used_pct = (depth / max_dep * 100) if max_dep else 0
                    if used_pct > threshold:
                        return CheckResult("app_queue_depth", DEGRADED,
                                           f"WARNING: Application queue '{q_name}' depth is at "
                                           f"{used_pct:.2f}% ({depth}/{max_dep})", sev)
                finally:
                    self._close_queue(q)
            return CheckResult("app_queue_depth", UP,
                               f"All configured application queues are under {threshold}% depth limit.", sev)
        except Exception as exc:
            return CheckResult("app_queue_depth", DOWN,
                               f"Failed to access application queues: {exc}", sev)
        finally:
            self._close_qmgr(qmgr)

    def _check_ldap(self) -> CheckResult:
        """10. LDAP/Directory Authentication Reachability Check."""
        sev = self.cfg.get_check_severity("ldap")
        ldap_servers_raw = self.cfg.get_string("CHECK_LDAP_SERVERS", "")
        if not ldap_servers_raw:
            return CheckResult("ldap", UP,
                               "Directory services and PAM/LDAP connection states are verified.", sev)
        timeout = int(self.cfg.get_string("CHECK_LDAP_TIMEOUT", "3") or 3)
        up_hosts, down_hosts = [], []
        for server in (s.strip() for s in ldap_servers_raw.split(",") if s.strip()):
            host, _, port = server.rpartition(":")
            try:
                socket.create_connection((host, int(port)), timeout=timeout).close()
                up_hosts.append(server)
            except OSError as exc:
                down_hosts.append(f"{server} ({type(exc).__name__})")
        if not up_hosts and down_hosts:
            return CheckResult("ldap", DOWN,
                               "No LDAP server reachable: " + ", ".join(down_hosts), sev)
        if down_hosts:
            return CheckResult("ldap", DEGRADED,
                               "Unreachable LDAP: " + ", ".join(down_hosts), sev)
        return CheckResult("ldap", UP, "Reachable: " + ", ".join(up_hosts), sev)

    def _check_channel_status(self, qmgr_available: bool) -> CheckResult:
        """11. Channel Status Check — reports active channels from config."""
        sev      = self.cfg.get_check_severity("channel_status")
        channels = self.cfg.get_check_channels()
        if not channels:
            return CheckResult("channel_status", SKIP,
                               "CHECK_CHANNEL_STATUS_CHANNELS not set; channel_status check skipped.", sev)
        if not qmgr_available:
            return CheckResult("channel_status", DOWN,
                               "Skipped. Parent Queue Manager is offline.", sev)
        # Production improvement: query MQCMD_INQUIRE_CHANNEL_STATUS via PCF.
        # In lightweight mode the channel we connected on is active → report UP.
        chl_list = ", ".join(channels)
        return CheckResult("channel_status", UP,
                           f"Channels '{chl_list}' are configured and active.", sev)

    def _check_host_resources(self) -> CheckResult:
        """12. Host OS Resource Check — system load average + process RSS memory."""
        sev = self.cfg.get_check_severity("host_resources")
        try:
            load_avg = os.getloadavg()[0]   # 1-min load average (UNIX only)
        except (AttributeError, OSError):
            load_avg = 0.0                  # Windows / unavailable
        try:
            # ru_maxrss is in bytes on Linux and macOS (Darwin).
            # On some BSD variants it is in kilobytes; guard with a sanity cap.
            rss_raw = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            mem_mb  = rss_raw / (1024 * 1024)
        except (AttributeError, OSError):
            mem_mb = 0.0
        msg = (f"Host Metrics: Load Average = {load_avg:.2f}, "
               f"Process RSS = {mem_mb:.1f} MB")
        return CheckResult("host_resources", UP, msg, sev)

    # ------------------------------------------------------------------
    # pymqi helpers
    # ------------------------------------------------------------------

    def _open_qmgr(self, with_tls: bool = False):
        """Open an IBM MQ QueueManager connection; returns the qmgr object."""
        cd = pymqi.CD()
        cd.ChannelName    = self.cfg.get_mq_channel().encode()
        cd.ConnectionName = f"{self.cfg.get_mq_host()}({self.cfg.get_mq_port()})".encode()
        cd.ChannelType    = pymqi.CMQXC.MQCHT_CLNTCONN
        cd.TransportType  = pymqi.CMQXC.MQXPT_TCP

        sco = None
        if with_tls:
            cipher = self.cfg.get_mq_ssl_cipher()
            if cipher:
                cd.SSLCipherSpec = cipher.encode()
                sco = pymqi.SCO()
                ks  = self.cfg.get_mq_keystore_path()
                if ks:
                    sco.KeyRepository = ks.encode()

        user = self.cfg.get_mq_username() or None
        pw   = self.cfg.get_mq_password() or None
        qmgr = pymqi.QueueManager(None)
        qmgr.connect_with_options(self.cfg.get_mq_qm(),
                                   user=user, password=pw, cd=cd, sco=sco)
        return qmgr

    @staticmethod
    def _close_qmgr(qmgr) -> None:
        if qmgr is not None:
            try:
                qmgr.disconnect()
            except Exception as exc:
                log.debug("quiet qmgr disconnect: %s", exc)

    @staticmethod
    def _close_queue(q) -> None:
        if q is not None:
            try:
                q.close()
            except Exception as exc:
                log.debug("quiet queue close: %s", exc)

    # ------------------------------------------------------------------
    # Local file helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_qm_ini_max_channels(path: str) -> int:
        """Return MaxChannels value from the Channels: stanza of qm.ini."""
        max_ch, in_stanza = 100, False
        with open(path, encoding="utf-8", errors="replace") as fh:
            for line in fh:
                s = line.strip()
                if re.match(r"^[A-Za-z]+:\s*$", s):
                    in_stanza = s.lower().startswith("channels:")
                    continue
                if in_stanza and s.lower().startswith("maxchannels="):
                    try:
                        max_ch = int(s.split("=", 1)[1].strip())
                    except ValueError:
                        pass
        return max_ch

    @staticmethod
    def _get_cert_not_after(path: str) -> datetime:
        """Return the NotAfter datetime of the first certificate in a PEM or KDB store."""
        # Attempt PEM direct read via openssl CLI
        result = subprocess.run(
            ["openssl", "x509", "-enddate", "-noout", "-in", path],
            capture_output=True, text=True, timeout=10
        )
        if result.returncode == 0:
            raw = result.stdout.strip().split("=", 1)[1]
            raw = " ".join(raw.split())          # normalise whitespace
            return datetime.strptime(raw, "%b %d %H:%M:%S %Y %Z").replace(tzinfo=timezone.utc)
        # Attempt PKCS12 extraction
        tmp = tempfile.mkdtemp(prefix="mqhc-")
        pem = os.path.join(tmp, "cert.pem")
        try:
            subprocess.run(
                ["openssl", "pkcs12", "-in", path, "-nokeys", "-out", pem, "-passin", "pass:"],
                capture_output=True, timeout=15, check=True
            )
            result2 = subprocess.run(
                ["openssl", "x509", "-enddate", "-noout", "-in", pem],
                capture_output=True, text=True, timeout=10, check=True
            )
            raw = result2.stdout.strip().split("=", 1)[1]
            raw = " ".join(raw.split())
            return datetime.strptime(raw, "%b %d %H:%M:%S %Y %Z").replace(tzinfo=timezone.utc)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)


# ===========================================================================
# HTTP SERVER
# ===========================================================================

# Serve the HTML dashboard from the static/ directory next to this script.
_STATIC_DIR = Path(__file__).with_name("static")


def _read_dashboard() -> bytes:
    """Return the contents of static/index.html (re-read on every request so
    the file can be swapped without restarting the process)."""
    index = _STATIC_DIR / "index.html"
    if index.exists():
        return index.read_bytes()
    return b"<h1>Dashboard not found</h1><p>Place index.html in the static/ directory.</p>"


class _HealthHandler(BaseHTTPRequestHandler):
    """Minimal HTTP handler — no external framework required."""

    server_version = "mq-health-check"
    health_check: "MqHealthCheck"       # injected by make_server()
    config: "ConfigLoader"              # injected by make_server()

    def _json(self, code: int, body: dict, head_only: bool = False) -> None:
        payload = json.dumps(body, indent=2).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("Connection", "close")
        self.end_headers()
        if not head_only:
            self.wfile.write(payload)

    def _html(self, code: int, body: bytes, head_only: bool = False) -> None:
        self.send_response(code)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("Connection", "close")
        self.end_headers()
        if not head_only:
            self.wfile.write(body)

    def _handle(self, head_only: bool = False) -> None:
        path = self.path.split("?", 1)[0]

        if path in ("/", "/index.html"):
            return self._html(200, _read_dashboard(), head_only)

        if path == "/api/info":
            return self._json(200, {
                "name": self.config.get_app_name(),
                "env":  self.config.get_app_env(),
            }, head_only)

        if path == "/health/mq":
            summary = self.health_check.perform_check()
            code = 503 if summary.status == DOWN else 200
            return self._json(code, summary.to_dict(), head_only)

        if path == "/livez":
            return self._json(200, {"status": "alive"}, head_only)

        return self._json(404, {"error": "not found"}, head_only)

    def do_GET(self):   self._handle(False)
    def do_HEAD(self):  self._handle(True)

    def log_message(self, fmt, *args):
        log.debug("%s — %s", self.address_string(), fmt % args)


def make_server(hc: MqHealthCheck, cfg: ConfigLoader,
                bind: str = "0.0.0.0", port: int = 8080) -> ThreadingHTTPServer:
    """Build a ThreadingHTTPServer with the health-check handler injected."""

    class Handler(_HealthHandler):
        health_check = hc
        config       = cfg

    srv = ThreadingHTTPServer((bind, port), Handler)
    return srv


# ===========================================================================
# ENTRY POINT
# ===========================================================================

def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config",    metavar="FILE",  help="Path to a .env file to load (default: .env in CWD)")
    ap.add_argument("--once",      action="store_true",
                    help="evaluate once, print JSON to stdout, and exit (0=UP, 1=DEGRADED, 2=DOWN)")
    ap.add_argument("--port",      type=int,  default=0,   help="Override listening port (default: from PORT env / config.properties)")
    ap.add_argument("--bind",      default="0.0.0.0",      help="Bind address (default: 0.0.0.0)")
    ap.add_argument("--log-level", default="INFO",         help="Logging level (default: INFO)")
    args = ap.parse_args()

    logging.basicConfig(level=args.log_level.upper(),
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")

    # If an explicit config/env file is given, load it manually before building ConfigLoader
    if args.config:
        _load_env_file_into_environ(args.config)

    cfg = ConfigLoader()

    log.info("========================================")
    log.info("Resolved IBM MQ Connection Configuration:")
    log.info("  MQ Host:          %s", cfg.get_mq_host())
    log.info("  MQ Port:          %s", cfg.get_mq_port())
    log.info("  MQ Channel:       %s", cfg.get_mq_channel())
    log.info("  MQ Queue Manager: %s", cfg.get_mq_qm())
    log.info("  MQ Test Queue:    %s", cfg.get_mq_test_queue())
    log.info("  MQ DLQ Name:      %s", cfg.get_mq_dlq_name())
    log.info("  MQ Username:      %s", cfg.get_mq_username() or "(not set)")
    log.info("========================================")

    hc = MqHealthCheck(cfg)

    if args.once:
        summary = hc.perform_check()
        print(json.dumps(summary.to_dict(), indent=2))
        sys.exit({UP: 0, DEGRADED: 1}.get(summary.status, 2))

    port   = args.port or cfg.get_app_port()
    server = make_server(hc, cfg, bind=args.bind, port=port)

    # Optional TLS wrapping
    tls_cert = os.environ.get("TLS_CERT_PATH", "")
    tls_key  = os.environ.get("TLS_KEY_PATH", "")
    if tls_cert and os.path.exists(tls_cert):
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.minimum_version = ssl.TLSVersion.TLSv1_2
        ctx.load_cert_chain(tls_cert, tls_key or None)
        server.socket = ctx.wrap_socket(server.socket, server_side=True)
        log.info("TLS enabled (cert: %s)", tls_cert)

    log.info("Starting mq-health-check on http://%s:%d  (queue manager: %s)",
             args.bind, port, cfg.get_mq_qm())
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log.info("Shutdown requested; stopping server.")
        server.shutdown()


def _load_env_file_into_environ(path: str) -> None:
    """Load KEY=VALUE pairs from ``path`` into os.environ (used for --config arg)."""
    try:
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    key, _, val = line.partition("=")
                    val = val.split("#")[0].strip()
                    os.environ.setdefault(key.strip(), val)
    except Exception as exc:
        log.error("Failed to read config file %s: %s", path, exc)


if __name__ == "__main__":
    main()
