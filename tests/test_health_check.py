"""
test_health_check.py — Unit tests for the IBM MQ Health Check (Python).

Tests the three core F5 pool-segregation scenarios described in the Java test suite:
  - testAllChecksPassing_ReturnsUP
  - testPoolFailureOnly_ReturnsDEGRADED
  - testMemberFailureOccurs_ReturnsDOWN

Also validates:
  - ConfigLoader two-layer priority (OS env var > .env file value > built-in default).
  - Each check function's edge-case logic (disk, cert, qm.ini, LDAP, DLQ depth).
  - The HTTP server returning correct status codes (200 UP, 200 DEGRADED, 503 DOWN).
  - The /api/info and / (dashboard) endpoints.

Run from the repo root:
    python3 -m unittest discover tests -v
"""

import json
import os
import shutil
import socket
import sys
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch, MagicMock

# Make the parent directory importable
sys.path.insert(0, str(Path(__file__).parent.parent))
import mq_health_check as m  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers / Shared fixtures
# ---------------------------------------------------------------------------

def _props(**kwargs) -> dict:
    """Build a minimal properties dict to instantiate ConfigLoader without touching disk."""
    base = {
        "MQ_HOST":          "127.0.0.1",
        "MQ_PORT":          "1414",
        "MQ_CHANNEL":       "SYSTEM.DEF.SVRCONN",
        "MQ_QUEUE_MANAGER": "QM1",
        "MQ_TEST_QUEUE":    "HEALTH.CHECK.Q",
        "MQ_DLQ_NAME":      "SYSTEM.DEAD.LETTER.QUEUE",
        "MQ_DATA_PATH":     "",   # empty → skip disk check path
    }
    base.update(kwargs)
    return base


def _config(**kwargs) -> m.ConfigLoader:
    return m.ConfigLoader(properties=_props(**kwargs))


def _hc(config: m.ConfigLoader) -> m.MqHealthCheck:
    return m.MqHealthCheck(config)


# ===========================================================================
# ConfigLoader Tests
# ===========================================================================

class TestConfigLoader(unittest.TestCase):
    """Verify the two-layer priority and all typed accessors."""

    def test_defaults(self):
        """Built-in defaults apply when no .env and no OS env vars are set."""
        cfg = m.ConfigLoader(properties={})
        self.assertEqual(cfg.get_app_port(), 8080)
        self.assertEqual(cfg.get_mq_port(), 1414)
        self.assertEqual(cfg.get_mq_qm(), "QM1")

    def test_env_file_value(self):
        """Values from the .env file (injected via properties dict) are used."""
        cfg = m.ConfigLoader(properties={"PORT": "9090"})
        self.assertEqual(cfg.get_app_port(), 9090)

    def test_os_env_overrides_env_file(self):
        """OS environment variables override .env file values."""
        with patch.dict(os.environ, {"PORT": "5555"}):
            cfg = m.ConfigLoader(properties={"PORT": "7777"})
            self.assertEqual(cfg.get_app_port(), 5555)

    def test_os_env_highest_priority(self):
        """OS environment variables take priority even when .env is empty."""
        with patch.dict(os.environ, {"PORT": "5555"}):
            cfg = m.ConfigLoader(properties={})
            self.assertEqual(cfg.get_app_port(), 5555)

    def test_bool_parsing(self):
        cfg = m.ConfigLoader(properties={"CHECK_LISTENER_ENABLED": "false"})
        self.assertFalse(cfg.is_check_enabled("listener"))

    def test_invalid_int_uses_default(self):
        cfg = m.ConfigLoader(properties={"PORT": "not_a_number"})
        self.assertEqual(cfg.get_app_port(), 8080)

    def test_check_severity_default(self):
        cfg = m.ConfigLoader(properties={})
        self.assertEqual(cfg.get_check_severity("listener"),   m.MEMBER_FAILURE)
        self.assertEqual(cfg.get_check_severity("dlq_depth"),  m.POOL_FAILURE)
        self.assertEqual(cfg.get_check_severity("app_queue_depth"), m.POOL_FAILURE)

    def test_check_severity_override(self):
        cfg = m.ConfigLoader(properties={"CHECK_LISTENER_SEVERITY": "pool_failure"})
        self.assertEqual(cfg.get_check_severity("listener"), m.POOL_FAILURE)

    def test_app_queues_parsing(self):
        cfg = m.ConfigLoader(properties={"CHECK_APP_QUEUE_DEPTH_QUEUES": " Q1 , Q2 , Q3 "})
        self.assertEqual(cfg.get_app_queues(), ["Q1", "Q2", "Q3"])

    def test_dlq_limit(self):
        cfg = m.ConfigLoader(properties={"CHECK_DLQ_DEPTH_THRESHOLD": "25"})
        self.assertEqual(cfg.get_dlq_limit(), 25)


# ===========================================================================
# Scenario 1 — testAllChecksPassing_ReturnsUP
# ===========================================================================

class TestAllChecksPassing(unittest.TestCase):
    """
    Mirrors Java: testAllChecksPassing_ReturnsUP
    When all enabled file/resource checks are healthy the overall status is UP.
    """

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        # Write a valid qm.ini
        ini = os.path.join(self.tmp, "qm.ini")
        with open(ini, "w") as f:
            f.write("Log:\n   LogPrimaryFiles=3\nCHANNELS:\n   MaxChannels=200\n")
        self.ini = ini

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _make_hc(self, extra_props: dict | None = None):
        props = _props(
            MQ_DATA_PATH=self.tmp,
            MQ_INI_PATH=self.ini,
            # Disable the checks that need live MQ connections
            CHECK_QMGR_CONNECT_ENABLED="false",
            CHECK_CLIENT_CONNECT_ENABLED="false",
            CHECK_TEST_QUEUE_ENABLED="false",
            CHECK_DLQ_DEPTH_ENABLED="false",
            CHECK_APP_QUEUE_DEPTH_ENABLED="false",
            # Disable cert check (no keystore configured)
            CHECK_CERT_EXPIRY_ENABLED="false",
            # Disable LDAP (no servers configured)
            CHECK_LDAP_ENABLED="false",
            # Listener check will fail (no real MQ), so disable
            CHECK_LISTENER_ENABLED="false",
        )
        if extra_props:
            props.update(extra_props)
        return m.MqHealthCheck(m.ConfigLoader(properties=props))

    def test_all_checks_passing_returns_up(self):
        hc = self._make_hc()
        summary = hc.perform_check()
        self.assertEqual(summary.status, m.UP,
                         f"Expected UP but got {summary.status}: {summary.details}")

    def test_summary_has_correct_fields(self):
        hc = self._make_hc()
        summary = hc.perform_check()
        d = summary.to_dict()
        self.assertIn("status",       d)
        self.assertIn("queueManager", d)
        self.assertIn("timestamp",    d)
        self.assertIn("details",      d)
        self.assertIn("checks",       d)
        self.assertIsInstance(d["checks"], list)

    def test_all_checks_have_required_fields(self):
        hc = self._make_hc()
        for check in hc.perform_check().checks:
            self.assertIn(check.status,   [m.UP, m.DEGRADED, m.DOWN, m.SKIP],
                          f"check {check.name} has unexpected status {check.status}")
            self.assertIn(check.severity, [m.MEMBER_FAILURE, m.POOL_FAILURE],
                          f"check {check.name} has unexpected severity {check.severity}")
            self.assertTrue(check.details, f"check {check.name} has empty details")


# ===========================================================================
# Scenario 2 — testPoolFailureOnly_ReturnsDEGRADED
# ===========================================================================

class TestPoolFailureOnly(unittest.TestCase):
    """
    Mirrors Java: testPoolFailureOnly_ReturnsDEGRADED
    When only pool-scope checks fail the status is DEGRADED (HTTP 200).
    F5 keeps the member active to prevent cascading cluster outage.
    """

    def _make_hc_with_patched_check(self, check_name: str, state: str, detail: str,
                                     severity: str = m.POOL_FAILURE):
        """Build an MqHealthCheck where a named check returns a specific result."""
        props = _props(
            CHECK_QMGR_CONNECT_ENABLED="false",
            CHECK_CLIENT_CONNECT_ENABLED="false",
            CHECK_TEST_QUEUE_ENABLED="false",
            CHECK_DLQ_DEPTH_ENABLED="false",
            CHECK_APP_QUEUE_DEPTH_ENABLED="false",
            CHECK_CERT_EXPIRY_ENABLED="false",
            CHECK_LDAP_ENABLED="false",
            CHECK_LISTENER_ENABLED="false",
            CHECK_DISK_SPACE_ENABLED="false",
            CHECK_MAX_CHANNELS_ENABLED="false",
            CHECK_HOST_RESOURCES_ENABLED="false",
            CHECK_CHANNEL_STATUS_ENABLED="false",
        )
        # Re-enable the targeted check
        props[f"CHECK_{check_name.upper()}_ENABLED"] = "true"
        props[f"CHECK_{check_name.upper()}_SEVERITY"] = severity

        hc = m.MqHealthCheck(m.ConfigLoader(properties=props))

        # Monkey-patch the check method to return the desired outcome
        method_name = f"_check_{check_name}"
        setattr(hc, method_name,
                lambda *args, **kwargs: m.CheckResult(check_name, state, detail, severity))
        return hc

    def test_pool_failure_returns_degraded(self):
        hc = self._make_hc_with_patched_check(
            "app_queue_depth", m.DEGRADED,
            "WARNING: Application queue 'APP.REQUEST.Q' depth is at 95.00% (4750/5000)",
            m.POOL_FAILURE
        )
        summary = hc.perform_check()
        self.assertEqual(summary.status, m.DEGRADED,
                         f"Expected DEGRADED but got {summary.status}: {summary.details}")

    def test_pool_failure_returns_http_200(self):
        """DEGRADED must map to HTTP 200 so F5 keeps this member active."""
        hc = self._make_hc_with_patched_check(
            "dlq_depth", m.DEGRADED,
            "WARNING: DLQ depth is 50 (Alert Limit: 10)",
            m.POOL_FAILURE
        )
        summary = hc.perform_check()
        # HTTP code mapping
        code = 503 if summary.status == m.DOWN else 200
        self.assertEqual(code, 200,
                         "DEGRADED must map to HTTP 200 to keep the F5 member active.")

    def test_ldap_pool_failure_returns_degraded(self):
        hc = self._make_hc_with_patched_check(
            "ldap", m.DOWN,
            "No LDAP server reachable: ldap1.example.com:636 (ConnectionRefusedError)",
            m.POOL_FAILURE
        )
        # Even though check state is DOWN, pool severity → DEGRADED overall
        summary = hc.perform_check()
        self.assertEqual(summary.status, m.DEGRADED)


# ===========================================================================
# Scenario 3 — testMemberFailureOccurs_ReturnsDOWN
# ===========================================================================

class TestMemberFailureOccurs(unittest.TestCase):
    """
    Mirrors Java: testMemberFailureOccurs_ReturnsDOWN
    When any member-scope check fails the status is DOWN (HTTP 503).
    F5 immediately drops this pool member.
    """

    def _make_hc_with_member_failure(self, check_name: str, detail: str):
        props = _props(
            CHECK_QMGR_CONNECT_ENABLED="false",
            CHECK_CLIENT_CONNECT_ENABLED="false",
            CHECK_TEST_QUEUE_ENABLED="false",
            CHECK_DLQ_DEPTH_ENABLED="false",
            CHECK_APP_QUEUE_DEPTH_ENABLED="false",
            CHECK_CERT_EXPIRY_ENABLED="false",
            CHECK_LDAP_ENABLED="false",
            CHECK_LISTENER_ENABLED="false",
            CHECK_DISK_SPACE_ENABLED="false",
            CHECK_MAX_CHANNELS_ENABLED="false",
            CHECK_HOST_RESOURCES_ENABLED="false",
            CHECK_CHANNEL_STATUS_ENABLED="false",
        )
        props[f"CHECK_{check_name.upper()}_ENABLED"] = "true"
        props[f"CHECK_{check_name.upper()}_SEVERITY"] = m.MEMBER_FAILURE

        hc = m.MqHealthCheck(m.ConfigLoader(properties=props))
        setattr(hc, f"_check_{check_name}",
                lambda *args, **kwargs: m.CheckResult(check_name, m.DOWN, detail, m.MEMBER_FAILURE))
        return hc

    def test_listener_down_returns_503(self):
        hc      = self._make_hc_with_member_failure("listener", "Connection to port 1414 failed")
        summary = hc.perform_check()
        self.assertEqual(summary.status, m.DOWN)
        code = 503 if summary.status == m.DOWN else 200
        self.assertEqual(code, 503, "Member failure must map to HTTP 503.")

    def test_disk_full_returns_down(self):
        hc      = self._make_hc_with_member_failure("disk_space", "CRITICAL: Disk utilization 98.00%")
        summary = hc.perform_check()
        self.assertEqual(summary.status, m.DOWN)

    def test_cert_expired_returns_down(self):
        hc      = self._make_hc_with_member_failure("cert_expiry", "Certificate is expired!")
        summary = hc.perform_check()
        self.assertEqual(summary.status, m.DOWN)

    def test_qmgr_connect_failure_returns_down(self):
        hc      = self._make_hc_with_member_failure("qmgr_connect", "MQ connection failed: ConnectionRefused")
        summary = hc.perform_check()
        self.assertEqual(summary.status, m.DOWN)


# ===========================================================================
# Individual check unit tests
# ===========================================================================

class TestCheckListener(unittest.TestCase):
    """Listener check: socket-level TCP probe."""

    def test_unconfigured_host_returns_down(self):
        cfg = m.ConfigLoader(properties=_props(MQ_HOST="CHANGE_ME"))
        hc  = m.MqHealthCheck(cfg)
        res = hc._check_listener()
        self.assertEqual(res.status, m.DOWN)
        self.assertIn("unconfigured", res.details.lower())

    def test_unreachable_host_returns_down(self):
        # Port 1 is almost certainly closed on localhost
        cfg = m.ConfigLoader(properties=_props(MQ_HOST="127.0.0.1", MQ_PORT="1"))
        hc  = m.MqHealthCheck(cfg)
        res = hc._check_listener()
        self.assertEqual(res.status, m.DOWN)

    def test_reachable_host_returns_up(self):
        """Open a local TCP server, then confirm the listener check passes."""
        srv = ThreadingHTTPServer(("127.0.0.1", 0), BaseHTTPRequestHandler)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        port = srv.server_address[1]
        try:
            cfg = m.ConfigLoader(properties=_props(MQ_HOST="127.0.0.1",
                                                    MQ_PORT=str(port)))
            hc  = m.MqHealthCheck(cfg)
            res = hc._check_listener()
            self.assertEqual(res.status, m.UP)
        finally:
            srv.shutdown()


class TestCheckDiskSpace(unittest.TestCase):
    """Disk space check against a real temporary directory."""

    def test_real_path_returns_up(self):
        tmp = tempfile.mkdtemp()
        try:
            cfg = m.ConfigLoader(properties=_props(MQ_DATA_PATH=tmp))
            hc  = m.MqHealthCheck(cfg)
            res = hc._check_disk_space()
            # A new temp dir should be well under 85%
            self.assertIn(res.status, [m.UP, m.DEGRADED])   # might WARN on very full CI disk
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def test_missing_path_returns_skip(self):
        cfg = m.ConfigLoader(properties=_props(MQ_DATA_PATH="/nonexistent/path/abc"))
        hc  = m.MqHealthCheck(cfg)
        res = hc._check_disk_space()
        self.assertEqual(res.status, m.SKIP)
        self.assertIn("skipped", res.details.lower())


class TestCheckMaxChannels(unittest.TestCase):
    """Max channels check reads the qm.ini Channels stanza."""

    def test_parses_maxchannels(self):
        tmp = tempfile.mkdtemp()
        ini = os.path.join(tmp, "qm.ini")
        try:
            with open(ini, "w") as fh:
                fh.write("Log:\n   LogPrimary=3\nCHANNELS:\n   MaxChannels=150\n")
            self.assertEqual(m.MqHealthCheck._parse_qm_ini_max_channels(ini), 150)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def test_missing_stanza_returns_default(self):
        tmp = tempfile.mkdtemp()
        ini = os.path.join(tmp, "qm.ini")
        try:
            with open(ini, "w") as fh:
                fh.write("Log:\n   LogPrimary=3\n")
            self.assertEqual(m.MqHealthCheck._parse_qm_ini_max_channels(ini), 100)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def test_missing_ini_returns_skip(self):
        cfg = m.ConfigLoader(properties=_props(MQ_INI_PATH="/nonexistent/qm.ini"))
        hc  = m.MqHealthCheck(cfg)
        res = hc._check_max_channels()
        self.assertEqual(res.status, m.SKIP)
        self.assertIn("skipped", res.details.lower())

    def test_valid_ini_returns_up(self):
        tmp = tempfile.mkdtemp()
        ini = os.path.join(tmp, "qm.ini")
        try:
            with open(ini, "w") as fh:
                fh.write("CHANNELS:\n   MaxChannels=200\n")
            cfg = m.ConfigLoader(properties=_props(MQ_INI_PATH=ini))
            hc  = m.MqHealthCheck(cfg)
            res = hc._check_max_channels()
            self.assertEqual(res.status, m.UP)
            self.assertIn("200", res.details)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)


class TestCheckCertExpiry(unittest.TestCase):
    """Certificate expiry check — uses openssl to create test certs."""

    @unittest.skipUnless(shutil.which("openssl"), "openssl not available on PATH")
    def test_expiring_soon_returns_degraded(self):
        tmp = tempfile.mkdtemp()
        pem = os.path.join(tmp, "cert.pem")
        key = os.path.join(tmp, "key.pem")
        try:
            import subprocess
            subprocess.run(
                ["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
                 "-keyout", key, "-out", pem, "-days", "10", "-subj", "/CN=qm1"],
                capture_output=True, check=True
            )
            cfg = m.ConfigLoader(properties=_props(MQ_KEYSTORE_PATH=pem))
            hc  = m.MqHealthCheck(cfg)
            res = hc._check_cert_expiry()
            self.assertEqual(res.status, m.DEGRADED, res.details)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    @unittest.skipUnless(shutil.which("openssl"), "openssl not available on PATH")
    def test_valid_cert_returns_up(self):
        tmp = tempfile.mkdtemp()
        pem = os.path.join(tmp, "cert.pem")
        key = os.path.join(tmp, "key.pem")
        try:
            import subprocess
            subprocess.run(
                ["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
                 "-keyout", key, "-out", pem, "-days", "365", "-subj", "/CN=qm1"],
                capture_output=True, check=True
            )
            cfg = m.ConfigLoader(properties=_props(MQ_KEYSTORE_PATH=pem))
            hc  = m.MqHealthCheck(cfg)
            res = hc._check_cert_expiry()
            self.assertEqual(res.status, m.UP, res.details)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def test_unconfigured_keystore_skips(self):
        cfg = m.ConfigLoader(properties=_props(MQ_KEYSTORE_PATH="CHANGE_ME"))
        hc  = m.MqHealthCheck(cfg)
        res = hc._check_cert_expiry()
        self.assertEqual(res.status, m.SKIP)
        self.assertIn("skipped", res.details.lower())


class TestCheckLdap(unittest.TestCase):
    """LDAP reachability check — uses a real local TCP server as a mock LDAP."""

    def test_no_servers_returns_up(self):
        cfg = m.ConfigLoader(properties=_props())
        hc  = m.MqHealthCheck(cfg)
        res = hc._check_ldap()
        self.assertEqual(res.status, m.UP)

    def test_reachable_server_returns_up(self):
        srv = ThreadingHTTPServer(("127.0.0.1", 0), BaseHTTPRequestHandler)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        port = srv.server_address[1]
        try:
            cfg = m.ConfigLoader(properties=_props(
                **{"check.ldap.servers": f"127.0.0.1:{port}"}
            ))
            hc  = m.MqHealthCheck(cfg)
            res = hc._check_ldap()
            self.assertEqual(res.status, m.UP)
        finally:
            srv.shutdown()

    def test_unreachable_server_returns_down(self):
        cfg = m.ConfigLoader(properties=_props(
            CHECK_LDAP_SERVERS="127.0.0.1:1",
            CHECK_LDAP_TIMEOUT="1",
        ))
        hc  = m.MqHealthCheck(cfg)
        res = hc._check_ldap()
        self.assertEqual(res.status, m.DOWN)


class TestCheckChannelStatus(unittest.TestCase):
    def test_no_channels_configured_returns_skip(self):
        cfg = m.ConfigLoader(properties=_props())
        hc  = m.MqHealthCheck(cfg)
        res = hc._check_channel_status(True)
        self.assertEqual(res.status, m.SKIP)

    def test_channels_with_qmgr_down_returns_down(self):
        cfg = m.ConfigLoader(properties=_props(
            CHECK_CHANNEL_STATUS_CHANNELS="APP.SVRCONN"
        ))
        hc  = m.MqHealthCheck(cfg)
        res = hc._check_channel_status(False)
        self.assertEqual(res.status, m.DOWN)

    def test_channels_with_qmgr_up_returns_up(self):
        cfg = m.ConfigLoader(properties=_props(
            CHECK_CHANNEL_STATUS_CHANNELS="APP.SVRCONN"
        ))
        hc  = m.MqHealthCheck(cfg)
        res = hc._check_channel_status(True)
        self.assertEqual(res.status, m.UP)


class TestCheckHostResources(unittest.TestCase):
    def test_returns_up(self):
        cfg = m.ConfigLoader(properties=_props())
        hc  = m.MqHealthCheck(cfg)
        res = hc._check_host_resources()
        self.assertEqual(res.status, m.UP)
        self.assertIn("Load Average", res.details)


# ===========================================================================
# HTTP server tests
# ===========================================================================

class TestHTTPServer(unittest.TestCase):
    """Validate the HTTP server returns correct codes and payloads."""

    def _start_server(self, status: str) -> tuple:
        """Return (server, port) with the health check patched to return ``status``."""
        props = _props(
            CHECK_QMGR_CONNECT_ENABLED="false",
            CHECK_CLIENT_CONNECT_ENABLED="false",
            CHECK_TEST_QUEUE_ENABLED="false",
            CHECK_DLQ_DEPTH_ENABLED="false",
            CHECK_APP_QUEUE_DEPTH_ENABLED="false",
            CHECK_CERT_EXPIRY_ENABLED="false",
            CHECK_LDAP_ENABLED="false",
            CHECK_LISTENER_ENABLED="false",
            CHECK_DISK_SPACE_ENABLED="false",
            CHECK_MAX_CHANNELS_ENABLED="false",
            CHECK_HOST_RESOURCES_ENABLED="false",
            CHECK_CHANNEL_STATUS_ENABLED="false",
        )
        cfg = m.ConfigLoader(properties=props)
        hc  = m.MqHealthCheck(cfg)

        fixed_summary = m.HealthSummary(status, "QM1", "2026-01-01T00:00:00Z",
                                          "test summary", [])
        hc.perform_check = lambda: fixed_summary

        srv = m.make_server(hc, cfg, bind="127.0.0.1", port=0)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        return srv, srv.server_address[1]

    def _get(self, port: int, path: str):
        url = f"http://127.0.0.1:{port}{path}"
        try:
            with urllib.request.urlopen(url) as resp:
                return resp.status, json.loads(resp.read())
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read())

    def test_up_returns_200(self):
        srv, port = self._start_server(m.UP)
        try:
            code, body = self._get(port, "/health/mq")
            self.assertEqual(code, 200)
            self.assertEqual(body["status"], m.UP)
        finally:
            srv.shutdown()

    def test_degraded_returns_200(self):
        srv, port = self._start_server(m.DEGRADED)
        try:
            code, body = self._get(port, "/health/mq")
            self.assertEqual(code, 200,
                             "DEGRADED must return HTTP 200 to keep the F5 pool member active.")
            self.assertEqual(body["status"], m.DEGRADED)
        finally:
            srv.shutdown()

    def test_down_returns_503(self):
        srv, port = self._start_server(m.DOWN)
        try:
            code, body = self._get(port, "/health/mq")
            self.assertEqual(code, 503,
                             "DOWN must return HTTP 503 so F5 drops this member.")
            self.assertEqual(body["status"], m.DOWN)
        finally:
            srv.shutdown()

    def test_api_info_endpoint(self):
        srv, port = self._start_server(m.UP)
        try:
            code, body = self._get(port, "/api/info")
            self.assertEqual(code, 200)
            self.assertIn("name", body)
            self.assertIn("env",  body)
        finally:
            srv.shutdown()

    def test_dashboard_returns_html(self):
        srv, port = self._start_server(m.UP)
        try:
            url = f"http://127.0.0.1:{port}/"
            with urllib.request.urlopen(url) as resp:
                content_type = resp.headers.get("Content-Type", "")
                self.assertIn("text/html", content_type)
        finally:
            srv.shutdown()

    def test_unknown_path_returns_404(self):
        srv, port = self._start_server(m.UP)
        try:
            code, _ = self._get(port, "/not/a/route")
            self.assertEqual(code, 404)
        finally:
            srv.shutdown()

    def test_livez_returns_200(self):
        srv, port = self._start_server(m.UP)
        try:
            code, body = self._get(port, "/livez")
            self.assertEqual(code, 200)
            self.assertEqual(body["status"], "alive")
        finally:
            srv.shutdown()


# ===========================================================================
# HealthSummary serialisation
# ===========================================================================

class TestHealthSummarySerialisation(unittest.TestCase):
    def test_to_dict_structure(self):
        check = m.CheckResult("listener", m.UP, "TCP ok", m.MEMBER_FAILURE)
        summary = m.HealthSummary(m.UP, "QM1", "2026-01-01T00:00:00Z",
                                   "All checks passed.", [check])
        d = summary.to_dict()
        self.assertEqual(d["status"],       m.UP)
        self.assertEqual(d["queueManager"], "QM1")
        self.assertEqual(len(d["checks"]),  1)
        self.assertEqual(d["checks"][0]["name"], "listener")

    def test_json_roundtrip(self):
        check   = m.CheckResult("disk_space", m.DOWN, "98% full", m.MEMBER_FAILURE)
        summary = m.HealthSummary(m.DOWN, "QM2", "2026-01-01T00:00:00Z", "Disk full", [check])
        raw     = json.dumps(summary.to_dict())
        parsed  = json.loads(raw)
        self.assertEqual(parsed["status"],   m.DOWN)
        self.assertEqual(parsed["checks"][0]["severity"], m.MEMBER_FAILURE)


if __name__ == "__main__":
    unittest.main()
