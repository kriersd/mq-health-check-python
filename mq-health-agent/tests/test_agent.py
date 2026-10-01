"""Tests against a mock mqweb server. Run: python3 -m unittest discover tests"""
import json
import os
import sys
import tempfile
import threading
import unittest
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import mq_health_agent as m  # noqa: E402


def ok(params_list):
    return {"commandResponse": [{"completionCode": 0, "reasonCode": 0, "parameters": p} for p in params_list],
            "overallCompletionCode": 0, "overallReasonCode": 0}


NOT_FOUND = {"commandResponse": [{"completionCode": 2, "reasonCode": 3065, "message": ["AMQ8420I: Channel Status not found."]}],
             "overallCompletionCode": 2, "overallReasonCode": 3008}


class MockMQWeb:
    """Configurable fake of the mqweb admin + messaging endpoints."""

    def __init__(self):
        self.state = "running"
        self.listener_status = "RUNNING"
        self.chl_stopped = False
        self.instances = 3
        self.maxinst = 100
        self.chlauth_block = False
        self.loginuse = 20
        self.put_status = 201
        self.queue = {"curdepth": 10, "maxdepth": 5000, "put": "ENABLED", "get": "ENABLED"}
        self.down = False
        mock = self

        class H(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def _json(self, code, body, headers=None):
                data = json.dumps(body).encode()
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                for k, v in (headers or {}).items():
                    self.send_header(k, v)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def do_GET(self):
                if mock.down:
                    return self._json(500, {})
                if self.path.startswith("/ibmmq/rest/v2/admin/qmgr/QM1"):
                    return self._json(200, {"qmgr": [{"name": "QM1", "state": mock.state}]})
                self._json(404, {})

            def do_DELETE(self):
                self._json(200, {}) if "/messaging/" in self.path else self._json(404, {})

            def do_POST(self):
                if mock.down:
                    return self._json(500, {})
                body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
                assert self.headers.get("ibm-mq-rest-csrf-token"), "CSRF header missing"
                if "/messaging/" in self.path:
                    if mock.put_status == 201:
                        return self._json(201, {}, {"ibm-mq-md-messageId": "414d5120514d31"})
                    return self._json(mock.put_status, {"error": [{"msgId": "MQWB0302E", "reasonCode": 2102,
                                                                  "message": "resource problem"}]})
                cmd = json.loads(body)
                q = cmd["qualifier"]
                if q == "lsstatus":
                    return self._json(200, ok([{"port": 1414, "status": mock.listener_status}]))
                if q == "channel":
                    return self._json(200, ok([{"chltype": "SVRCONN", "maxinst": mock.maxinst, "maxinstc": 999999999}]))
                if q == "chstatus":
                    if mock.chl_stopped:
                        return self._json(200, ok([{"status": "STOPPED", "conname": ""}]))
                    if mock.instances == 0:
                        return self._json(200, NOT_FOUND)
                    return self._json(200, ok([{"status": "RUNNING", "conname": f"10.0.0.{i}"}
                                               for i in range(mock.instances)]))
                if q == "chlauth":
                    assert cmd["parameters"]["match"] == "runcheck"
                    if mock.chlauth_block:
                        return self._json(200, ok([{"chlauth": "APP.SVRCONN", "type": "BLOCKADDR"}]))
                    return self._json(200, ok([{"chlauth": "APP.SVRCONN", "type": "ADDRESSMAP", "usersrc": "MAP"}]))
                if q == "qmstatus":
                    return self._json(200, ok([{"status": "RUNNING", "loginuse": mock.loginuse}]))
                if q == "qlocal":
                    return self._json(200, ok([mock.queue]))
                self._json(400, {})

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), H)
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()


class AgentTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.mkdtemp()
        cls.qm_ini = os.path.join(cls.tmp, "qm.ini")
        with open(cls.qm_ini, "w") as fh:
            fh.write("Log:\n   LogPrimaryFiles=3\nCHANNELS:\n   MaxChannels=50\n   MaxActiveChannels=40\n")

    def setUp(self):
        self.mock = MockMQWeb()
        self.config = {
            "queue_manager": "QM1",
            "qmgr_data_dir": self.tmp,
            "interval_seconds": 1,
            "check_timeout_seconds": 5,
            "rest": {"url": self.mock.url, "user": "u", "ca_bundle": False, "timeout_seconds": 2},
            "checks": {
                "qmgr_status": {},
                "listener": {"port": 1414},
                "svrconn_channel": {"channels": ["APP.SVRCONN"]},
                "max_channels": {"qm_ini": self.qm_ini},
                "chlauth": {"probes": [{"channel": "APP.SVRCONN", "address": "10.1.1.1", "clntuser": "app"}]},
                "log_usage": {},
                "put_get": {"queue": "HEALTH.CHECK.Q"},
                "app_queues": {"queues": ["APP.REQUEST.Q"]},
                "disk_space": {"paths": [self.tmp], "warn_pct": 101, "fail_pct": 101},
            },
        }

    def tearDown(self):
        self.mock.server.shutdown()

    def run_agent(self):
        return m.Agent(self.config).evaluate()

    def test_all_healthy(self):
        r = self.run_agent()
        self.assertEqual(r["status"], "UP", json.dumps(r, indent=1))

    def test_qmgr_ended(self):
        self.mock.state = "ended immediately"
        r = self.run_agent()
        self.assertEqual(r["status"], "DOWN")
        self.assertIn("qmgr_status", r["failing_member_checks"])

    def test_listener_stopped(self):
        self.mock.listener_status = "STOPPING"
        self.assertEqual(self.run_agent()["status"], "DOWN")

    def test_channel_stopped(self):
        self.mock.chl_stopped = True
        r = self.run_agent()
        self.assertIn("svrconn_channel", r["failing_member_checks"])

    def test_no_channel_instances_is_ok(self):
        self.mock.instances = 0
        self.assertEqual(self.run_agent()["status"], "UP")

    def test_maxinst_reached(self):
        self.mock.maxinst = 3
        self.assertIn("svrconn_channel", self.run_agent()["failing_member_checks"])

    def test_maxinst_warn(self):
        self.mock.maxinst = 4  # 3/4 = 75% < 85 -> OK; 4/4 would fail
        self.mock.instances = 4
        self.mock.maxinst = 5  # 80% -> OK
        self.assertEqual(self.run_agent()["checks"]["svrconn_channel"]["state"], "OK")
        self.mock.instances = 9
        self.mock.maxinst = 10  # 90% -> WARN
        self.assertEqual(self.run_agent()["checks"]["svrconn_channel"]["state"], "WARN")

    def test_max_channels_from_qm_ini(self):
        self.mock.instances = 39  # limit = min(50, 40) = 40 -> 97.5% WARN
        self.mock.maxinst = 999
        r = self.run_agent()
        self.assertEqual(r["checks"]["max_channels"]["state"], "WARN")
        self.mock.instances = 40
        self.assertIn("max_channels", self.run_agent()["failing_member_checks"])

    def test_chlauth_blocked(self):
        self.mock.chlauth_block = True
        self.assertIn("chlauth", self.run_agent()["failing_member_checks"])

    def test_log_full(self):
        self.mock.loginuse = 97
        self.assertIn("log_usage", self.run_agent()["failing_member_checks"])

    def test_put_fails(self):
        self.mock.put_status = 500 - 1  # 499: an MQ error response, not a REST outage
        r = self.run_agent()
        self.assertIn("put_get", r["failing_member_checks"])
        self.assertIn("2102", r["checks"]["put_get"]["detail"])

    def test_app_queue_full_is_pool_scope(self):
        self.mock.queue = {"curdepth": 5000, "maxdepth": 5000, "put": "ENABLED", "get": "ENABLED"}
        r = self.run_agent()
        self.assertEqual(r["status"], "DEGRADED")
        self.assertEqual(r["checks"]["app_queues"]["state"], "FAIL")

    def test_app_queue_scope_override(self):
        self.config["checks"]["app_queues"]["scope"] = "member"
        self.mock.queue = {"curdepth": 1, "maxdepth": 5000, "put": "DISABLED", "get": "ENABLED"}
        self.assertEqual(self.run_agent()["status"], "DOWN")

    def test_rest_down_without_client_check_is_down(self):
        self.mock.down = True
        r = self.run_agent()
        self.assertEqual(r["status"], "DOWN")
        self.assertEqual(r["failing_member_checks"], ["qmgr_status"])

    def test_rest_down_with_client_ok_is_degraded(self):
        self.mock.down = True
        m.CHECKS["client_connect"] = lambda agent, cfg: (m.OK, "stub client ok")
        m.pymqi = m.pymqi or object()
        try:
            self.config["checks"]["client_connect"] = {"channel": "APP.SVRCONN"}
            r = self.run_agent()
            self.assertEqual(r["status"], "DEGRADED", json.dumps(r, indent=1))
        finally:
            m.CHECKS["client_connect"] = m.check_client_connect

    def test_qm_ini_defaults(self):
        p = os.path.join(self.tmp, "empty.ini")
        open(p, "w").close()
        self.assertEqual(m._qm_ini_limits(p), (100, 100))

    def test_http_endpoint(self):
        agent = m.Agent(self.config)
        agent.evaluate()
        srv = ThreadingHTTPServer(("127.0.0.1", 0), m.make_handler(agent, []))
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        url = f"http://127.0.0.1:{srv.server_address[1]}/health"
        with urllib.request.urlopen(url) as resp:
            self.assertEqual(resp.status, 200)
            self.assertEqual(json.load(resp)["status"], "UP")
        self.mock.state = "ended"
        agent.evaluate()
        with self.assertRaises(urllib.error.HTTPError) as ctx:
            urllib.request.urlopen(url)
        self.assertEqual(ctx.exception.code, 503)
        agent.latest_at = 0  # simulate a stuck loop
        with self.assertRaises(urllib.error.HTTPError) as ctx:
            urllib.request.urlopen(url)
        self.assertEqual(json.load(ctx.exception)["status"], "STALE")
        srv.shutdown()

    def test_allow_from(self):
        agent = m.Agent(self.config)
        agent.evaluate()
        srv = ThreadingHTTPServer(("127.0.0.1", 0), m.make_handler(agent, ["10.99.0.0/16"]))
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        with self.assertRaises(urllib.error.HTTPError) as ctx:
            urllib.request.urlopen(f"http://127.0.0.1:{srv.server_address[1]}/health")
        self.assertEqual(ctx.exception.code, 403)
        srv.shutdown()

    def test_tls_cert_expiry(self):
        pem = os.path.join(self.tmp, "c.pem")
        key = os.path.join(self.tmp, "k.pem")
        os.system(f"openssl req -x509 -newkey rsa:2048 -nodes -keyout {key} -out {pem} -days 10 "
                  f"-subj /CN=qm1 >/dev/null 2>&1")
        state, detail = m.check_tls_cert(None, {"pem": pem, "warn_days": 30})
        self.assertEqual(state, "WARN", detail)
        state, _ = m.check_tls_cert(None, {"pem": pem, "warn_days": 5})
        self.assertEqual(state, "OK")


if __name__ == "__main__":
    unittest.main()
