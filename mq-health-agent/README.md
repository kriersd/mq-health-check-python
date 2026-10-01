# mq-health-agent

A small health service that runs on each IBM MQ host. The F5 BIG-IP calls it to decide whether that host should stay in the load-balancing pool.

The agent runs a set of checks against the local queue manager every few seconds and caches the result. The F5 then makes one simple HTTP call to `GET /health`.

| Status | HTTP | Meaning | F5 action |
|---|---|---|---|
| `UP` | 200 | All checks pass | Keep in pool |
| `DEGRADED` | 200 | A warning, or a failure that affects every member equally | Keep in pool, raise an alert |
| `DOWN` | 503 | A failure specific to this member | Mark member down |
| `STALE` | 503 | The agent's check loop has stopped producing results | Mark member down |

## Checks

| Check | How | Default scope |
|---|---|---|
| `qmgr_status` | REST `GET /admin/qmgr/{qm}`: state must be `running` | member |
| `listener` | `DISPLAY LSSTATUS(*)`: a listener on the MQ port must be RUNNING | member |
| `svrconn_channel` | `DISPLAY CHANNEL` + `DISPLAY CHSTATUS`: channel defined, not STOPPED, instances below MAXINST; warns at MAXINSTC per client | member |
| `max_channels` | Active instances from `DISPLAY CHSTATUS(*)` compared with `MaxChannels`/`MaxActiveChannels` read from the local qm.ini | member |
| `chlauth` | `DISPLAY CHLAUTH ... MATCH(RUNCHECK)` simulates configured client connections and fails if they would be blocked | member |
| `log_usage` | `DISPLAY QMSTATUS`: `LOGINUSE` % of recovery log | member |
| `put_get` | Messaging REST: persistent put and destructive get on `HEALTH.CHECK.Q` (catches log full) | member |
| `disk_space` | Local filesystem usage of the MQ data and log paths | member |
| `tls_cert` | Queue manager certificate expiry, via `runmqakm` extract or a PEM file | member |
| `client_connect` | A real MQCONNX through the app SVRCONN channel with TLS and CONNAUTH, plus optional put/get (needs `pymqi`) | member |
| `app_queues` | `DISPLAY QLOCAL`: depth against MAXDEPTH, PUT/GET inhibited | **pool** |
| `ldap` | TCP reachability of the LDAP servers | **pool** |

**Scope.** A failed `member` check returns 503, and the F5 fails this member over. A failed `pool` check returns 200 `DEGRADED` and is only logged. Pool-scope conditions, such as a full application queue or an LDAP outage, usually hit every member at once. If they failed members, the F5 would take the whole pool offline. Every check accepts `scope:` and `enabled:` overrides in config.yaml.

**When mqweb is down.** If the REST API is unreachable but `client_connect` passes, the agent reports `DEGRADED` instead of `DOWN`, because the queue manager is still serving clients. Without `client_connect`, an unreachable mqweb gives `DOWN`, because there's no evidence the queue manager is working. This is the main reason to enable `client_connect`.

**Hung queue manager.** A check that exceeds `check_timeout_seconds` counts as FAIL.

## Install (per MQ host)

```bash
sudo mkdir -p /opt/mq-health-agent /etc/mq-health-agent
sudo cp mq_health_agent.py /opt/mq-health-agent/
sudo cp config.example.yaml /etc/mq-health-agent/config.yaml   # then edit
sudo pip3 install -r requirements.txt     # drop pymqi if client_connect is disabled
printf 'MQHEALTH_REST_PASSWORD=...\nMQHEALTH_CLIENT_PASSWORD=...\n' | sudo tee /etc/mq-health-agent/secrets.env
sudo chmod 600 /etc/mq-health-agent/secrets.env
sudo cp mq-health-agent.service /etc/systemd/system/
sudo systemctl daemon-reload && sudo systemctl enable --now mq-health-agent

# Verify (prints the full JSON; exit code 0=UP, 1=DEGRADED, 2=DOWN)
sudo -u mqhealth MQHEALTH_REST_PASSWORD=... python3 /opt/mq-health-agent/mq_health_agent.py \
     --config /etc/mq-health-agent/config.yaml --once
curl -s http://localhost:9444/health
```

## MQ and mqweb setup

1. Run `runmqsc QM1 < mq-setup.mqsc`. It creates `HEALTH.CHECK.Q`, grants authorities, adds a CHLAUTH mapping for the agent's own client connection, and sets HBINT(60).
2. In mqwebuser.xml, give the `mqhealth` user the **MQWebAdminRO** role (for the DISPLAY commands) and the **MQWebUser** role (for the messaging put/get). Don't grant MQWebAdmin. Check against the customer's MQ level that MQWebAdminRO can issue DISPLAY commands through the `/admin/action/.../mqsc` endpoint.
3. Put the CA that signed the mqweb certificate in `rest.ca_bundle`.
4. For `client_connect`, create a client keystore that trusts the queue manager certificate, and set `key_repository` to its path without the `.kdb` extension.

## F5 setup

Apply `f5-monitor.tmsh`. It configures:
- the HTTP monitor on port 9444, matching `200`
- the pool monitor rule `tcp_half_open and mq_health_agent`
- `service-down-action reset`
- a 60-second slow ramp
- a TCP profile with a 180-second idle timeout

Restrict `http.allow_from` to the F5 self IPs.

## Operating notes

- **If the agent stops, the member goes down.** The F5 can't tell a dead agent from a dead queue manager. systemd restarts the agent within seconds. Monitor the service itself too.
- **Logs.** State changes are logged as JSON lines (`"event": "state_change"`) to journald. Forward them to the customer's alerting so pool-scope and WARN conditions get attention.
- **Not a replacement for MQ monitoring.** The agent decides routing. Queue depth trends, DLQ, channel errors and capacity still belong in the customer's MQ monitoring tooling.
- **Tests.** `python3 -m unittest discover tests` runs the logic against a mock mqweb server.

## Assumptions to verify on the customer's MQ level

The agent was tested against a mock of the MQ REST API, not a live queue manager. Before production, run `--once` against a test queue manager and confirm:
- the `runCommandJSON` parameter names (`loginuse`, `maxinst`, `curdepth` and so on) appear as expected
- the messaging API returns the `ibm-mq-md-messageId` header
- the CHLAUTH `RUNCHECK` output marks a blocked connection the way `check_chlauth` expects
