# IBM MQ High-Availability Health Monitor — Python

A production-grade, self-contained **Python 3.10+** application that exposes a REST
health-checking API and serves an embedded HTML monitoring dashboard. Designed to run
side-by-side with an IBM MQ Queue Manager and provide intelligent status signalling to
an **F5 BIG-IP load balancer** to support high-availability pooling.

> **This is the Python implementation.** A Java version of the same application is
> available at [kriersd/mq-health-check](https://github.com/kriersd/mq-health-check).
> Both expose the same REST API shape and F5 pooling logic — choose the runtime that
> fits your environment.

---

## Overview

In enterprise architectures, naive TCP port-ping monitors can cause catastrophic
cascading cluster outages. If a queue manager experiences a localised, non-fatal issue
(a full application queue, DLQ backlog, etc.), dropping the entire node from the load
balancer pool forces surviving nodes to absorb the extra load, cascading the failure.

This application solves the problem by exposing `GET /health/mq` which performs
**12 comprehensive subsystem checks** and segregates failures into two F5-safe states:

| Status | HTTP | Meaning | F5 action |
|---|---|---|---|
| `UP` | 200 | All checks pass | Keep in pool |
| `DEGRADED` | 200 | Only pool-scope checks failed | Keep in pool — alert only |
| `DOWN` | 503 | A member-scope check failed | Drop this member immediately |

**Member failures** are host-localised (dead TCP port, offline queue manager, full disk,
expired certificate). F5 drops this member.

**Pool failures** are cluster-wide or logical (full app queue, DLQ backlog, LDAP
outage). F5 keeps the member active — removing it would only cascade the failure across
the rest of the pool.

---

## How It Works

### Request Flow

```
[F5 BIG-IP / Client]
        │
        │  GET /health/mq
        ▼
[ThreadingHTTPServer]
        │
        └──► [MqHealthCheck Engine]
                      │
             ├─►  1. TCP Listener Check         (socket)
             ├─►  2. Queue Manager Connect       (pymqi)
             ├─►  3. Client Credentials Connect  (pymqi + TLS)
             ├─►  4. Max Channels Check          (qm.ini parse)
             ├─►  5. Disk Space Check            (shutil.disk_usage)
             ├─►  6. Certificate Expiry Check    (openssl CLI)
             ├─►  7. Test Queue PUT/GET          (pymqi INQUIRE/BROWSE)
             ├─►  8. DLQ Depth Check             (pymqi MQIA_CURRENT_Q_DEPTH)
             ├─►  9. App Queue Depth Check       (pymqi)
             ├─► 10. LDAP Reachability           (socket)
             ├─► 11. Channel Status              (pymqi / config)
             └─► 12. Host OS Resources           (os.getloadavg + resource.getrusage)
```

### Key Design Points

- **On-demand checking:** Every `GET /health/mq` runs all enabled checks synchronously.
  MQ connections are opened, queried, and closed cleanly in `finally` blocks.
- **pymqi is optional:** If not installed, MQ-client-dependent checks gracefully report
  `SKIP` (transparent to the overall status). TCP, disk, cert, LDAP, and host resource
  checks always run. Once `pymqi` is installed on the target MQ server host all skipped
  checks automatically activate — no code changes required.
- **Zero external dependencies for the server:** Only Python stdlib is required to
  start the HTTP server and serve the dashboard.

---

## Technology Stack

| Category | Technology | Notes |
|---|---|---|
| Runtime | Python 3.10+ | No external framework for the server |
| HTTP Server | `http.server.ThreadingHTTPServer` (stdlib) | Zero dependencies |
| MQ Client | `pymqi` (optional) | Install + IBM MQ client libs for live checks |
| Cert Checking | `openssl` CLI | Must be on PATH |
| Configuration | `.env` file + OS env vars | Single config file — no properties files |
| Dashboard | Self-contained HTML + vanilla JS | Served from `static/index.html` |
| Testing | `unittest` (stdlib) | 48 tests, zero external test dependencies |

---

## Project Structure

```
.
├── mq_health_check.py       # Entry point — ConfigLoader, 12 checks, HTTP server
├── .env.sample              # Complete config template — copy to .env and fill in
├── .env                     # Your local config (gitignored — never commit this)
├── requirements.txt         # Python deps (pymqi optional)
├── mq-health-check.service  # Systemd unit for Linux production hosts
├── static/
│   └── index.html           # HTML health dashboard
└── tests/
    └── test_health_check.py # 48-test unit suite
```

---

## Configuration

There is **one configuration file: `.env`**. Copy the sample and fill in your values:

```bash
cp .env.sample .env
# Edit .env — never commit it
```

Priority order (highest wins):

```
OS environment variables  >  .env file  >  built-in defaults
```

### All Variables

| Variable | Default | Purpose |
|---|---|---|
| `PORT` | `8080` | HTTP port for the dashboard and REST API |
| `APP_ENV` | `Production` | Environment label shown on the dashboard |
| `MQ_HOST` | *(required)* | Hostname or IP of the IBM MQ server |
| `MQ_PORT` | `1414` | MQ Listener port |
| `MQ_CHANNEL` | `SYSTEM.DEF.SVRCONN` | SVRCONN channel name |
| `MQ_QUEUE_MANAGER` | `QM1` | Queue Manager name |
| `MQ_TEST_QUEUE` | `DEV.QUEUE.1` | Queue for the test_queue check |
| `MQ_DLQ_NAME` | `SYSTEM.DEAD.LETTER.QUEUE` | Dead Letter Queue for depth check |
| `MQ_INI_PATH` | `/var/mqm/qmgrs/QM1/qm.ini` | qm.ini for max_channels check |
| `MQ_DATA_PATH` | `/var/mqm` | Filesystem path for disk_space check |
| `MQ_KEYSTORE_PATH` | *(empty)* | PEM or PKCS12 file for cert expiry check |
| `MQ_KEYSTORE_PASSWORD` | *(empty)* | PKCS12 password (blank for PEM) |
| `MQ_USERNAME` | *(empty)* | MQ authentication user |
| `MQ_PASSWORD` | *(empty)* | MQ authentication password |
| `MQ_SSL_CIPHER_SUITE` | *(empty)* | SSL cipher (blank = plain TCP) |
| `CHECK_LDAP_SERVERS` | *(empty)* | Comma-separated `host:port` LDAP endpoints |
| `CHECK_<NAME>_ENABLED` | `true` | Enable/disable any of the 12 checks |
| `CHECK_<NAME>_SEVERITY` | *(see below)* | Override F5 severity for any check |
| `CHECK_DLQ_DEPTH_THRESHOLD` | `10` | Max DLQ depth before DEGRADED |
| `CHECK_APP_QUEUE_DEPTH_QUEUES` | *(empty)* | Comma-separated app queues to check |
| `CHECK_APP_QUEUE_DEPTH_THRESHOLD_PCT` | `90` | % of MaxDepth before DEGRADED |
| `CHECK_CHANNEL_STATUS_CHANNELS` | *(empty)* | Comma-separated channels to verify |

### Default F5 Severities

| Check | Default | Rationale |
|---|---|---|
| `listener` | `member_failure` | Dead TCP port = this host is offline |
| `qmgr_connect` | `member_failure` | Can't connect = queue manager is down |
| `client_connect` | `member_failure` | Auth/TLS failure = all clients will fail |
| `max_channels` | `member_failure` | MaxChannels reached = no new connections |
| `disk_space` | `member_failure` | Full disk = queue manager will hang |
| `cert_expiry` | `member_failure` | Expired cert = all TLS connections fail |
| `test_queue` | `pool_failure` | Queue issues may affect all members |
| `dlq_depth` | `pool_failure` | DLQ backlog is a cluster-wide concern |
| `app_queue_depth` | `pool_failure` | Full app queue affects all members |
| `ldap` | `pool_failure` | LDAP outage is cluster-wide |
| `channel_status` | `pool_failure` | Channel issues may be cluster-wide |
| `host_resources` | `member_failure` | CPU/RAM exhaustion is host-specific |

---

## Running the Application

### 1. Run locally

```bash
cp .env.sample .env
# Set at minimum: MQ_HOST, MQ_QUEUE_MANAGER, MQ_CHANNEL, MQ_USERNAME, MQ_PASSWORD
python3 mq_health_check.py
```

Open `http://localhost:8080` for the dashboard, or call the API directly:

```bash
curl http://localhost:8080/health/mq
```

### 2. Evaluate once (CI / smoke test)

```bash
python3 mq_health_check.py --once
# Exit code: 0 = UP, 1 = DEGRADED, 2 = DOWN
```

### 3. Run in production (systemd)

```bash
sudo mkdir -p /opt/mq-health-check
sudo cp mq_health_check.py /opt/mq-health-check/
sudo cp -r static/           /opt/mq-health-check/
sudo cp .env                 /opt/mq-health-check/   # your filled-in .env
sudo cp mq-health-check.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now mq-health-check
```

### 4. Install pymqi for live MQ connection checks

`pymqi` is a C extension that wraps the IBM MQ C client library. It is **only required
on the target MQ server host** — you do not need it on a development laptop to run,
test, or develop the application.

#### Checks that require `pymqi`

| Check | What it does when pymqi is present |
|---|---|
| **QMgr Connect** | Direct bindings connect to the local queue manager |
| **Client Connect** | Full client connect with credentials + optional TLS |
| **Test Queue** | Put/Get a test message to verify queue access |
| **DLQ Depth** | Query dead letter queue depth via PCF |
| **App Queue Depth** | Query application queue depths via PCF |

#### Checks that never require `pymqi`

| Check | How it works |
|---|---|
| **Listener** | Pure TCP socket connect |
| **Max Channels** | Reads `qm.ini` — local file parse only |
| **Disk Space** | `shutil.disk_usage()` on the MQ data path |
| **Cert Expiry** | Runs `openssl x509` on the keystore file |
| **LDAP** | TCP socket connect to each configured LDAP server |
| **Channel Status** | Piggybacked on the existing qmgr connection |
| **Host Resources** | `os.getloadavg()` + `resource.getrusage()` |

#### Installation

The IBM MQ client or server libraries must already be installed on the host (they will
be on any MQ server). Then:

```bash
# Ensure the MQ library path is on LD_LIBRARY_PATH
export LD_LIBRARY_PATH=/opt/mqm/lib64:$LD_LIBRARY_PATH

# Install pymqi
pip3 install pymqi
```

> The `mq-health-check.service` systemd unit already sets `LD_LIBRARY_PATH=/opt/mqm/lib64`
> so this is handled automatically in production.

Without `pymqi`, the five MQ-connection-dependent checks report `SKIP` (shown as a grey
badge on the dashboard). `SKIP` results are transparent — they do not affect the overall
`UP`/`DEGRADED`/`DOWN` status. All other checks run normally.

---

## API Reference

### `GET /health/mq`

Evaluates all 12 subsystems. Returns HTTP 200 (`UP` or `DEGRADED`) or 503 (`DOWN`).

```json
{
  "status":       "UP | DEGRADED | DOWN",
  "queueManager": "QM1",
  "timestamp":    "2026-10-02T16:00:00.000Z",
  "details":      "All enabled health checks passed successfully.",
  "checks": [
    {
      "name":     "listener",
      "status":   "UP",
      "details":  "TCP connection established successfully.",
      "severity": "member_failure"
    }
  ]
}
```

### `GET /api/info`

Returns branding identity for the dashboard. Always HTTP 200.

```json
{ "name": "IBM MQ High-Availability Monitor", "env": "Production" }
```

### `GET /`

Serves the HTML monitoring dashboard (`static/index.html`).

### `GET /livez`

Liveness probe. Always returns `200 { "status": "alive" }`.

---

## Testing

```bash
python3 -m unittest discover tests -v
```

All 48 tests use only the Python standard library — no test dependencies to install.

### Test coverage

| Test class | What it validates |
|---|---|
| `TestConfigLoader` | Two-layer priority, bool/int parsing, severity defaults |
| `TestAllChecksPassing` | All enabled checks pass → status `UP` |
| `TestPoolFailureOnly` | Only pool-scope checks fail → `DEGRADED` (HTTP 200) |
| `TestMemberFailureOccurs` | Any member-scope check fails → `DOWN` (HTTP 503) |
| `TestCheckListener` | Socket probe: unreachable, unconfigured, reachable |
| `TestCheckDiskSpace` | Real path vs missing path fallback |
| `TestCheckMaxChannels` | qm.ini parse, missing stanza default, missing file |
| `TestCheckCertExpiry` | Expiring cert → DEGRADED, valid cert → UP, unconfigured → skip |
| `TestCheckLdap` | Reachable, unreachable, no servers configured |
| `TestCheckChannelStatus` | No channels, qmgr down, qmgr up |
| `TestCheckHostResources` | Returns UP with load/RSS metrics |
| `TestHTTPServer` | 200 UP, 200 DEGRADED, 503 DOWN, /api/info, /, /livez, 404 |
| `TestHealthSummarySerialisation` | `to_dict()` structure, JSON round-trip |

---

## Known Limitations / TODOs

- **`max_channels`** parses the local `qm.ini` Channels stanza. A production improvement
  is to query live channel counts via PCF (`MQCMD_INQUIRE_CHANNEL_STATUS`) using `pymqi`.
- **`channel_status`** reports health based on configured channel names. A production
  improvement is to query running state via PCF commands.
- **`cert_expiry`** relies on the `openssl` CLI being on `PATH`. Point
  `MQ_KEYSTORE_PATH` at a PEM file that `openssl x509` can read directly.
