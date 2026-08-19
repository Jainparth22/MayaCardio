# Deploying the cloud service

This puts the BeatKeeper cloud service on one Linux server, behind HTTPS. The
device pipeline keeps running on your own machine and uploads to it.

**Use simulated data only.** There is one shared API key, no user accounts, no
rate limiting and no encryption of the database at rest. This is not suitable
for real patient data.

## What you need

- A Linux server (VM) with a persistent disk, with Docker and the Compose plugin
- A domain or subdomain whose DNS A record points to the server
- Ports 80 and 443 open to the internet; no other service using them

Serverless hosts, and hosts that wipe the disk on restart, will not work: the
service stores everything in a SQLite database.

## Deploy

On the server:

```bash
git clone <your-repository-url> beatkeeper
cd beatkeeper
cp .env.example .env
openssl rand -hex 32        # paste the output into BEATKEEPER_API_KEY in .env
nano .env                   # set DOMAIN and BEATKEEPER_API_KEY
chmod 600 .env
docker compose up -d --build
```

Check it:

```bash
docker compose ps
curl https://YOUR_DOMAIN/health                                          # {"status": "ok"}
curl -s -o /dev/null -w '%{http_code}\n' https://YOUR_DOMAIN/v1/sessions   # 401 without the key
```

## Point a device at it

From the project folder on your own machine, start the simulator in one terminal:

```powershell
$env:PYTHONPATH = "src"
python -m beatkeeper.simulator --duration 120
```

and the pipeline in a second one:

```powershell
$env:PYTHONPATH = "src"
$env:BEATKEEPER_CLOUD_URL = "https://YOUR_DOMAIN"
$env:BEATKEEPER_API_KEY = "the key from .env"
python -m beatkeeper run
```

On Linux or macOS use `export NAME=value` instead of `$env:NAME = "value"`.

When the session has finished, check the cloud's copy against the simulator's
ground truth (a third terminal with the same three variables set):

```powershell
python -m beatkeeper export --session latest --from cloud > cloud.ndjson
python -m beatkeeper.verify --export cloud.ndjson --samples 48000
```

`--samples` is the session length in seconds times 400.

## Operate

| Task | Command |
|---|---|
| Logs | `docker compose logs -f cloud` |
| Update after `git pull` | `docker compose up -d --build` |
| Stop (data is kept) | `docker compose down` |
| Delete the service **and all data** | `docker compose down -v` |
| Disk used | `docker system df -v` |

Back up the database while the service is running:

```bash
docker compose exec cloud python -c "import sqlite3; s=sqlite3.connect('/data/cloud.db'); d=sqlite3.connect('/data/backup.db'); s.backup(d); d.close()"
docker compose cp cloud:/data/backup.db ./backup.db
```

Nothing deletes old sessions, so watch the free disk space.

## If something fails

- **`set DOMAIN in .env` or `set BEATKEEPER_API_KEY in .env`:** the value is missing.
- **Certificate errors:** the DNS record does not point here yet, or port 80 or
  443 is blocked. See `docker compose logs proxy`.
- **Device stays offline:** check `/health` from the device's machine.
- **`401 unauthorized`:** the device's key differs from the server's.

## After deploying

The latency and catch-up figures in [EVIDENCE.md](EVIDENCE.md) were measured on
loopback. They do not describe a deployed server; measure again over the real
network before quoting any number for it.
