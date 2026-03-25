# Validator Docker Deployment

This directory contains a minimal validator deployment with:

- Docker image for `nexis validate`
- automatic image publish from GitHub to Docker Hub
- automatic local container updates via Watchtower
- optional Grafana log view (through Loki + Promtail)

## Files In This Directory

- `Dockerfile.validator`: validator image build file
- `healthcheck_validator.py`: container health check script
- `docker-compose.validator.yml`: main validator + watchtower stack
- `docker-compose.source-auth-validator.yml`: optional source-auth validator stack
- `docker-compose.owner-sync-worker.yml`: optional owner dataset sync worker stack
- `validator.env.example`: validator runtime environment template
- `compose.env.example`: host/compose environment template

Optional log stack:

- `docker-compose.observability.yml`: Loki + Promtail + Grafana services
- `loki-config.yml`: Loki configuration
- `promtail.yml`: Promtail scrape and relabel rules
- `grafana/provisioning/datasources/datasources.yml`: auto-provision Loki datasource

## Quick Start

```bash
cd docker
cp validator.env.example validator.env
cp compose.env.example compose.env
chmod 600 validator.env compose.env
```

Edit:

- `compose.env`: set `BT_WALLET_HOST_PATH` and `NEXIS_VALIDATOR_IMAGE`
- `validator.env`: fill wallet/network/storage/secrets values

Start validator + watchtower:

```bash
docker compose --env-file compose.env -f docker-compose.validator.yml up -d
```

Start source-auth validator (optional):

```bash
docker compose --env-file compose.env -f docker-compose.source-auth-validator.yml up -d
```

Start owner sync worker (optional):

```bash
docker compose --env-file compose.env -f docker-compose.owner-sync-worker.yml up -d
```

Check logs:

```bash
docker logs -f nexis-validator
docker logs -f nexis-watchtower
```

## GitHub To Docker Hub Publish

Workflow file:

- `.github/workflows/docker-publish.yml`

Required repository secrets:

- `DOCKERHUB_USERNAME`
- `DOCKERHUB_TOKEN`

Optional repository variable:

- `DOCKERHUB_IMAGE` (example: `myorg/nexisgen-validator`)

Current behavior:

- every push to `main` builds and pushes:
  - `latest`
  - immutable `sha-*` tag

## Watchtower Auto-Update

How updates happen:

- validator service is labeled for Watchtower
- Watchtower checks registry every `WATCHTOWER_POLL_INTERVAL` seconds
- when `latest` updates in Docker Hub, Watchtower recreates `nexis-validator`

## Rollback

1. Stop watchtower:

```bash
docker compose --env-file compose.env -f docker-compose.validator.yml stop watchtower
```

2. Pin an older digest in `compose.env`:

```bash
NEXIS_VALIDATOR_IMAGE=docker.io/<namespace>/nexisgen-validator@sha256:<digest>
```

3. Recreate validator:

```bash
docker compose --env-file compose.env -f docker-compose.validator.yml up -d validator
```

4. Start watchtower again:

```bash
docker compose --env-file compose.env -f docker-compose.validator.yml up -d watchtower
```

## Optional: Grafana Log View

Grafana does not collect container logs by itself.
To show validator logs in Grafana, Loki and Promtail are required:

- Promtail reads Docker logs from Docker socket
- Promtail sends logs to Loki
- Grafana queries Loki

Start optional observability stack:

```bash
docker compose --env-file compose.env \
  -f docker-compose.validator.yml \
  -f docker-compose.observability.yml \
  --profile observability \
  up -d
```

Access:

- Grafana: `http://localhost:3000` (default `admin/admin`, change immediately)

Explore query example:

- label filter: `compose_service="validator"`

Common pitfall after cleanup/redeploy:

- `promtail.yml` must be a regular file in this directory.
- If it is accidentally created as a directory, Promtail will crash with:
  `Unable to parse config: read /etc/promtail/promtail.yml: is a directory`

Quick recovery:

```bash
cd docker
rm -rf promtail.yml
# recreate promtail.yml file from repository version if needed
docker compose --env-file compose.env \
  -f docker-compose.validator.yml \
  -f docker-compose.observability.yml \
  --profile observability \
  up -d --force-recreate promtail
docker logs --tail 50 nexis-promtail
```

## Security Notes

- never commit `validator.env` or `compose.env`
- use Docker Hub access token, not account password
- Watchtower requires Docker socket access; run on trusted hosts only

## Validation Evidence API + Postgres

Run the evidence API stack (Postgres + FastAPI):

```bash
cd docker
cp validation-api.env.example validation-api.env
docker compose --env-file validation-api.env -f docker-compose.validation-api.yml up -d --build
```

Default endpoints:

- API: `http://localhost:8080/healthz`
- Postgres: `localhost:5432`

Point validators to the API by setting in `validator.env`:

```bash
NEXIS_VALIDATION_API_URL=http://<api-host>:8080/v1/validation-results
```
