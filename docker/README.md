# Nexisgen Docker Deployment (v2)

The `nexis` image runs all four roles via the `command` override:

| Role                | Compose file                       | Notes                                              |
|---------------------|-------------------------------------|----------------------------------------------------|
| Validator (scoring) | `docker-compose.validator.yml`     | Runs VBench in sibling containers (host docker.sock).|
| Trainer (any validator) | `docker-compose.trainer.yml`   | NVIDIA GPUs + sibling containers.                  |
| Validation API      | `docker-compose.validation-api.yml`| FastAPI + Postgres; writes to `nexis_miner` bucket.|
| Observability       | `docker-compose.observability.yml` | Optional Loki + Promtail + Grafana.                |

## Files

- `Dockerfile.validator` — single image (ffmpeg, yt-dlp, docker CLI, `nexis`).
- `Dockerfile.validation-api` — FastAPI server image.
- `healthcheck_validator.py` — accepts `validate`, `mine`, `train`, `commit-credentials`.
- `validator.env.example`, `trainer.env.example`, `validation-api.env.example` — runtime env templates.
- `compose.env.example` — host-side compose interpolation vars.
- `sql/001_validation_evidence.sql` — Postgres bootstrap (matches v2 schema).

## Quick start (validator)

```bash
cd docker
cp validator.env.example  validator.env
cp compose.env.example    compose.env
chmod 600 validator.env compose.env

# Edit the two .env files.  Required at minimum:
#   compose.env: BT_WALLET_HOST_PATH
#   validator.env: NEXIS_MINER_ACCOUNT_ID + NEXIS_MINER_READ_*

docker compose --env-file compose.env -f docker-compose.validator.yml up -d
docker logs -f nexis-validator
```

The validator container needs `/var/run/docker.sock` mounted so it can spawn
`rendixnetwork/vbench:latest` on the host. The compose file already sets that up.

## Quick start (trainer — any validator)

```bash
cd docker
cp trainer.env.example  trainer.env
cp compose.env.example  compose.env
chmod 600 trainer.env compose.env

# Edit:
#   compose.env: BT_WALLET_HOST_PATH, NEXIS_TRAINER_MODELS_DIR, NEXIS_TRAINER_CONFIG_JSON,
#                NEXIS_ELIGIBILITY_HOST_PATH (host repo eligibility/ dir)
#   trainer.env: NEXIS_MINER_*_KEY (read+write), wallet

# Populate the model dir on the host once:
#   cd /path/to/nexisgen && pip install huggingface_hub && python download_model.py

docker compose --env-file compose.env -f docker-compose.trainer.yml up -d
docker logs -f nexis-trainer
```

The host paths in `NEXIS_TRAINER_MODELS_DIR`, `NEXIS_TRAINER_CONFIG_JSON`, and
`NEXIS_TRAINER_EVAL_DIR` are mounted **1:1** into the trainer container so that
when the trainer asks the host docker daemon to spawn `rendixnetwork/train:latest`
with `-v <those paths>`, the host actually has files at those locations.

## Quick start (validation API)

```bash
cd docker
cp validation-api.env.example validation-api.env
chmod 600 validation-api.env

# Required: NEXIS_MINER_*_KEY (read+write) so the API can write each
# validator's `{validator_hotkey}.json` score file into the shared bucket.

docker compose --env-file validation-api.env \
  -f docker-compose.validation-api.yml up -d --build
```

Default endpoints:

- API: `http://localhost:8080/healthz`
- Postgres: `localhost:5432`

Validators point at the API by setting in `validator.env`:

```bash
NEXIS_VALIDATION_API_URL=http://<api-host>:8080/v1/training-scores
```

## GitHub → Docker Hub publish

Workflow: `.github/workflows/docker-publish.yml`

Repo secrets required:

- `DOCKERHUB_USERNAME`
- `DOCKERHUB_TOKEN`

Optional repo variable: `DOCKERHUB_IMAGE` (e.g. `myorg/nexisgen-validator`).

Every push to `main` builds and pushes `latest` plus an immutable `sha-*` tag.

## Watchtower auto-update

`docker-compose.validator.yml` labels the validator for Watchtower; it polls the
registry every `WATCHTOWER_POLL_INTERVAL` seconds and recreates the container on
new digests. To freeze a deployment, pin a digest in `compose.env`:

```bash
NEXIS_VALIDATOR_IMAGE=docker.io/<namespace>/nexisgen-validator@sha256:<digest>
```

## Optional Grafana log view

```bash
docker compose --env-file compose.env \
  -f docker-compose.validator.yml \
  -f docker-compose.observability.yml \
  --profile observability up -d
```

Grafana: `http://localhost:3000` (default `admin/admin` — change immediately).
Loki query: `compose_service="validator"`.

## Security notes

- Never commit `*.env` files.
- Use Docker Hub access tokens, not account passwords.
- Both `validator` and `trainer` stacks mount the host docker socket; run only on
  trusted hosts.  This is required for sibling-container execution and cannot
  be sandboxed away.

## Removed in v2

The following compose files / commands were removed when the validator
workflow switched to train→score:

- `docker-compose.owner-sync-worker.yml` (owner-sync worker)
- `docker-compose.source-auth-validator.yml` (source-auth loop)
- `nexis sync-owner-datasets`, `nexis validate-source-auth`
