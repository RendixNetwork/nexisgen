# Nexisgen

Nexisgen is a Bittensor-style data subnet implementation. Miners produce
interval-based video clip datasets, and validators verify those datasets,
score miners, and submit weights on-chain.

This README is an operator-focused guide for:

- understanding miner and validator roles
- setting up and running miners
- setting up and running validators (Docker and local)
- understanding exactly how miner data is validated

## Contents

- [How Nexisgen Works](#how-nexisgen-works)
- [Roles: Miner vs Validator](#roles-miner-vs-validator)
- [System Requirements](#system-requirements)
- [Local Project Setup](#local-project-setup)
- [Miner Setup and Run](#miner-setup-and-run)
- [Validator Setup and Run](#validator-setup-and-run)
- [How Miner Data Validation Works](#how-miner-data-validation-works)
- [Dataset and Manifest Format](#dataset-and-manifest-format)
- [Useful Commands](#useful-commands)
- [Troubleshooting](#troubleshooting)
- [More Documentation](#more-documentation)

## How Nexisgen Works

Nexisgen runs on fixed block intervals:

- one dataset package per miner per interval
- interval length: `50` blocks
- validator waits for closed interval + `2` reserve blocks before evaluation
- validator submits chain weights every `250` blocks

At a high level:

1. Miner generates `dataset.parquet` + `manifest.json` for an interval.
2. Miner uploads package to storage (Cloudflare R2 via S3 API).
3. Validator discovers miners with committed read credentials.
4. Validator downloads each miner interval package and validates it.
5. Validator accepts/rejects each miner interval and computes scores/weights.
6. Validator submits weights to chain (every 250 blocks).

## Roles: Miner vs Validator

### Miner role

- collects source videos and builds clip records
- creates captions
- writes interval package (`dataset.parquet` + `manifest.json`)
- uploads package to bucket
- commits read credentials on-chain so validators can access miner submissions

### Validator role

- discovers miner credentials and active interval submissions
- validates miner datasets with schema and anti-cheat checks
- samples and verifies clip/frame assets
- enforces sampled clip resolution (`1280x720`)
- runs optional semantic caption checks
- prunes overlap rows and arbitrates cross-miner conflicts
- computes miner scores and submits chain weights
- fetches invalid hotkeys from validation API and zeros them before `set_weights`

### Owner-validator (special validator mode)

When validator hotkey equals `NEXIS_OWNER_VALIDATOR_HOTKEY`, it also:

- publishes accepted metadata bundles (`dataset.parquet`, `manifest.json`) to
  record-info bucket (`NEXIS_RECORD_INFO_BUCKET`)
- writes the shared overlap index snapshot (`NEXIS_RECORD_INFO_OBJECT_KEY`)
- can run an independent `nexis sync-owner-datasets` worker to copy full assets
  into owner dataset bucket (`NEXIS_OWNER_DB_BUCKET`)

## System Requirements

### Required binaries

Install these before running miner or validator:

- `yt-dlp`
- `ffmpeg`
- `ffprobe`

### Required software

- Python `>=3.10,<3.13`
- access to Bittensor wallet files
- Cloudflare R2 credentials (account id + read/write keys)

### For Docker validator deployment

- Docker Engine
- Docker Compose v2

## Local Project Setup

```bash
cd nexisgen
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
cp .env.example .env
```

Edit `.env` and fill required values before running commands.

## Miner Setup and Run

### 1) Configure miner `.env`

At minimum, set wallet, Cloudflare R2 credentials, and source file path.
Shared buckets require explicit account IDs:
`NEXIS_RECORD_INFO_ACCOUNT_ID` and `NEXIS_OWNER_DB_ACCOUNT_ID`.

### 2) Commit miner read credentials on-chain

Validators rely on this to discover miner buckets.

```bash
nexis commit-credentials
```

### 3) Run miner loop

```bash
nexis mine
```

Optional:

```bash
# choose spec explicitly
nexis mine --spec video_v1

# debug logging
nexis mine --debug

# override polling interval
nexis mine --poll-sec 4
```

Miner behavior:

- long-running process (stop with `Ctrl+C`)
- builds one package per 50-block interval
- skips interval if manifest already exists

## Validator Setup and Run

You can run a validator in either of these ways:

- **Docker**: recommended for most operators because it is easier to deploy,
  update, and keep isolated from host Python dependencies
- **Local (non-Docker)**: supported for development, debugging, or operators
  who prefer managing the Python environment directly

Choose one setup path below. Do not run both on the same validator instance.

### Recommended: Docker validator deployment

This is the best default choice for production or long-running validator nodes.

```bash
cd docker
cp validator.env.example validator.env
cp compose.env.example compose.env
chmod 600 validator.env compose.env
```

Edit:

- `compose.env`
  - `BT_WALLET_HOST_PATH`
- `validator.env`
  - wallet/wallet_path/api_key values

Start validator + watchtower:

```bash
docker compose --env-file compose.env -f docker-compose.validator.yml up -d
```

Check logs:

```bash
docker logs -f nexis-validator
docker logs -f nexis-watchtower
```

### Local (non-Docker) validator

Use this path if you want to run the validator directly on the host instead of
in Docker.

From project root, complete `Local Project Setup` first so the virtualenv and
dependencies are installed. If `.env` does not already exist, create it from
the example file:

```bash
cp .env.example .env
```

Then edit `.env` with the validator settings you need and run:

```bash
nexis validate
```

Optional:

```bash
# validate specific specs
nexis validate --specs video_v1

# enable debug output (recommended while tuning)
nexis validate --debug

# runtime hotkey exclusions (blacklist file is always enforced)
nexis validate --exclude-hotkeys hotkey1,hotkey2

# override polling interval
nexis validate --poll-sec 4
```

## How Miner Data Validation Works

Validator checks miner submissions in layers. A miner interval is accepted only
if all required checks pass.

1. **Discover + fetch**
   - discover miners from metagraph + committed credentials
   - download `manifest.json` and `dataset.parquet`
2. **Manifest and identity checks**
   - manifest must match miner hotkey and interval id
   - spec and protocol/schema versions must be compatible and enabled
3. **Integrity checks**
   - `manifest.dataset_sha256` must match downloaded dataset hash
   - `manifest.record_count` must match dataset row count
4. **Schema + hard checks (full dataset)**
   - row schema must parse correctly
   - source URLs must be YouTube (`youtube.com` / `youtu.be`)
   - clip overlap policy (`>=5s` gap) must hold
   - captions must pass lexical checks (non-empty, not too short, not URL-like)
5. **Sampled asset verification**
   - validator samples rows
   - verifies clip/frame assets against SHA256 fields
   - validates sampled clip resolution is exactly `1280x720`
6. **Optional semantic caption check**
   - model checks whether caption matches sampled multi-frame visual context
7. **Overlap pruning**
   - rows already seen in global index are pruned
   - cross-miner same-source overlaps are arbitrated by earliest manifest time
8. **Decision + scoring**
   - emits per-miner accept/reject decision with failure reasons
   - API maintains invalid-hotkey windows (`interval_id-500` to `interval_id`)
   - validator zeros API-invalid hotkeys before submitting chain weights


### How to observe validation results

Run validator with debug:

```bash
nexis validate --debug
```

Validator outputs per-interval decision JSON including:

- `accepted` (true/false)
- `failures` (list of failure reason codes)
- `sampled_rows`
- `notes` (record/sample counts, overlap prune counts, spec id)

Optional: forward signed interval results to central evidence API by setting:

- `NEXIS_VALIDATION_API_URL`
- `NEXIS_VALIDATION_API_TIMEOUT_SEC`

## Dataset and Manifest Format

Miner submissions use:

- `dataset.parquet` with clip-level rows
- `manifest.json` with interval metadata and hashes

Core dataset columns include:

- `clip_id`, `clip_uri`, `clip_sha256`
- `first_frame_uri`, `first_frame_sha256`
- `source_video_id`, `source_video_url`
- `clip_start_sec`, `duration_sec`
- `width`, `height`, `fps`, `num_frames`, `has_audio`
- `caption`, `source_proof`

Manifest includes:

- `protocol_version`, `schema_version`
- `spec_id` / `dataset_type`
- `netuid`, `miner_hotkey`, `interval_id`
- `created_at`
- `record_count`
- `dataset_sha256`

Current default spec: `video_v1`.

## Useful Commands

```bash
# commit miner read credentials for validator discovery
nexis commit-credentials

# run miner
nexis mine

# run validator
nexis validate

# run independent owner dataset copy worker (every 60s)
nexis sync-owner-datasets --poll-sec 60
```

## Troubleshooting

- **No miner data validated**
  - ensure miners have run `nexis commit-credentials`
  - verify validator can read metagraph and bucket credentials
- **Semantic checks failing unexpectedly**
  - disable with `NEXIS_VALIDATOR_SEMANTIC_CHECK_ENABLED=false` for isolation
  - if `OPENAI_API_KEY` is set, validator uses `gpt-4o`
  - if `OPENAI_API_KEY` is unset and `GEMINI_API_KEY` is set, validator uses `gemini-3.1-flash-lite-preview`
  - confirm timeout/model settings and provider key configuration
- **Source authenticity failures**
  - confirm `yt-dlp` and `ffmpeg` are installed and working
  - verify miner `source_video_url` is valid and reachable
- **Docker wallet issues**
  - ensure `BT_WALLET_HOST_PATH` is correct in `docker/compose.env`
  - ensure `BT_WALLET_PATH=/wallets` in `docker/validator.env`

## More Documentation

- miner guide: `docs/miner.md`
- validator guide: `docs/validator.md`
- validator docker deployment: `docker/README.md`
- dataset schema details: `docs/dataset-schema.md`
- adding new dataset specs: `docs/adding-dataset-spec.md`

