"""Convert miner dataset (parquet + clips) into trainer-image manifest.jsonl format.

The trainer image (`rendixnetwork/train:latest` → `train_wan22_ti2v_lora.read_manifest`)
requires each line to contain at least `video` and `prompt`. We also emit
`image` (first-frame conditioning), `width`, `height`, `fps`, and `num_frames`
since the trainer reads them when present.

Paths are written from the **container's** point of view, not the host's. The
trainer image bind-mounts the host `miner_dir` at
`/workspace/training/<miner_hotkey>/`, so the manifest references that
container path; host paths don't exist inside the container.

Captions are sourced exclusively from `row.caption`. The validator
(`dataset_check.validate_miner_dataset`) rejects datasets with any missing
caption before we get here, so this module trusts every row to have one.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

from ..serialization import read_dataset_parquet

logger = logging.getLogger(__name__)


def convert_to_trainer_manifest(
    *,
    miner_dir: Path,
    container_dataset_dir: str,
    output_path: Path | None = None,
) -> Path:
    """Write a `manifest.jsonl` next to the miner dataset.

    Args:
      miner_dir: host path to the miner's local dataset dir (parquet, clips/,
        frames/). The output JSONL is written here.
      container_dataset_dir: path INSIDE the trainer container where this same
        data is mounted (e.g. ``/workspace/training/<miner_hotkey>``). All
        `video`/`image` paths in the JSONL are rooted here.
      output_path: optional override for the JSONL location. Defaults to
        ``miner_dir / 'manifest.jsonl'``.
    """
    dataset_path = miner_dir / "dataset.parquet"
    if not dataset_path.exists():
        raise FileNotFoundError(f"dataset.parquet missing in {miner_dir}")
    records = read_dataset_parquet(dataset_path)
    target = output_path or (miner_dir / "manifest.jsonl")
    target.parent.mkdir(parents=True, exist_ok=True)
    base = container_dataset_dir.rstrip("/")
    with target.open("w", encoding="utf-8") as fp:
        for row in records:
            clip_path = f"{base}/{row.clip_uri.lstrip('/')}"
            frame_path = f"{base}/{row.first_frame_uri.lstrip('/')}"
            payload = {
                "video": clip_path,
                "prompt": row.caption,
                "image": frame_path,
                "width": int(row.width),
                "height": int(row.height),
                "fps": float(row.fps),
                "num_frames": int(row.num_frames),
                # Free-form metadata the trainer is free to ignore.
                "clip_id": row.clip_id,
                "source_video_id": row.source_video_id,
                "source_video_url": row.source_video_url,
                "clip_start_sec": float(row.clip_start_sec),
                "duration_sec": float(row.duration_sec),
            }
            fp.write(json.dumps(payload, ensure_ascii=True) + "\n")
    logger.info("trainer manifest written path=%s rows=%d", target, len(records))
    return target
