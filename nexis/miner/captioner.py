"""Per-clip captioning for the miner pipeline.

The captioner takes the first frame of each clip and asks an OpenAI-compatible
vision model for a short prompt-style description. Captions feed into the
trainer manifest's `prompt` field; if no API key is configured the captioner
returns an empty string and the trainer falls back to its default prompt.
"""

from __future__ import annotations

import base64
import logging
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)


_PROMPT = (
    "Describe this video frame in one short sentence (≤ 20 words) that would "
    "work as a text-to-video generation prompt. Focus on subject, setting, and "
    "motion cues. Do not add commentary."
)


def _b64_image(path: Path) -> str:
    return base64.b64encode(path.read_bytes()).decode("ascii")


@dataclass
class Captioner:
    api_key: str = ""
    model: str = "gpt-4o-mini"
    base_url: str | None = None
    timeout_sec: int = 30

    def __post_init__(self) -> None:
        self._client = None
        if not self.api_key.strip():
            logger.info("captioner disabled: no API key configured")
            return
        try:
            from openai import OpenAI

            self._client = OpenAI(
                api_key=self.api_key,
                base_url=self.base_url or None,
                timeout=float(self.timeout_sec),
            )
        except Exception as exc:
            logger.warning("captioner init failed err=%s; will return empty captions", exc)
            self._client = None

    @property
    def enabled(self) -> bool:
        return self._client is not None

    def caption_frame(self, frame_path: Path) -> str:
        if self._client is None or not frame_path.exists():
            return ""
        try:
            data_url = f"data:image/jpeg;base64,{_b64_image(frame_path)}"
            resp = self._client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {"type": "text", "text": _PROMPT},
                            {"type": "image_url", "image_url": {"url": data_url}},
                        ],
                    }
                ],
                max_tokens=80,
            )
            text = (resp.choices[0].message.content or "").strip()
            return text[:300]
        except Exception as exc:
            logger.warning("caption call failed frame=%s err=%s", frame_path, exc)
            return ""
