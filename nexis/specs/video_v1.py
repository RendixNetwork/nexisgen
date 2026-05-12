"""Video v1 dataset spec adapter."""

from __future__ import annotations

from dataclasses import dataclass

from ..models import ClipRecord
from ..validator.dataset_check import canonical_source_key


@dataclass(frozen=True)
class VideoV1Spec:
    spec_id: str = "video_v1"
    supported_protocol_versions: set[str] = frozenset({"2.0.0"})  # type: ignore[assignment]
    supported_schema_versions: set[str] = frozenset({"2.0.0"})  # type: ignore[assignment]
    row_model: type[ClipRecord] = ClipRecord

    def source_identity_key(self, row: ClipRecord) -> str:
        canonical = canonical_source_key(row.source_video_url)
        if canonical:
            return canonical
        return row.source_video_id.strip() or row.source_video_url.strip()

    def source_identity_keys(self, row: ClipRecord) -> list[str]:
        keys: list[str] = []
        canonical = canonical_source_key(row.source_video_url)
        if canonical:
            keys.append(canonical)
        source_id = row.source_video_id.strip()
        if source_id and source_id not in keys:
            keys.append(source_id)
        source_url = row.source_video_url.strip()
        if source_url and source_url not in keys:
            keys.append(source_url)
        return keys

    def overlap_index_keys(self, row: ClipRecord) -> list[str]:
        keys: list[str] = []
        for key in self.source_identity_keys(row):
            keys.append(key)
            keys.append(f"{self.spec_id}:{key}")
        return keys

    def is_compatible(self, *, protocol_version: str, schema_version: str) -> bool:
        return (
            protocol_version in self.supported_protocol_versions
            and schema_version in self.supported_schema_versions
        )
