# ==============================================
# 🔧 TXID Backfill Progress (Load / Save)
# Crash-safe state handling for backfill workers
# ==============================================

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple


DEFAULT_STATE = {
    "entity": "txids",
    "segment_size": 10000,

    "last_height": -1,
    "current_segment_start": 0,
    "current_segment_end": 9999,

    "segments_completed": 0,
    "events_written_total": 0,

    "updated_utc": None
}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def segment_range_for_height(height: int, segment_size: int) -> Tuple[int, int]:
    """
    Given a height, return (segment_start, segment_end) for segment_size blocks.
    Example: height=932481, segment_size=10000 -> (930000, 939999)
    """
    if segment_size <= 0:
        raise ValueError("segment_size must be > 0")
    start = (height // segment_size) * segment_size
    end = start + (segment_size - 1)
    return start, end


def segment_filename(entity: str, start: int, end: int, pad: int = 9, ext: str = "jsonl") -> str:
    """
    Build deterministic segment filename, e.g.:
    txids_000930000_000939999.jsonl
    """
    return f"{entity}_{start:0{pad}d}_{end:0{pad}d}.{ext}"


def segment_filename_for_height(entity: str, height: int, segment_size: int, pad: int = 9) -> str:
    start, end = segment_range_for_height(height, segment_size)
    return segment_filename(entity, start, end, pad=pad, ext="jsonl")


@dataclass
class BackfillState:
    entity: str = "txids"
    segment_size: int = 10000

    last_height: int = -1
    current_segment_start: int = 0
    current_segment_end: int = 9999

    segments_completed: int = 0
    events_written_total: int = 0

    updated_utc: Optional[str] = None

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "BackfillState":
        # Merge with defaults (forward-compatible)
        merged = dict(DEFAULT_STATE)
        merged.update(d or {})
        return cls(
            entity=str(merged["entity"]),
            segment_size=int(merged["segment_size"]),
            last_height=int(merged["last_height"]),
            current_segment_start=int(merged["current_segment_start"]),
            current_segment_end=int(merged["current_segment_end"]),
            segments_completed=int(merged["segments_completed"]),
            events_written_total=int(merged["events_written_total"]),
            updated_utc=merged.get("updated_utc"),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "entity": self.entity,
            "segment_size": self.segment_size,
            "last_height": self.last_height,
            "current_segment_start": self.current_segment_start,
            "current_segment_end": self.current_segment_end,
            "segments_completed": self.segments_completed,
            "events_written_total": self.events_written_total,
            "updated_utc": self.updated_utc,
        }

    def next_height(self) -> int:
        return self.last_height + 1

    def refresh_segment_for_height(self, height: int) -> None:
        start, end = segment_range_for_height(height, self.segment_size)
        self.current_segment_start = start
        self.current_segment_end = end

    def mark_updated(self) -> None:
        self.updated_utc = _utc_now_iso()


def load_state(path: str, allow_create: bool = True) -> BackfillState:
    """
    Load state JSON from path. If missing or invalid and allow_create=True,
    create default state on disk atomically and return it.
    """
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        return BackfillState.from_dict(raw)
    except FileNotFoundError:
        if not allow_create:
            raise
    except Exception:
        # Corrupt / partial file
        if not allow_create:
            raise

    # Create default state (atomic)
    state = BackfillState.from_dict(DEFAULT_STATE)
    save_state_atomic(path, state)
    return state


def save_state_atomic(path: str, state: BackfillState) -> None:
    """
    Atomic state write: write to .tmp then os.replace.
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"

    state.mark_updated()
    payload = state.to_dict()

    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, separators=(",", ":"))

    os.replace(tmp, path)
