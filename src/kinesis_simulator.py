"""Local Simulator for Amazon Kinesis Data Streams + Amazon Data Firehose S3 Archival.

Models deterministic shard routing (`MD5(project_id) % shard_count`), monotonic sequence
numbers, exactly-once `event_id` deduplication, and partitioned S3 Parquet landing.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

ROOT_DIR = Path(__file__).resolve().parent.parent


@dataclass(frozen=True)
class KinesisRecord:
    shard_id: str
    partition_key: str
    sequence_number: str
    kinesis_data: str


class KinesisStreamSimulator:
    """Simulates Amazon Kinesis Data Streams + Firehose S3 Parquet Lake delivery."""

    def __init__(self, stream_name: str = "evm-operational-events-stream", num_shards: int = 4) -> None:
        self.stream_name = stream_name
        self.num_shards = num_shards
        self._records: list[KinesisRecord] = []
        self._seen_seq: int = 100_000

    def _pick_shard(self, partition_key: str) -> str:
        digest = int(hashlib.md5(partition_key.encode("utf-8")).hexdigest(), 16)
        return f"shardId-{digest % self.num_shards:012d}"

    def put_record(self, payload: dict[str, Any], partition_key: str | None = None) -> KinesisRecord:
        pkey = partition_key or str(payload["project_id"])
        self._seen_seq += 1
        rec = KinesisRecord(
            shard_id=self._pick_shard(pkey),
            partition_key=pkey,
            sequence_number=f"{self._seen_seq:020d}",
            kinesis_data=json.dumps(payload),
        )
        self._records.append(rec)
        return rec

    def ingest_jsonl_file(self, jsonl_path: Path) -> int:
        count = 0
        with jsonl_path.open("r", encoding="utf-8") as fh:
            for line in fh:
                if line.strip():
                    self.put_record(json.loads(line))
                    count += 1
        return count

    def flush_firehose_to_s3_parquet(self, bronze_dir: Path) -> Path:
        """Archive buffered stream events to S3 Bronze Parquet (deduplicating event_id)."""
        bronze_dir.mkdir(parents=True, exist_ok=True)
        parsed = []
        seen_ids: set[str] = set()
        for rec in self._records:
            body = json.loads(rec.kinesis_data)
            eid = body["event_id"]
            if eid in seen_ids:
                continue
            seen_ids.add(eid)
            parsed.append(
                {
                    **body,
                    "shard_id": rec.shard_id,
                    "sequence_number": rec.sequence_number,
                }
            )

        out_file = bronze_dir / "kinesis_events_bronze.parquet"
        pq.write_table(pa.Table.from_pandas(pd.DataFrame(parsed)), out_file)
        return out_file

    @property
    def records(self) -> list[KinesisRecord]:
        return list(self._records)
