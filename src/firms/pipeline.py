"""End-to-end FIRMS ETL pipeline: download -> validate -> dedupe -> persist."""
from __future__ import annotations

import io
import logging
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

import httpx
import pandas as pd

from firms.schema import FireDetection

log = logging.getLogger(__name__)

FIRMS_URLS = {
    "MODIS_C6_1_global_24h":
        "https://firms.modaps.eosdis.nasa.gov/data/active_fire/modis-c6.1/csv/MODIS_C6_1_Global_24h.csv",
    "VIIRS_SNPP_NRT_global_24h":
        "https://firms.modaps.eosdis.nasa.gov/data/active_fire/suomi-npp-viirs-c2/csv/SUOMI_VIIRS_C2_Global_24h.csv",
}


@dataclass
class PipelineStats:
    fetched: int
    valid: int
    deduplicated: int
    written_to: Path


def fetch_csv(url: str, *, timeout: float = 60.0) -> str:
    log.info("fetching %s", url)
    with httpx.Client(follow_redirects=True, timeout=timeout) as client:
        resp = client.get(url)
        resp.raise_for_status()
        return resp.text


def parse_csv(raw: str) -> pd.DataFrame:
    df = pd.read_csv(io.StringIO(raw))
    if "acq_date" in df.columns:
        df["acq_date"] = pd.to_datetime(df["acq_date"]).dt.date
    if "acq_time" in df.columns:
        # FIRMS encodes acq_time as integer "HHMM"
        df["acq_time"] = df["acq_time"].astype(int).apply(
            lambda v: pd.Timestamp(f"{v // 100:02d}:{v % 100:02d}").time()
        )
    return df


def validate(df: pd.DataFrame) -> pd.DataFrame:
    """Drop rows that fail the FireDetection schema."""
    kept_rows: list[dict] = []
    rejected = 0
    for row in df.to_dict("records"):
        try:
            FireDetection(**row)
        except Exception as e:  # noqa: BLE001
            log.debug("rejected row: %s", e)
            rejected += 1
            continue
        kept_rows.append(row)
    log.info("validate: kept=%d rejected=%d", len(kept_rows), rejected)
    return pd.DataFrame(kept_rows)


def deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    """Drop exact-coordinate / date / time / satellite duplicates."""
    return df.drop_duplicates(
        subset=["latitude", "longitude", "acq_date", "acq_time", "satellite"],
        keep="first",
    ).reset_index(drop=True)


def run_pipeline(urls: Iterable[str], out_path: str | Path) -> PipelineStats:
    raw_dfs: list[pd.DataFrame] = []
    fetched = 0
    for url in urls:
        raw = fetch_csv(url)
        df = parse_csv(raw)
        fetched += len(df)
        raw_dfs.append(df)
    df = pd.concat(raw_dfs, ignore_index=True)
    validated = validate(df)
    deduped = deduplicate(validated)
    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    deduped.to_parquet(out, index=False)
    return PipelineStats(
        fetched=fetched, valid=len(validated),
        deduplicated=len(deduped), written_to=out,
    )
