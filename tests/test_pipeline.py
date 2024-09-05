import io
from datetime import date, time
from pathlib import Path

import pandas as pd
import pytest

from firms.pipeline import deduplicate, parse_csv, validate
from firms.schema import FireDetection


_SAMPLE = """latitude,longitude,brightness,scan,track,acq_date,acq_time,satellite,confidence,version,bright_t31,frp,daynight
55.21,-105.40,310.5,1.0,1.0,2024-06-10,1245,N,80,2.0NRT,290.1,12.3,D
55.21,-105.40,310.5,1.0,1.0,2024-06-10,1245,N,80,2.0NRT,290.1,12.3,D
56.30,-104.00,320.0,1.0,1.0,2024-06-10,1300,T,30,2.0NRT,295.0,14.0,D
"""


def test_parse_csv_converts_time_to_time_objects():
    df = parse_csv(_SAMPLE)
    assert isinstance(df["acq_time"].iloc[0], time)
    assert df["acq_time"].iloc[0] == time(12, 45)


def test_validate_drops_invalid_rows():
    bad = pd.DataFrame([{
        "latitude": 200.0, "longitude": 0.0, "brightness": 100.0,
        "scan": 1.0, "track": 1.0,
        "acq_date": date(2024, 1, 1), "acq_time": time(12, 0),
        "satellite": "N", "confidence": 80, "version": "2.0",
        "bright_t31": 200.0, "frp": 5.0, "daynight": "D",
    }])
    out = validate(bad)
    assert len(out) == 0


def test_deduplicate_collapses_duplicates():
    df = parse_csv(_SAMPLE)
    out = deduplicate(df)
    assert len(out) == 2


def test_schema_rejects_invalid_daynight():
    with pytest.raises(Exception):
        FireDetection(
            latitude=51.0, longitude=-115.0, brightness=300.0,
            scan=1.0, track=1.0, acq_date=date(2024, 1, 1),
            acq_time=time(12, 0), satellite="N", confidence=80,
            version="2", frp=10.0, daynight="X",
        )


def test_run_pipeline_writes_parquet(tmp_path: Path, monkeypatch):
    from firms import pipeline as pl

    def _fake_fetch(url: str, *, timeout: float = 60.0) -> str:
        return _SAMPLE

    monkeypatch.setattr(pl, "fetch_csv", _fake_fetch)
    stats = pl.run_pipeline(["http://example.invalid"], tmp_path / "f.parquet")
    assert stats.fetched == 3
    assert stats.deduplicated == 2
    assert (tmp_path / "f.parquet").exists()
