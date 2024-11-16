"""CLI."""
from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

from firms.pipeline import FIRMS_URLS, run_pipeline


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--sources", nargs="+", default=list(FIRMS_URLS.keys()),
                   help="named FIRMS feeds to ingest")
    p.add_argument("--out", type=Path, default=Path("data/firms.parquet"))
    p.add_argument("--log-level", default="INFO")
    args = p.parse_args(argv)
    logging.basicConfig(level=args.log_level,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    urls = [FIRMS_URLS[s] for s in args.sources]
    stats = run_pipeline(urls, args.out)
    print(json.dumps({
        "fetched": stats.fetched,
        "valid": stats.valid,
        "deduplicated": stats.deduplicated,
        "written_to": str(stats.written_to),
    }, indent=2))
    return 0
