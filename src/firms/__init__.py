"""NASA FIRMS active-fire ETL pipeline."""

from firms.pipeline import deduplicate, run_pipeline, validate
from firms.schema import FireDetection

__all__ = ["FireDetection", "run_pipeline", "validate", "deduplicate"]
__version__ = "0.3.0"
