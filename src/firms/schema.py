"""Pydantic schema for one FIRMS active-fire detection."""
from __future__ import annotations

from datetime import date, time

from pydantic import BaseModel, Field, field_validator


class FireDetection(BaseModel):
    latitude: float = Field(ge=-90, le=90)
    longitude: float = Field(ge=-180, le=180)
    brightness: float = Field(gt=0)
    scan: float = Field(gt=0)
    track: float = Field(gt=0)
    acq_date: date
    acq_time: time
    satellite: str
    confidence: int = Field(ge=0, le=100)
    version: str
    bright_t31: float | None = None
    frp: float = Field(ge=0)
    daynight: str

    @field_validator("daynight")
    @classmethod
    def _check_daynight(cls, v: str) -> str:
        if v not in ("D", "N"):
            raise ValueError("daynight must be 'D' or 'N'")
        return v
