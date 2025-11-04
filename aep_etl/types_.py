# aep_etl/types_.py
"""
Shared data models (Pydantic).
"""
from __future__ import annotations
from pydantic import BaseModel, Field
from typing import Dict, Optional

class MetricRow(BaseModel):
    """
    One metric for one country across years.
    """
    country: str
    country_serial: str
    metric: str
    unit: str
    sector: str
    sub_sector: Optional[str] = None
    sub_sub_sector: Optional[str] = None
    source_link: str
    source: str
    yearly: Dict[int, Optional[float]] = Field(default_factory=dict)

    def to_mongo_doc(self) -> dict:
        """Convert to MongoDB-ready dict with years as string keys."""
        doc = {
            "country": self.country,
            "country_serial": self.country_serial,
            "metric": self.metric,
            "unit": self.unit,
            "sector": self.sector,
            "sub_sector": self.sub_sector,
            "sub_sub_sector": self.sub_sub_sector,
            "source_link": self.source_link,
            "source": self.source,
        }
        for year, val in self.yearly.items():
            doc[str(year)] = val
        return doc
