"""Pydantic models for request/response validation."""
from datetime import datetime
from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, Field, field_validator, model_validator


class IntervalEnum(str, Enum):
    """Valid aggregation intervals."""
    ONE_MIN = "1min"
    FIVE_MIN = "5min"
    ONE_HOUR = "1h"
    ONE_DAY = "1d"


class TradeEvent(BaseModel):
    """Single trade event model."""
    timestamp: datetime
    symbol: str = Field(..., min_length=1, description="Trading symbol identifier")
    price: float = Field(..., gt=0, description="Trade execution price")
    volume: int = Field(..., gt=0, description="Trade volume")

    @field_validator("symbol")
    @classmethod
    def validate_symbol(cls, v: str) -> str:
        """Ensure symbol is uppercase and non-empty."""
        return v.strip().upper()


class TradeIngestRequest(BaseModel):
    """Request model for trade ingestion endpoint."""
    trades: List[TradeEvent] = Field(..., min_length=1, description="List of trade events")

    @model_validator(mode="after")
    def validate_consistent_symbol(self) -> "TradeIngestRequest":
        """Ensure all trades in the request have the same symbol."""
        if not self.trades:
            return self
        
        symbols = {trade.symbol for trade in self.trades}
        if len(symbols) > 1:
            raise ValueError(
                f"All trades must have the same symbol. Found: {sorted(symbols)}"
            )
        return self


class TradeIngestResponse(BaseModel):
    """Response model for successful trade ingestion."""
    status: str = "success"
    message: str
    symbol: str
    records_ingested: int
    duplicates_skipped: int = 0


class OHLCVRecord(BaseModel):
    """Single OHLCV record."""
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int


class OHLCVResponse(BaseModel):
    """Response model for OHLCV statistics endpoint."""
    symbol: str
    interval: str
    data: List[OHLCVRecord]


class OHLCVQueryParams(BaseModel):
    """Query parameters for OHLCV endpoint."""
    symbol: str = Field(..., min_length=1, description="Trading symbol to query")
    start: Optional[datetime] = Field(None, description="Start of time range (ISO 8601)")
    end: Optional[datetime] = Field(None, description="End of time range (ISO 8601)")
    interval: Optional[IntervalEnum] = Field(
        IntervalEnum.ONE_MIN, 
        description="Aggregation interval"
    )

    @field_validator("symbol")
    @classmethod
    def validate_symbol(cls, v: str) -> str:
        """Ensure symbol is uppercase and non-empty."""
        return v.strip().upper()

    @model_validator(mode="after")
    def validate_date_range(self) -> "OHLCVQueryParams":
        """Ensure start date is before end date if both provided."""
        if self.start and self.end and self.start >= self.end:
            raise ValueError("Start date must be before end date")
        return self


class ErrorResponse(BaseModel):
    """Standard error response model."""
    detail: str
    error_code: Optional[str] = None

