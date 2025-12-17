"""Service layer for business logic."""
from datetime import datetime
from typing import List, Optional

import pandas as pd

from models import (
    IntervalEnum,
    OHLCVRecord,
    OHLCVResponse,
    TradeEvent,
    TradeIngestResponse,
)
from repository import CSVRepository, get_repository


class TradeIngestionError(Exception):
    """Exception for trade ingestion errors."""
    pass


class DataNotFoundError(Exception):
    """Exception when requested data is not found."""
    pass


class TradeService:
    """Service for handling trade-related business logic."""
    
    # Mapping from interval enum to pandas resample frequency
    INTERVAL_MAP = {
        IntervalEnum.ONE_MIN: "1min",
        IntervalEnum.FIVE_MIN: "5min",
        IntervalEnum.ONE_HOUR: "1h",
        IntervalEnum.ONE_DAY: "1D",
    }
    
    def __init__(self, repository: Optional[CSVRepository] = None):
        """Initialize service with optional repository (for testing)."""
        self._repository = repository
    
    @property
    def repository(self) -> CSVRepository:
        """Get repository instance (lazy loading for dependency injection)."""
        if self._repository is None:
            self._repository = get_repository()
        return self._repository
    
    def ingest_trades(self, trades: List[TradeEvent]) -> TradeIngestResponse:
        """
        Ingest trade events into storage.
        
        Args:
            trades: List of trade events to ingest
            
        Returns:
            TradeIngestResponse with ingestion results
            
        Raises:
            TradeIngestionError: If ingestion fails
        """
        if not trades:
            raise TradeIngestionError("No trades provided for ingestion")
        
        try:
            records_ingested, duplicates_skipped = self.repository.ingest_trades(trades)
            
            symbol = trades[0].symbol
            
            if records_ingested == 0 and duplicates_skipped > 0:
                message = f"All {duplicates_skipped} trades were duplicates and skipped"
            elif duplicates_skipped > 0:
                message = f"Ingested {records_ingested} trades, skipped {duplicates_skipped} duplicates"
            else:
                message = f"Successfully ingested {records_ingested} trades"
            
            return TradeIngestResponse(
                status="success",
                message=message,
                symbol=symbol,
                records_ingested=records_ingested,
                duplicates_skipped=duplicates_skipped
            )
        except Exception as e:
            raise TradeIngestionError(f"Failed to ingest trades: {str(e)}") from e
    
    def get_ohlcv_stats(
        self,
        symbol: str,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        interval: IntervalEnum = IntervalEnum.ONE_MIN
    ) -> OHLCVResponse:
        """
        Get aggregated OHLCV statistics for a symbol.
        
        Args:
            symbol: Trading symbol to query
            start: Optional start of time range
            end: Optional end of time range
            interval: Aggregation interval
            
        Returns:
            OHLCVResponse with aggregated data
            
        Raises:
            DataNotFoundError: If symbol or data not found
        """
        # Check if symbol exists
        if not self.repository.symbol_exists(symbol):
            raise DataNotFoundError(f"Symbol '{symbol}' not found in dataset")
        
        # Get filtered data
        df = self.repository.get_data_by_symbol(symbol, start, end)
        
        if df.empty:
            raise DataNotFoundError(
                f"No data available for symbol '{symbol}' in the specified time range"
            )
        
        # Aggregate data by interval
        aggregated_df = self._aggregate_ohlcv(df, interval)
        
        # Convert to response model
        records = [
            OHLCVRecord(
                timestamp=row.name,
                open=row["open"],
                high=row["high"],
                low=row["low"],
                close=row["close"],
                volume=int(row["volume"])
            )
            for _, row in aggregated_df.iterrows()
        ]
        
        return OHLCVResponse(
            symbol=symbol.upper(),
            interval=interval.value,
            data=records
        )
    
    def _aggregate_ohlcv(
        self, 
        df: pd.DataFrame, 
        interval: IntervalEnum
    ) -> pd.DataFrame:
        """
        Aggregate OHLCV data to the specified interval.
        
        Args:
            df: DataFrame with OHLCV data
            interval: Target aggregation interval
            
        Returns:
            Aggregated DataFrame
        """
        # Set timestamp as index for resampling
        df = df.set_index("timestamp")
        
        # Get pandas frequency string
        freq = self.INTERVAL_MAP[interval]
        
        # Resample and aggregate
        aggregated = df.resample(freq).agg({
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum"
        })
        
        # Remove rows with NaN values (empty intervals)
        aggregated = aggregated.dropna()
        
        return aggregated


# Service instance for dependency injection
_service_instance: Optional[TradeService] = None


def get_trade_service() -> TradeService:
    """Get or create the trade service singleton."""
    global _service_instance
    if _service_instance is None:
        _service_instance = TradeService()
    return _service_instance


def reset_trade_service(repository: Optional[CSVRepository] = None) -> TradeService:
    """Reset trade service instance (useful for testing)."""
    global _service_instance
    _service_instance = TradeService(repository)
    return _service_instance

