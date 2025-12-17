"""Repository layer for CSV data access."""
import os
import hashlib
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Set
import threading

import pandas as pd

from models import TradeEvent


class CSVRepository:
    """Repository for managing trade data in CSV format."""
    
    # CSV file path
    DEFAULT_CSV_PATH = Path(__file__).parent / "trades.csv"
    
    # CSV columns for OHLCV data
    COLUMNS = ["timestamp", "symbol", "open", "high", "low", "close", "volume"]
    
    # Lock for thread-safe file operations
    _lock = threading.Lock()
    
    def __init__(self, csv_path: Optional[Path] = None):
        """Initialize repository with optional custom CSV path."""
        self.csv_path = csv_path or self.DEFAULT_CSV_PATH
        # Instance-level set for tracking ingested trade hashes (for idempotency)
        self._ingested_hashes: Set[str] = set()
        self._ensure_csv_exists()
        self._load_existing_hashes()
    
    def _ensure_csv_exists(self) -> None:
        """Create CSV file with headers if it doesn't exist."""
        if not self.csv_path.exists():
            df = pd.DataFrame(columns=self.COLUMNS)
            df.to_csv(self.csv_path, index=False)
    
    def _load_existing_hashes(self) -> None:
        """Load hashes of existing records for idempotency check."""
        try:
            df = self._read_csv()
            if not df.empty:
                for _, row in df.iterrows():
                    hash_key = self._compute_hash(
                        timestamp=row["timestamp"],
                        symbol=row["symbol"],
                        close=row["close"],
                        volume=row["volume"]
                    )
                    self._ingested_hashes.add(hash_key)
        except Exception:
            # If file is corrupted or empty, start fresh
            pass
    
    @staticmethod
    def _compute_hash(timestamp: str, symbol: str, close: float, volume: int) -> str:
        """Compute a unique hash for a trade record."""
        data = f"{timestamp}|{symbol}|{close}|{volume}"
        return hashlib.md5(data.encode()).hexdigest()
    
    def _read_csv(self) -> pd.DataFrame:
        """Read CSV file into DataFrame."""
        if not self.csv_path.exists():
            return pd.DataFrame(columns=self.COLUMNS)
        
        df = pd.read_csv(self.csv_path)
        if df.empty:
            return pd.DataFrame(columns=self.COLUMNS)
        return df
    
    def ingest_trades(self, trades: List[TradeEvent]) -> tuple[int, int]:
        """
        Ingest trade events into CSV storage.
        
        Converts trade events (with price) to OHLCV format where:
        - open = high = low = close = price (single trade point)
        
        Returns:
            Tuple of (records_ingested, duplicates_skipped)
        """
        if not trades:
            return 0, 0
        
        new_records = []
        duplicates = 0
        
        for trade in trades:
            timestamp_str = trade.timestamp.isoformat()
            hash_key = self._compute_hash(
                timestamp=timestamp_str,
                symbol=trade.symbol,
                close=trade.price,
                volume=trade.volume
            )
            
            # Idempotency check
            if hash_key in self._ingested_hashes:
                duplicates += 1
                continue
            
            # Convert trade event to OHLCV record
            # For a single trade, OHLC values are all the same (the trade price)
            record = {
                "timestamp": timestamp_str,
                "symbol": trade.symbol,
                "open": trade.price,
                "high": trade.price,
                "low": trade.price,
                "close": trade.price,
                "volume": trade.volume
            }
            new_records.append(record)
            self._ingested_hashes.add(hash_key)
        
        if new_records:
            with self._lock:
                new_df = pd.DataFrame(new_records, columns=self.COLUMNS)
                
                # Append to existing CSV
                if self.csv_path.exists() and os.path.getsize(self.csv_path) > 0:
                    existing_df = self._read_csv()
                    if existing_df.empty:
                        combined_df = new_df
                    else:
                        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
                else:
                    combined_df = new_df
                
                combined_df.to_csv(self.csv_path, index=False)
        
        return len(new_records), duplicates
    
    def get_data_by_symbol(
        self, 
        symbol: str, 
        start: Optional[datetime] = None, 
        end: Optional[datetime] = None
    ) -> pd.DataFrame:
        """
        Retrieve OHLCV data for a specific symbol within optional time range.
        
        Args:
            symbol: Trading symbol to filter by
            start: Optional start of time range
            end: Optional end of time range
            
        Returns:
            DataFrame with filtered OHLCV data
        """
        df = self._read_csv()
        
        if df.empty:
            return df
        
        # Filter by symbol
        df = df[df["symbol"].str.upper() == symbol.upper()]
        
        if df.empty:
            return df
        
        # Parse timestamps
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        
        # Filter by time range
        if start:
            df = df[df["timestamp"] >= start]
        if end:
            df = df[df["timestamp"] <= end]
        
        return df.sort_values("timestamp")
    
    def symbol_exists(self, symbol: str) -> bool:
        """Check if a symbol exists in the dataset."""
        df = self._read_csv()
        if df.empty:
            return False
        return symbol.upper() in df["symbol"].str.upper().values
    
    def clear_data(self) -> None:
        """Clear all data (useful for testing)."""
        with self._lock:
            df = pd.DataFrame(columns=self.COLUMNS)
            df.to_csv(self.csv_path, index=False)
            self._ingested_hashes.clear()


# Singleton instance for the application
_repository_instance: Optional[CSVRepository] = None


def get_repository() -> CSVRepository:
    """Get or create the repository singleton."""
    global _repository_instance
    if _repository_instance is None:
        _repository_instance = CSVRepository()
    return _repository_instance


def reset_repository(csv_path: Optional[Path] = None) -> CSVRepository:
    """Reset repository instance (useful for testing)."""
    global _repository_instance
    _repository_instance = CSVRepository(csv_path)
    return _repository_instance

