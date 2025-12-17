"""Unit tests for Pydantic models."""
import pytest
from datetime import datetime
from pydantic import ValidationError

from models import (
    TradeEvent,
    TradeIngestRequest,
    OHLCVQueryParams,
    IntervalEnum,
)


class TestTradeEvent:
    """Tests for TradeEvent model."""
    
    def test_valid_trade_event(self):
        """Test creating a valid trade event."""
        trade = TradeEvent(
            timestamp=datetime(2025, 1, 2, 9, 30, 0),
            symbol="AAPL",
            price=190.50,
            volume=1200000
        )
        assert trade.symbol == "AAPL"
        assert trade.price == 190.50
        assert trade.volume == 1200000
    
    def test_symbol_normalized_to_uppercase(self):
        """Test that symbol is normalized to uppercase."""
        trade = TradeEvent(
            timestamp=datetime(2025, 1, 2, 9, 30, 0),
            symbol="aapl",
            price=190.50,
            volume=1200000
        )
        assert trade.symbol == "AAPL"
    
    def test_symbol_trimmed(self):
        """Test that symbol whitespace is trimmed."""
        trade = TradeEvent(
            timestamp=datetime(2025, 1, 2, 9, 30, 0),
            symbol="  AAPL  ",
            price=190.50,
            volume=1200000
        )
        assert trade.symbol == "AAPL"
    
    def test_invalid_price_zero(self):
        """Test that zero price is rejected."""
        with pytest.raises(ValidationError) as exc_info:
            TradeEvent(
                timestamp=datetime(2025, 1, 2, 9, 30, 0),
                symbol="AAPL",
                price=0,
                volume=1200000
            )
        assert "greater than 0" in str(exc_info.value).lower()
    
    def test_invalid_price_negative(self):
        """Test that negative price is rejected."""
        with pytest.raises(ValidationError):
            TradeEvent(
                timestamp=datetime(2025, 1, 2, 9, 30, 0),
                symbol="AAPL",
                price=-10.50,
                volume=1200000
            )
    
    def test_invalid_volume_zero(self):
        """Test that zero volume is rejected."""
        with pytest.raises(ValidationError):
            TradeEvent(
                timestamp=datetime(2025, 1, 2, 9, 30, 0),
                symbol="AAPL",
                price=190.50,
                volume=0
            )
    
    def test_invalid_volume_negative(self):
        """Test that negative volume is rejected."""
        with pytest.raises(ValidationError):
            TradeEvent(
                timestamp=datetime(2025, 1, 2, 9, 30, 0),
                symbol="AAPL",
                price=190.50,
                volume=-100
            )
    
    def test_missing_required_field(self):
        """Test that missing required fields raise validation error."""
        with pytest.raises(ValidationError):
            TradeEvent(
                timestamp=datetime(2025, 1, 2, 9, 30, 0),
                symbol="AAPL",
                price=190.50
                # volume missing
            )


class TestTradeIngestRequest:
    """Tests for TradeIngestRequest model."""
    
    def test_valid_request_single_trade(self):
        """Test valid request with single trade."""
        request = TradeIngestRequest(
            trades=[
                TradeEvent(
                    timestamp=datetime(2025, 1, 2, 9, 30, 0),
                    symbol="AAPL",
                    price=190.50,
                    volume=1200000
                )
            ]
        )
        assert len(request.trades) == 1
    
    def test_valid_request_multiple_trades_same_symbol(self):
        """Test valid request with multiple trades of same symbol."""
        request = TradeIngestRequest(
            trades=[
                TradeEvent(
                    timestamp=datetime(2025, 1, 2, 9, 30, 0),
                    symbol="AAPL",
                    price=190.50,
                    volume=1200000
                ),
                TradeEvent(
                    timestamp=datetime(2025, 1, 2, 9, 31, 0),
                    symbol="AAPL",
                    price=191.00,
                    volume=850000
                )
            ]
        )
        assert len(request.trades) == 2
    
    def test_empty_trades_list_rejected(self):
        """Test that empty trades list is rejected."""
        with pytest.raises(ValidationError) as exc_info:
            TradeIngestRequest(trades=[])
        assert "min_length" in str(exc_info.value).lower() or "at least" in str(exc_info.value).lower()
    
    def test_inconsistent_symbols_rejected(self):
        """Test that trades with different symbols are rejected."""
        with pytest.raises(ValidationError) as exc_info:
            TradeIngestRequest(
                trades=[
                    TradeEvent(
                        timestamp=datetime(2025, 1, 2, 9, 30, 0),
                        symbol="AAPL",
                        price=190.50,
                        volume=1200000
                    ),
                    TradeEvent(
                        timestamp=datetime(2025, 1, 2, 9, 31, 0),
                        symbol="MSFT",
                        price=410.50,
                        volume=730000
                    )
                ]
            )
        assert "same symbol" in str(exc_info.value).lower()


class TestOHLCVQueryParams:
    """Tests for OHLCVQueryParams model."""
    
    def test_valid_query_symbol_only(self):
        """Test valid query with symbol only."""
        params = OHLCVQueryParams(symbol="AAPL")
        assert params.symbol == "AAPL"
        assert params.interval == IntervalEnum.ONE_MIN
    
    def test_valid_query_with_all_params(self):
        """Test valid query with all parameters."""
        params = OHLCVQueryParams(
            symbol="AAPL",
            start=datetime(2025, 1, 2, 9, 0, 0),
            end=datetime(2025, 1, 2, 17, 0, 0),
            interval=IntervalEnum.FIVE_MIN
        )
        assert params.symbol == "AAPL"
        assert params.interval == IntervalEnum.FIVE_MIN
    
    def test_symbol_normalized_to_uppercase(self):
        """Test that symbol is normalized to uppercase."""
        params = OHLCVQueryParams(symbol="aapl")
        assert params.symbol == "AAPL"
    
    def test_invalid_date_range(self):
        """Test that start after end is rejected."""
        with pytest.raises(ValidationError) as exc_info:
            OHLCVQueryParams(
                symbol="AAPL",
                start=datetime(2025, 1, 2, 17, 0, 0),
                end=datetime(2025, 1, 2, 9, 0, 0)
            )
        assert "before end" in str(exc_info.value).lower()
    
    def test_valid_intervals(self):
        """Test all valid interval values."""
        for interval in IntervalEnum:
            params = OHLCVQueryParams(symbol="AAPL", interval=interval)
            assert params.interval == interval

