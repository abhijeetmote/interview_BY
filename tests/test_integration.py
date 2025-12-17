"""Integration tests for FastAPI endpoints."""
import pytest
import tempfile
from pathlib import Path
from datetime import datetime

from fastapi.testclient import TestClient

from main import app
from repository import reset_repository
from services import reset_trade_service


@pytest.fixture
def temp_csv():
    """Create a temporary CSV file for testing."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        temp_path = Path(f.name)
    yield temp_path
    # Cleanup
    if temp_path.exists():
        temp_path.unlink()


@pytest.fixture
def client(temp_csv):
    """Create test client with isolated repository."""
    # Reset repository with fresh temp file
    repo = reset_repository(temp_csv)
    repo.clear_data()  # Ensure fresh state
    reset_trade_service(repo)
    
    yield TestClient(app)
    
    # Cleanup after test
    repo.clear_data()


class TestHealthEndpoint:
    """Tests for health check endpoint."""
    
    def test_health_check(self, client):
        """Test health check returns healthy status."""
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json() == {"status": "healthy"}


class TestIngestEndpoint:
    """Integration tests for POST /v1/trades/ingest."""
    
    def test_successful_ingestion_single_trade(self, client):
        """Test successful ingestion of a single trade."""
        payload = {
            "trades": [
                {
                    "timestamp": "2025-01-02T09:30:00Z",
                    "symbol": "AAPL",
                    "price": 190.50,
                    "volume": 1200000
                }
            ]
        }
        response = client.post("/v1/trades/ingest", json=payload)
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "success"
        assert data["symbol"] == "AAPL"
        assert data["records_ingested"] == 1
        assert data["duplicates_skipped"] == 0
    
    def test_successful_ingestion_multiple_trades(self, client):
        """Test successful ingestion of multiple trades."""
        payload = {
            "trades": [
                {
                    "timestamp": "2025-01-02T09:30:00Z",
                    "symbol": "AAPL",
                    "price": 190.50,
                    "volume": 1200000
                },
                {
                    "timestamp": "2025-01-02T09:31:00Z",
                    "symbol": "AAPL",
                    "price": 191.10,
                    "volume": 850000
                }
            ]
        }
        response = client.post("/v1/trades/ingest", json=payload)
        assert response.status_code == 200
        data = response.json()
        assert data["records_ingested"] == 2
    
    def test_duplicate_detection(self, client):
        """Test that duplicate trades are detected and skipped."""
        payload = {
            "trades": [
                {
                    "timestamp": "2025-01-02T09:30:00Z",
                    "symbol": "AAPL",
                    "price": 190.50,
                    "volume": 1200000
                }
            ]
        }
        # First ingestion
        response1 = client.post("/v1/trades/ingest", json=payload)
        assert response1.status_code == 200
        assert response1.json()["records_ingested"] == 1
        
        # Second ingestion (duplicate)
        response2 = client.post("/v1/trades/ingest", json=payload)
        assert response2.status_code == 200
        assert response2.json()["records_ingested"] == 0
        assert response2.json()["duplicates_skipped"] == 1
    
    def test_empty_trades_list_rejected(self, client):
        """Test that empty trades list is rejected."""
        payload = {"trades": []}
        response = client.post("/v1/trades/ingest", json=payload)
        assert response.status_code == 422
    
    def test_inconsistent_symbols_rejected(self, client):
        """Test that trades with different symbols are rejected."""
        payload = {
            "trades": [
                {
                    "timestamp": "2025-01-02T09:30:00Z",
                    "symbol": "AAPL",
                    "price": 190.50,
                    "volume": 1200000
                },
                {
                    "timestamp": "2025-01-02T09:31:00Z",
                    "symbol": "MSFT",
                    "price": 410.50,
                    "volume": 730000
                }
            ]
        }
        response = client.post("/v1/trades/ingest", json=payload)
        assert response.status_code == 422
    
    def test_invalid_price_rejected(self, client):
        """Test that invalid price is rejected."""
        payload = {
            "trades": [
                {
                    "timestamp": "2025-01-02T09:30:00Z",
                    "symbol": "AAPL",
                    "price": -10.0,
                    "volume": 1200000
                }
            ]
        }
        response = client.post("/v1/trades/ingest", json=payload)
        assert response.status_code == 422
    
    def test_missing_required_field_rejected(self, client):
        """Test that missing required field is rejected."""
        payload = {
            "trades": [
                {
                    "timestamp": "2025-01-02T09:30:00Z",
                    "symbol": "AAPL",
                    "price": 190.50
                    # volume missing
                }
            ]
        }
        response = client.post("/v1/trades/ingest", json=payload)
        assert response.status_code == 422
    
    def test_invalid_timestamp_rejected(self, client):
        """Test that invalid timestamp format is rejected."""
        payload = {
            "trades": [
                {
                    "timestamp": "not-a-date",
                    "symbol": "AAPL",
                    "price": 190.50,
                    "volume": 1200000
                }
            ]
        }
        response = client.post("/v1/trades/ingest", json=payload)
        assert response.status_code == 422


class TestOHLCVEndpoint:
    """Integration tests for GET /v1/stats/ohlc."""
    
    def _ingest_sample_data(self, client):
        """Helper to ingest sample trade data."""
        payload = {
            "trades": [
                {
                    "timestamp": "2025-01-02T09:30:00Z",
                    "symbol": "AAPL",
                    "price": 190.50,
                    "volume": 1200000
                },
                {
                    "timestamp": "2025-01-02T09:31:00Z",
                    "symbol": "AAPL",
                    "price": 191.10,
                    "volume": 850000
                },
                {
                    "timestamp": "2025-01-02T09:32:00Z",
                    "symbol": "AAPL",
                    "price": 191.30,
                    "volume": 640000
                }
            ]
        }
        client.post("/v1/trades/ingest", json=payload)
    
    def test_get_ohlcv_success(self, client):
        """Test successful OHLCV retrieval."""
        self._ingest_sample_data(client)
        
        response = client.get("/v1/stats/ohlc", params={"symbol": "AAPL"})
        assert response.status_code == 200
        data = response.json()
        assert data["symbol"] == "AAPL"
        assert data["interval"] == "1min"
        assert len(data["data"]) == 3
    
    def test_get_ohlcv_with_time_range(self, client):
        """Test OHLCV retrieval with time range."""
        self._ingest_sample_data(client)
        
        response = client.get(
            "/v1/stats/ohlc",
            params={
                "symbol": "AAPL",
                "start": "2025-01-02T09:30:00Z",
                "end": "2025-01-02T09:31:00Z"
            }
        )
        assert response.status_code == 200
        data = response.json()
        assert len(data["data"]) == 2
    
    def test_get_ohlcv_with_5min_interval(self, client):
        """Test OHLCV retrieval with 5-minute aggregation."""
        self._ingest_sample_data(client)
        
        response = client.get(
            "/v1/stats/ohlc",
            params={"symbol": "AAPL", "interval": "5min"}
        )
        assert response.status_code == 200
        data = response.json()
        assert data["interval"] == "5min"
        # 3 trades within 3 minutes should aggregate into 1 5-min bar
        assert len(data["data"]) == 1
        # Verify aggregation
        bar = data["data"][0]
        assert bar["open"] == 190.50  # First price
        assert bar["close"] == 191.30  # Last price
        assert bar["high"] == 191.30  # Max price
        assert bar["low"] == 190.50  # Min price
        assert bar["volume"] == 2690000  # Sum of volumes
    
    def test_get_ohlcv_unknown_symbol_404(self, client):
        """Test that unknown symbol returns 404."""
        response = client.get("/v1/stats/ohlc", params={"symbol": "UNKNOWN"})
        assert response.status_code == 404
    
    def test_get_ohlcv_missing_symbol_422(self, client):
        """Test that missing symbol returns 422."""
        response = client.get("/v1/stats/ohlc")
        assert response.status_code == 422
    
    def test_get_ohlcv_invalid_interval_422(self, client):
        """Test that invalid interval returns 422."""
        self._ingest_sample_data(client)
        
        response = client.get(
            "/v1/stats/ohlc",
            params={"symbol": "AAPL", "interval": "invalid"}
        )
        assert response.status_code == 422
    
    def test_get_ohlcv_invalid_date_range_400(self, client):
        """Test that start after end returns 400."""
        self._ingest_sample_data(client)
        
        response = client.get(
            "/v1/stats/ohlc",
            params={
                "symbol": "AAPL",
                "start": "2025-01-02T17:00:00Z",
                "end": "2025-01-02T09:00:00Z"
            }
        )
        assert response.status_code == 400
    
    def test_get_ohlcv_no_data_in_range_404(self, client):
        """Test that no data in time range returns 404."""
        self._ingest_sample_data(client)
        
        response = client.get(
            "/v1/stats/ohlc",
            params={
                "symbol": "AAPL",
                "start": "2025-01-03T09:00:00Z",
                "end": "2025-01-03T17:00:00Z"
            }
        )
        assert response.status_code == 404


class TestEndToEndWorkflow:
    """End-to-end workflow tests."""
    
    def test_ingest_then_query_workflow(self, client):
        """Test complete workflow: ingest trades, then query OHLCV."""
        # Step 1: Ingest trades
        ingest_payload = {
            "trades": [
                {
                    "timestamp": "2025-01-02T09:30:00Z",
                    "symbol": "AAPL",
                    "price": 190.50,
                    "volume": 1200000
                },
                {
                    "timestamp": "2025-01-02T09:31:00Z",
                    "symbol": "AAPL",
                    "price": 191.10,
                    "volume": 850000
                }
            ]
        }
        ingest_response = client.post("/v1/trades/ingest", json=ingest_payload)
        assert ingest_response.status_code == 200
        
        # Step 2: Query OHLCV
        ohlcv_response = client.get("/v1/stats/ohlc", params={"symbol": "AAPL"})
        assert ohlcv_response.status_code == 200
        
        data = ohlcv_response.json()
        assert data["symbol"] == "AAPL"
        assert len(data["data"]) == 2
    
    def test_multiple_symbols_workflow(self, client):
        """Test ingesting and querying multiple symbols."""
        # Ingest AAPL trades
        aapl_payload = {
            "trades": [
                {
                    "timestamp": "2025-01-02T09:30:00Z",
                    "symbol": "AAPL",
                    "price": 190.50,
                    "volume": 1200000
                }
            ]
        }
        client.post("/v1/trades/ingest", json=aapl_payload)
        
        # Ingest MSFT trades
        msft_payload = {
            "trades": [
                {
                    "timestamp": "2025-01-02T09:30:00Z",
                    "symbol": "MSFT",
                    "price": 410.50,
                    "volume": 730000
                }
            ]
        }
        client.post("/v1/trades/ingest", json=msft_payload)
        
        # Query AAPL
        aapl_response = client.get("/v1/stats/ohlc", params={"symbol": "AAPL"})
        assert aapl_response.status_code == 200
        assert aapl_response.json()["symbol"] == "AAPL"
        
        # Query MSFT
        msft_response = client.get("/v1/stats/ohlc", params={"symbol": "MSFT"})
        assert msft_response.status_code == 200
        assert msft_response.json()["symbol"] == "MSFT"

