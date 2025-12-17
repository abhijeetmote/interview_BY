"""FastAPI application for financial time-series endpoints."""
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, HTTPException, Query, status
from fastapi.responses import JSONResponse
from pydantic import ValidationError

from models import (
    ErrorResponse,
    IntervalEnum,
    OHLCVResponse,
    TradeIngestRequest,
    TradeIngestResponse,
)
from services import (
    DataNotFoundError,
    TradeIngestionError,
    TradeService,
    get_trade_service,
)


# Create FastAPI application
app = FastAPI(
    title="Financial Time-Series API",
    description="API for ingesting and querying financial trade data",
    version="1.0.0",
)


# Dependency injection for service
def get_service() -> TradeService:
    """Get trade service instance."""
    return get_trade_service()


@app.post(
    "/v1/trades/ingest",
    response_model=TradeIngestResponse,
    responses={
        200: {"model": TradeIngestResponse, "description": "Successful ingestion"},
        400: {"model": ErrorResponse, "description": "Invalid input data"},
        422: {"model": ErrorResponse, "description": "Validation errors"},
        500: {"model": ErrorResponse, "description": "Server-side failure"},
    },
    summary="Ingest Trade Events",
    description="Ingests trade event data for a single financial symbol.",
)
async def ingest_trades(request: TradeIngestRequest) -> TradeIngestResponse:
    """
    Ingest trade event data for a single financial symbol.
    
    - **trades**: List of trade events with timestamp, symbol, price, and volume
    - All trades in a single request must have the same symbol
    - Duplicate trades are detected and skipped (idempotency)
    """
    try:
        service = get_service()
        response = service.ingest_trades(request.trades)
        return response
    except TradeIngestionError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Unexpected error during ingestion: {str(e)}"
        )


@app.get(
    "/v1/stats/ohlc",
    response_model=OHLCVResponse,
    responses={
        200: {"model": OHLCVResponse, "description": "Successful retrieval"},
        400: {"model": ErrorResponse, "description": "Malformed query parameters"},
        404: {"model": ErrorResponse, "description": "Symbol or data not found"},
        422: {"model": ErrorResponse, "description": "Invalid interval value"},
        500: {"model": ErrorResponse, "description": "Unexpected processing failure"},
    },
    summary="Get OHLCV Statistics",
    description="Retrieves aggregated OHLCV statistics for a specified symbol and time range.",
)
async def get_ohlcv_stats(
    symbol: str = Query(
        ..., 
        min_length=1, 
        description="Trading symbol to query"
    ),
    start: Optional[datetime] = Query(
        None, 
        description="Start of time range (ISO 8601 format)"
    ),
    end: Optional[datetime] = Query(
        None, 
        description="End of time range (ISO 8601 format)"
    ),
    interval: Optional[IntervalEnum] = Query(
        IntervalEnum.ONE_MIN,
        description="Aggregation interval: 1min, 5min, 1h, 1d"
    ),
) -> OHLCVResponse:
    """
    Retrieve aggregated OHLCV statistics for a specified symbol and time range.
    
    - **symbol**: Trading symbol (required)
    - **start**: Start of time range in ISO 8601 format (optional)
    - **end**: End of time range in ISO 8601 format (optional)
    - **interval**: Aggregation interval - 1min, 5min, 1h, 1d (default: 1min)
    """
    # Validate date range
    if start and end and start >= end:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Start date must be before end date"
        )
    
    try:
        service = get_service()
        response = service.get_ohlcv_stats(
            symbol=symbol,
            start=start,
            end=end,
            interval=interval or IntervalEnum.ONE_MIN
        )
        return response
    except DataNotFoundError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Unexpected error during data retrieval: {str(e)}"
        )


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy"}


# Custom exception handler for validation errors
@app.exception_handler(ValidationError)
async def validation_exception_handler(request, exc: ValidationError):
    """Handle Pydantic validation errors."""
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={"detail": exc.errors()}
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
