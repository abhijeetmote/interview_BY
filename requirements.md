# FastAPI Financial Time-Series Endpoints Design
 
## Overview
 
Design two FastAPI endpoints for processing financial time-series CSV data with the following schema:
 
- `timestamp`
- `symbol`
- `open`
- `high`
- `low`
- `close`
- `volume`
 
---
 
## Endpoint Specifications
 
### 1. POST `/v1/trades/ingest`
 
#### Purpose
 
Ingests trade event data for a single financial symbol.
 
#### Request Body
 
Accepts a JSON body containing a list of trade events with the following structure:
 
- `timestamp` - Time of the trade event
- `symbol` - Trading symbol identifier
- `price` - Trade execution price
- `volume` - Trade volume
 
#### Implementation Requirements
 
**Validation & Data Models**
 
- Use Pydantic models for request body validation
- Validate for:
  - Empty list scenarios
  - Inconsistent symbols within the same request
  - Incorrect data types
  - Required field presence
 


**Data Storage**
 
- Write/append data to CSV storage
- Ensure data availability for the GET endpoint
- Implement idempotency mechanisms to handle duplicate ingests
 
**Error Handling**
 
- Define appropriate HTTP status codes for various error scenarios:
  - `200 OK` - Successful ingestion
  - `400 Bad Request` - Invalid input data
  - `422 Unprocessable Entity` - Validation errors
  - `500 Internal Server Error` - Server-side failures
 
**Code Structure**
 
- Separate route handlers from business logic
- Implement service layer for data processing
- Use repository pattern for data access
 
**Testing Strategy**
 
- Unit tests for validation logic
- Integration tests for end-to-end workflow
- Test edge cases (empty lists, duplicates, invalid data)
 
---
 
### 2. GET `/v1/stats/ohlc`
 
#### Purpose
 
Retrieves aggregated OHLCV (Open, High, Low, Close, Volume) statistics for a specified symbol and time range.
 
#### Query Parameters
 
| Parameter  | Required | Description             | Valid Values               |
| ---------- | -------- | ----------------------- | -------------------------- |
| `symbol`   | Yes      | Trading symbol to query | String                     |
| `start`    | No       | Start of time range     | ISO 8601 date format       |
| `end`      | No       | End of time range       | ISO 8601 date format       |
| `interval` | No       | Aggregation interval    | `1min`, `5min`, `1h`, `1d` |
 
#### Data Processing Flow
 
1. Load CSV data into Pandas DataFrame
2. Filter by symbol and time range
3. Resample data to requested interval
4. Aggregate OHLCV metrics
5. Return structured JSON response
 
#### Response Schema
 
```json
{
  "symbol": "string",
  "interval": "string",
  "data": [
    {
      "timestamp": "ISO 8601 string",
      "open": "float",
      "high": "float",
      "low": "float",
      "close": "float",
      "volume": "int"
    }
  ]
}
```
 
#### Route Definition & Validation
 
- Define query parameter models with Pydantic
- Validate interval against allowed values
- Validate date formats (ISO 8601 standard)
- Ensure `symbol` parameter is provided
 
#### Error Handling
 
| Error Scenario     | HTTP Status Code            | Description                      |
| ------------------ | --------------------------- | -------------------------------- |
| Invalid parameters | `400 Bad Request`           | Malformed query parameters       |
| Unknown symbol     | `404 Not Found`             | Symbol not found in dataset      |
| No data available  | `404 Not Found`             | No data for specified time range |
| Invalid interval   | `422 Unprocessable Entity`  | Interval not in allowed list     |
| Server error       | `500 Internal Server Error` | Unexpected processing failure    |
 
---
 
### Testing Approach
 
- **Unit Tests**: Test individual functions and validation logic
- **Integration Tests**: Test complete request/response cycles
- **Mock Data**: Use fixture data for consistent testing
- **Edge Cases**: Test boundary conditions and error scenarios
 

---

sample 
{
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
      "price": 191.10,
      "volume": 850000
    },
    {
      "timestamp": "2025-01-02T09:32:00Z",
      "symbol": "AAPL",
      "price": 191.30,
      "volume": 640000
    },
    {
      "timestamp": "2025-01-02T09:33:00Z",
      "symbol": "MSFT",
      "price": 191.60,
      "volume": 500000
    },
    {
      "timestamp": "2025-01-02T09:34:00Z",
      "symbol": "AAPL",
      "price": 192.00,
      "volume": 450000
    }
  ]
}
  
 