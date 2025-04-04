# Trade Data Enrichment Service

A Spring Boot-based service for enriching trade data with product information. Processes CSV files efficiently with streaming to handle large datasets.

## Table of Contents
- [Features](#features)
- [Prerequisites](#prerequisites)
- [Getting Started](#getting-started)
- [API Documentation](#api-documentation)
- [Architecture](#architecture)
- [Limitations](#limitations)
- [Future Improvements](#future-improvements)

## Features <a name="features"></a>
- CSV stream processing for 1M+ records
- Product ID to name mapping
- Data validation:
    - Date format (yyyyMMdd with strict validation)
    - Price formatting
- Thread-safe operations
- Batch processing optimizations

## Prerequisites <a name="prerequisites"></a>
- Java 17+
- Maven 3.6+
- 4GB+ RAM (for large datasets)

## Getting Started <a name="getting-started"></a>

### 1. Build & Run
```bash
mvn clean package
java -jar target/trade-enrich-service-1.0.0.jar
```
### 2. Generate Test Data
```java
// Generate 100K products and 1M trades
TestDataGenerator.generateProductCsv(Paths.get("products.csv"), 100_000);
TestDataGenerator.generateTradeCsv(Paths.get("trades.csv"), 1_000_000, 100_000);
```
## API Documentation <a name="api-documentation"></a>
### 1. Initialize Products
```bash
   curl -X POST -H "Content-Type: text/csv" \
   --data-binary @products.csv \
   http://localhost:8080/api/v1/product/init
```
### 2. Update Products
```bash
   curl -X POST -H "Content-Type: text/csv" \
   --data-binary @product_updates.csv \
   http://localhost:8080/api/v1/product/update
```
### 3. Enrich Trades
```bash
   curl -X POST -H "Content-Type: text/csv" \
   --data-binary @trades.csv \
   http://localhost:8080/api/v1/enrich > enriched_trades.csv
   ```
   Response Format:
```csv
date,productName,currency,price
20230101,Treasury Bills Domestic,EUR,10.5
20230101,Missing Product Name,USD,25.0
```
## Architecture <a name="architecture"></a>
### Key Design Decisions
#### Stream Processing

Uses StreamingResponseBody for O(1) memory consumption

Batched flushing (every 1K records)

#### Concurrency

ConcurrentHashMap for thread-safe product data

Atomic map replacement during full updates

#### Validation

Strict date parsing (Rejects 2023-02-29)

Graceful error handling with error logging

#### Resource Management

Automatic input stream closing

Try-with-resources for file operations

## Limitations <a name="limitations"></a>
### Memory Constraints

Product data must fit in memory (~100MB per 1M entries)

No disk-backed caching

### Data Validation

No currency code validation

Limited price validation (only numeric check)

### Performance

Single-node architecture

No request prioritization

### API Features

No pagination/sorting/filtering

CSV-only input/output

## Future Improvements <a name="future-improvements"></a>
### Immediate Priorities
Performance

Add Redis caching for product data

Implement Spring Batch for distributed processing

Resilience

Circuit breakers for bulk operations

Retry mechanisms for transient failures

Observability

Prometheus metrics endpoint

Request tracing with OpenTelemetry

API Enhancements

