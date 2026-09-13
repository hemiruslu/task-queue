# Go Concurrent Task Queue & Worker Pool

A high-performance, concurrent background job processing REST API built entirely with Go's standard library. This project demonstrates production-ready system design principles, including thread-safe state management, backpressure handling, and graceful termination.

## Core Architecture

* **Worker Pool Pattern:** Utilizes Goroutines and buffered channels to process background tasks asynchronously without blocking the main HTTP thread.
* **Thread-Safe State Management:** Implements `sync.Mutex` to ensure data integrity, preventing race conditions when multiple workers update job statuses simultaneously.
* **Load Shedding (Backpressure):** Protects system resources during traffic spikes. If the queue reaches capacity, the API immediately rejects new requests with an HTTP 503 status rather than exhausting memory or causing client timeouts.
* **Graceful Shutdown:** Intercepts OS-level signals (`SIGINT`, `SIGTERM`) and utilizes `sync.WaitGroup` and `context.Timeout`. This ensures the server stops accepting new traffic while allowing all active workers to complete their in-flight processes before terminating, guaranteeing zero data loss.

## Endpoints

### 1. Submit a Job
Queues a new job for asynchronous processing.

**Request:**
`POST /add`
Content-Type: `application/json`

```json
{
  "payload": "image_processing_01"
}