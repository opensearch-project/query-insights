# Finished Queries Tracking Feature

## Overview
Enhanced the live queries API to store and return recently finished (completed/cancelled) queries, enabling users to track queries that just completed.

## Key Features

### Retention Policy
- **30 seconds maximum** retention for finished queries
- Queries older than 30 seconds are automatically removed on read

### Capacity Management
- **1000 query limit** with automatic cleanup
- When capacity is exceeded, oldest queries are removed first (FIFO)

### Smart Tracking
- **On-demand activation**: Tracking starts only when API is called
- **Auto-stop**: Stops tracking after 5 minutes of API inactivity
- **Zero overhead** when not in use

## API Usage

### Get Finished Queries
```bash
GET /_insights/live_queries?cached=true&include_finished=true
```

### Get Live Running Queries (existing)
```bash
GET /_insights/live_queries?cached=true
```

## Implementation Details

### Data Structure
- Single `PriorityQueue` ordered by finish time (oldest first)
- O(log n) insertion, O(1) peek/poll for expiration
- Lazy expiration on read (no polling overhead)

### Detection Logic
- Compares previous poll snapshot with current running queries
- Queries present in previous but not current = finished
- Runs every 100ms during existing polling cycle (no extra overhead)

### Memory Footprint
- ~200 bytes per query record
- Max 1000 queries = ~200KB maximum memory usage

## Benefits
- Track queries that complete between API calls
- Debug intermittent query issues
- Analyze recently completed query patterns
- Minimal performance impact (only when actively used)
