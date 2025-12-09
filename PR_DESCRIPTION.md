# Enhanced Live Queries API to Track Recently Finished Queries

## Description
Adds support for storing and returning recently finished (completed/cancelled) queries through the live queries API.

## Changes
- Enhanced `LiveQueriesCache` to track finished queries with 30-second retention
- Added `include_finished` parameter to live queries API
- Implemented smart tracking that auto-starts on API call and stops after 5 minutes of inactivity
- Uses PriorityQueue for efficient expiration (O(log n) insertion, O(1) peek/poll)
- Lazy expiration on read - zero overhead when not in use

## Configuration
- **Retention**: 30 seconds maximum for finished queries
- **Capacity**: 1000 query limit with automatic FIFO cleanup
- **Tracking**: Auto-starts on first API call, stops after 5 minutes of inactivity

## API Usage
```bash
# Get finished queries
GET /_insights/live_queries?cached=true&include_finished=true

# Get live running queries (existing behavior)
GET /_insights/live_queries?cached=true
```

## Testing
- Added unit tests in `LiveQueriesCacheTests`
- Verified capacity limits and retention behavior
- Tested auto-start/stop tracking mechanism

## Performance Impact
- Minimal: Only tracks when API is actively used
- Memory: ~200KB max (1000 queries × ~200 bytes)
- CPU: Piggybacks on existing 100ms polling cycle
