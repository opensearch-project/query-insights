# Track Recently Finished Queries in Live Queries API

## Summary
Enhances the live queries API to store and return recently finished (completed/cancelled) queries, enabling users to track queries that just completed.

## Changes
- Added `FinishedQueriesCache` to track recently finished queries
- Enhanced `LiveQueriesCache` to detect when queries finish and populate finished cache
- Added `include_finished` query parameter to live queries API
- Updated `LiveQueriesResponse` to support separate `live_queries` and `finished_queries` arrays

## Configuration
- **Retention**: 30 seconds for finished queries
- **Capacity**: 1000 query limit with FIFO eviction
- **Tracking**: Auto-starts on API call, stops after 5 minutes of inactivity
- **Data Structure**: PriorityQueue for O(log n) insertion and O(1) expiration

## API Usage
```bash
# Get both live and finished queries
GET /_insights/live_queries?cached=true&include_finished=true

# Get only live queries (default)
GET /_insights/live_queries?cached=true
```

## Response Format
```json
{
  "live_queries": [...],
  "finished_queries": [...]  // Only present when include_finished=true
}
```

## Implementation Details
- Finished queries detected by comparing consecutive polling snapshots
- Lazy expiration on read - no overhead during polling
- Uses existing SearchQueryRecord model (no new data structures)
- Zero overhead when not in use

## Performance Impact
- Memory: ~200KB max (1000 queries)
- CPU: Minimal - piggybacks on existing 100ms polling
- Network: Only when API is called with `include_finished=true`
