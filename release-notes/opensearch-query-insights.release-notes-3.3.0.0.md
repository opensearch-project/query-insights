## Version 3.3.0 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 3.3.0

### Features
* Add Workload Management (WLM) Group Filtering for Live Queries API ([#389](https://github.com/opensearch-project/query-insights/pull/389))

### Bug Fixes
* Change matchQuery to termQuery to correctly determine if query IDs match ([#426](https://github.com/opensearch-project/query-insights/pull/426))
* Filter out shard-level tasks from live queries ([#420](https://github.com/opensearch-project/query-insights/pull/420))
* Fix missing check for time range validation that from timestamp is before to timestamp ([#413](https://github.com/opensearch-project/query-insights/pull/413))
* Fix validation check for positive size parameter in live queries ([#414](https://github.com/opensearch-project/query-insights/pull/414))
* Fix flaky testTopQueriesResponses by clearing stale records from queues when disabling metrics ([#430](https://github.com/opensearch-project/query-insights/pull/430))
* Fix: Update System.env syntax for Gradle 9 compatibility ([#407](https://github.com/opensearch-project/query-insights/pull/407))

### Maintenance
* [AUTO] Increment version to 3.3.0-SNAPSHOT ([#404](https://github.com/opensearch-project/query-insights/pull/404))

### Infrastructure
* Add unit tests when clear the shared queue ([#435](https://github.com/opensearch-project/query-insights/pull/435))