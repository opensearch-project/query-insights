## Version 3.5.0 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 3.5.0

### Features
* Add username and user roles to top n queries through thread context extraction ([#508](https://github.com/opensearch-project/query-insights/pull/508))
* Add wrapper endpoints for query insights settings ([#491](https://github.com/opensearch-project/query-insights/pull/491))

### Enhancements
* Add integTest script in query insights to support multinode run on Jenkins ([#533](https://github.com/opensearch-project/query-insights/pull/533))
* Add integTestRemote target to support remote cluster testing ([#530](https://github.com/opensearch-project/query-insights/pull/530))
* Better strategy to identify missing mapping fields ([#519](https://github.com/opensearch-project/query-insights/pull/519))
* Change timestamp field to date type ([#523](https://github.com/opensearch-project/query-insights/pull/523))
* Delay username and user roles extraction to after Top N Filtering ([#527](https://github.com/opensearch-project/query-insights/pull/527))
* Store source field as a string in local index to optimize query storage ([#483](https://github.com/opensearch-project/query-insights/pull/483))
* Truncate source string in local index to optimize query storage ([#484](https://github.com/opensearch-project/query-insights/pull/484))

### Bug Fixes
* Fix Installation Documentation ([#512](https://github.com/opensearch-project/query-insights/pull/512))
* Fix excluded indices integ test ([#495](https://github.com/opensearch-project/query-insights/pull/495))
* Remove expired indices check on start-up ([#521](https://github.com/opensearch-project/query-insights/pull/521))
* Retain local indices when exporter type is changed ([#465](https://github.com/opensearch-project/query-insights/pull/465))
* Fix flaky test testTimeFilterIncludesSomeRecords ([#518](https://github.com/opensearch-project/query-insights/pull/518))
* Fix flaky testTopQueriesResponses ([#513](https://github.com/opensearch-project/query-insights/pull/513))

### Refactoring
* Remove index template functionality from LocalIndexExporter ([#479](https://github.com/opensearch-project/query-insights/pull/479))