## Version 3.5.0 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 3.5.0

### Features

* Add username and user roles to top n queries ([#508](https://github.com/opensearch-project/query-insights/pull/508))
* Add wrapper endpoints for query insights settings ([#491](https://github.com/opensearch-project/query-insights/pull/491))

### Enhancements

* Better strategy to identify missing mapping fields ([#519](https://github.com/opensearch-project/query-insights/pull/519))
* Delay username and user roles extraction to after Top N Filtering ([#527](https://github.com/opensearch-project/query-insights/pull/527))
* Retain local indices on exporter type change ([#465](https://github.com/opensearch-project/query-insights/pull/465))
* Store source field as a string in local index to optimize query storage ([#483](https://github.com/opensearch-project/query-insights/pull/483))
* Truncate source string in local index to optimize query storage ([#484](https://github.com/opensearch-project/query-insights/pull/484))

### Bug Fixes

* Change timestamp field to date type ([#523](https://github.com/opensearch-project/query-insights/pull/523))
* Fix excluded indices integ test ([#495](https://github.com/opensearch-project/query-insights/pull/495))
* Remove expired indices check on start-up ([#521](https://github.com/opensearch-project/query-insights/pull/521))
* Fix flaky test testTimeFilterIncludesSomeRecords ([#518](https://github.com/opensearch-project/query-insights/pull/518))
* Fix flaky testTopQueriesResponses ([#513](https://github.com/opensearch-project/query-insights/pull/513))

### Infrastructure

* Add integTest script in query insights to support multinode run on Jenkins ([#533](https://github.com/opensearch-project/query-insights/pull/533))
* Add integTestRemote target to support remote cluster testing ([#530](https://github.com/opensearch-project/query-insights/pull/530))

### Documentation

* Fix Installation Documentation ([#512](https://github.com/opensearch-project/query-insights/pull/512))

### Refactoring

* Remove index template ([#479](https://github.com/opensearch-project/query-insights/pull/479))