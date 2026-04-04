## Version 3.6.0 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 3.6.0

### Features
* Add RemoteRepositoryExporter to support exporting top N queries to remote blob store repositories ([#541](https://github.com/opensearch-project/query-insights/pull/541))
* Add recommendation data models for rule-based recommendation engine ([#549](https://github.com/opensearch-project/query-insights/pull/549))
* Add shard-level task details to the live queries API for full distributed search visibility ([#548](https://github.com/opensearch-project/query-insights/pull/548))
* Add streaming dimension to query categorization metrics ([#551](https://github.com/opensearch-project/query-insights/pull/551))
* Implement access control for query insights data with username and backend role filtering ([#552](https://github.com/opensearch-project/query-insights/pull/552))
* Add rule-based recommendation service for analyzing search queries with actionable suggestions ([#555](https://github.com/opensearch-project/query-insights/pull/555))
* Track failed queries by tagging them with a failed attribute ([#540](https://github.com/opensearch-project/query-insights/pull/540))
* Add finished queries cache to the live queries API for retrieving recently completed searches ([#554](https://github.com/opensearch-project/query-insights/pull/554))

### Bug Fixes
* Fix IS_STREAMING_TAG not propagated in incrementAggCounter due to immutable Tags object ([#570](https://github.com/opensearch-project/query-insights/pull/570))
* Fix MultiIndexDateRangeIT test failure ([#558](https://github.com/opensearch-project/query-insights/pull/558))
* Fix exporter retry logic for MapperParsingException by moving detection to onResponse callback ([#556](https://github.com/opensearch-project/query-insights/pull/556))
* Fix grouping field_name/field_type settings being overwritten to false on initialization ([#578](https://github.com/opensearch-project/query-insights/pull/578))
* Fix cluster setting validation by moving it to Setting definitions to prevent invalid persistent values ([#487](https://github.com/opensearch-project/query-insights/pull/487))
* Stop fetching security snapshot artifacts in non-snapshot builds ([#560](https://github.com/opensearch-project/query-insights/pull/560))

### Infrastructure
* Enable internalClusterTest and yamlRestTest tasks and fix uncovered test issues ([#522](https://github.com/opensearch-project/query-insights/pull/522))
* Exclude RemoteRepositoryExporterIT and TopQueriesRbacIT from integTestRemote ([#577](https://github.com/opensearch-project/query-insights/pull/577))
* Fix integTestRemote task spinning up unnecessary test cluster nodes by changing task type ([#587](https://github.com/opensearch-project/query-insights/pull/587))
* Revert cluster health check before running integration tests ([#594](https://github.com/opensearch-project/query-insights/pull/594))
* Add cluster health check before running integration tests to prevent connection failures ([#588](https://github.com/opensearch-project/query-insights/pull/588))
* Pin LocalStack version to v4.4 and increase health check timeout for CI stability ([#572](https://github.com/opensearch-project/query-insights/pull/572))
