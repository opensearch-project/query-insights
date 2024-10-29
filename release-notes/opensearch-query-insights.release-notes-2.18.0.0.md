## Version 2.18.0.0 Release Notes
Compatible with OpenSearch 2.18.0

### Maintenance
* Enhanced security based integration tests ([#113](https://github.com/opensearch-project/query-insights/pull/113))
* Set default true for field name and type setting ([#144](https://github.com/opensearch-project/query-insights/pull/144))

### Bug Fixes
* Refactor parsing logic for Measurement ([#112](https://github.com/opensearch-project/query-insights/pull/112))

### Enhancements
* Support time range parameter to get historical top n queries from local index ([#84](https://github.com/opensearch-project/query-insights/pull/84))
* Refactor query shape field data maps to support the WithFieldName interface ([#111](https://github.com/opensearch-project/query-insights/pull/111))
* Add data models for health stats API ([#120](https://github.com/opensearch-project/query-insights/pull/120))
* Create health_stats API for query insights ([#122](https://github.com/opensearch-project/query-insights/pull/122))
* Add OpenTelemetry counters for error metrics ([#124](https://github.com/opensearch-project/query-insights/pull/124))
* Add grouping settings for query field name and type ([#135](https://github.com/opensearch-project/query-insights/pull/135))
* Add field type to query shape ([#140](https://github.com/opensearch-project/query-insights/pull/140))
* Adding cache eviction and listener for invalidating index field type mappings on index deletion/update ([#142](https://github.com/opensearch-project/query-insights/pull/142))

### Infrastructure
* Upgrade deprecated actions/upload-artifact versions to v3 ([#117](https://github.com/opensearch-project/query-insights/pull/117))

### Documentation
* Add document for Query Insights health_stats API and error counter metrics ([#8627](https://github.com/opensearch-project/documentation-website/pull/8627))
* Added 2.18 release notes ([#148](https://github.com/opensearch-project/query-insights/pull/))
