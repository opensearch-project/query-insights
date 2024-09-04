## Version 2.17.0.0 Release Notes
Compatible with OpenSearch 2.17.0

### Maintenance
* Fix CVE-2023-2976 for checkstyle ([#58](https://github.com/opensearch-project/query-insights/pull/58))
* Fix security based integration tests ([#59](https://github.com/opensearch-project/query-insights/pull/59))
* Add query shape hash method ([#64](https://github.com/opensearch-project/query-insights/pull/64))
* Add more integration tests for query insights ([#71](https://github.com/opensearch-project/query-insights/pull/71))

### Bug Fixes
* Make sure listener is started when query metrics enabled ([#74](https://github.com/opensearch-project/query-insights/pull/74))

### Enhancements
* Add ability to generate query shape for aggregation and sort portions of search body ([#44](https://github.com/opensearch-project/query-insights/pull/44))
* Query grouping framework for Top N queries and group by query similarity ([#66](https://github.com/opensearch-project/query-insights/pull/66))
* Minor enhancements to query categorization on tags and unit types ([#73](https://github.com/opensearch-project/query-insights/pull/73))

### Infrastructure
* Add code hygiene checks for query insights [#51](https://github.com/opensearch-project/query-insights/pull/51)
* Add configuration for publishing snapshot ([#90](https://github.com/opensearch-project/query-insights/pull/90))

### Documentation
* Update GET top N api documentation about the type parameter ([#8139](https://github.com/opensearch-project/documentation-website/pull/8139))
* Added 2.17 release notes ([#91](https://github.com/opensearch-project/query-insights/pull/91))
