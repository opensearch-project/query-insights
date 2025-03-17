## Version 2.19.0.0 Release Notes
Compatible with OpenSearch 2.19.0

### Maintenance
* Remove local index custom name setting ([#166](https://github.com/opensearch-project/query-insights/pull/166))
* Migrate from Joda-Time to java.time API ([#176](https://github.com/opensearch-project/query-insights/pull/176))
* Change default values for window,n and exporter ([#196](https://github.com/opensearch-project/query-insights/pull/196))
* Bump up version for upload-artifact to fix the build ([#202](https://github.com/opensearch-project/query-insights/pull/202))
* Update default .enabled and exporter.type settings ([#217](https://github.com/opensearch-project/query-insights/pull/217))

### Bug Fixes
* Fix parsing error in SearchQueryRecord ([#184](https://github.com/opensearch-project/query-insights/pull/184))
* Fix bug on node_id missing in local index ([#207](https://github.com/opensearch-project/query-insights/pull/207))
* Fix null indexFieldMap bug & add UTs ([#214](https://github.com/opensearch-project/query-insights/pull/214))
* Fix toString on operational metrics ([#219](https://github.com/opensearch-project/query-insights/pull/219))
* Fix grouping integ tests ([#223](https://github.com/opensearch-project/query-insights/pull/223))

### Enhancements
* Usage counter for Top N queries ([#153](https://github.com/opensearch-project/query-insights/pull/153))
* Add type attribute to search query record ([#157](https://github.com/opensearch-project/query-insights/pull/157))
* Make default window size valid for all metric types ([#156](https://github.com/opensearch-project/query-insights/pull/156))
* Top N indices auto deletion config & functionality ([#172](https://github.com/opensearch-project/query-insights/pull/172))
* Make sure query_group_hashcode is present in top_queries response in all cases ([#187](https://github.com/opensearch-project/query-insights/pull/187))
* Model changes for hashcode and id ([#191](https://github.com/opensearch-project/query-insights/pull/191))
* Add fetch top queries by id API ([#195](https://github.com/opensearch-project/query-insights/pull/195))
* Add field type cache stats ([#193](https://github.com/opensearch-project/query-insights/pull/193))
* Always collect available metrics in top queries service ([#205](https://github.com/opensearch-project/query-insights/pull/205))
* Refactor Exporters and Readers ([#210](https://github.com/opensearch-project/query-insights/pull/210))

### Infrastructure
* Fix 2.x github checks ([#171](https://github.com/opensearch-project/query-insights/pull/171))
* Fix github CI by adding eclipse dependency in build.gradle ([#181](https://github.com/opensearch-project/query-insights/pull/181))

### Documentation
* 2.19 Release Notes ([#225](https://github.com/opensearch-project/query-insights/pull/225))
