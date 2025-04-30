## Version 3.0.0.0 Release Notes
Compatible with OpenSearch 3.0.0

### Maintenance
* Bump version to 3.0.0-alpha1 & upgrade to gradle 8.10.2 ([#247](https://github.com/opensearch-project/query-insights/pull/247))
* Fix default exporter settings ([#234](https://github.com/opensearch-project/query-insights/pull/234))
* Change local index replica count to 0 ([#257](https://github.com/opensearch-project/query-insights/pull/257))
* Use ClusterStateRequest with index pattern when searching for expired local indi ces ([#262](https://github.com/opensearch-project/query-insights/pull/262))
* Reduce MAX_TOP_N_INDEX_READ_SIZE to 50, sort by desc latency ([#281](https://github.com/opensearch-project/query-insights/pull/281))
* Update 3.0.0 qualifier from alpha1 to beta1 ([#290](https://github.com/opensearch-project/query-insights/pull/290))
* Support phasing off SecurityManager usage in favor of Java Agent ([#296](https://github.com/opensearch-project/query-insights/pull/296))
* Using java-agent gradle plugin to phase off Security Manager in favor of Java-ag ent. ([#303](https://github.com/opensearch-project/query-insights/pull/303))
* integ tests for exporter n reader ([#267](https://github.com/opensearch-project/query-insights/pull/267))
* Remove beta1 qualifier ([#329](https://github.com/opensearch-project/query-insights/pull/329))
* Increment version to 3.1.0-SNAPSHOT ([#325](https://github.com/opensearch-project/query-insights/pull/325))

### Bug Fixes
* Fix github upload artifact error ([#229](https://github.com/opensearch-project/query-insights/pull/229))
* fix unit test SearchQueryCategorizerTests.testFunctionScoreQuery ([#270](https://github.com/opensearch-project/query-insights/pull/270))
* Fix pipeline on main branch ([#274](https://github.com/opensearch-project/query-insights/pull/274))
* Fix local index deletion timing ([#289](https://github.com/opensearch-project/query-insights/pull/289))
* Top queries api bugs ([#293](https://github.com/opensearch-project/query-insights/pull/293))
* Fix CVE-2025-27820 ([#317](https://github.com/opensearch-project/query-insights/pull/317))
* fix flaky live queries tests ([#335](https://github.com/opensearch-project/query-insights/pull/335))

### Enhancements
* Add strict hash check on top queries indices ([#266](https://github.com/opensearch-project/query-insights/pull/266))
* Add default index template for query insights local index ([#254](https://github.com/opensearch-project/query-insights/pull/254))
* Skip profile queries ([#298](https://github.com/opensearch-project/query-insights/pull/298))
* Add top_queries API verbose param ([#300](https://github.com/opensearch-project/query-insights/pull/300))
* Feat integ tests for exporter n reader ([#310](https://github.com/opensearch-project/query-insights/pull/310))
* Inflight Queries API ([#295](https://github.com/opensearch-project/query-insights/pull/295))

### Documentation
* Release Notes 3.0.0.0-Alpha1 ([#278](https://github.com/opensearch-project/query-insights/pull/278))
* 3.0.0.0-beta1 Release Notes ([#294](https://github.com/opensearch-project/query-insights/pull/294))