## Version 3.0.0.0-alpha1 Release Notes
Compatible with OpenSearch 3.0.0-alpha1

### Maintenance
* Bump version to 3.0.0-alpha1 & upgrade to gradle 8.10.2 ([#247](https://github.com/opensearch-project/query-insights/pull/247))

### Bug Fixes
* Fix github upload artifact error ([#229](https://github.com/opensearch-project/query-insights/pull/229))
* Fix default exporter settings ([#234](https://github.com/opensearch-project/query-insights/pull/234))
* Fix unit test SearchQueryCategorizerTests.testFunctionScoreQuery ([#270](https://github.com/opensearch-project/query-insights/pull/270))

### Enhancements
* Add default index template for query insights local index ([#254](https://github.com/opensearch-project/query-insights/pull/254))
* Change local index replica count to 0 ([#257](https://github.com/opensearch-project/query-insights/pull/257))
* Use ClusterStateRequest with index pattern when searching for expired local indices  ([#262](https://github.com/opensearch-project/query-insights/pull/262))
* Add strict hash check on top queries indices ([#266](https://github.com/opensearch-project/query-insights/pull/266))

### Infrastructure
* Fix pipeline on main branch ([#274](https://github.com/opensearch-project/query-insights/pull/274))

### Documentation
* 3.0.0.0-Alpha1 Release Notes ([#278](https://github.com/opensearch-project/query-insights/pull/278))

