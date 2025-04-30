## Version 2.19.2.0 Release Notes
Compatible with OpenSearch 2.19.2

### Maintenance
* Reduce LocalIndexReader size to 50 ([#281](https://github.com/opensearch-project/query-insights/pull/281))


### Bug Fixes
* Fix unit test SearchQueryCategorizerTests.testFunctionScoreQuery ([#270](https://github.com/opensearch-project/query-insights/pull/270))
* Fix bugs in top_queries, including a wrong illegal argument exception and size limit ([#293](https://github.com/opensearch-project/query-insights/pull/293))
* Use ClusterStateRequest with index pattern when searching for expired local indices ([#262](https://github.com/opensearch-project/query-insights/pull/262))
* Add strict hash check on top queries indices ([#266](https://github.com/opensearch-project/query-insights/pull/266))
* Add default index template for query insights local index ([#255](https://github.com/opensearch-project/query-insights/pull/255))
* Fix local index deletion timing ([#297](https://github.com/opensearch-project/query-insights/pull/297))
* Skip profile queries ([#298](https://github.com/opensearch-project/query-insights/pull/298))
* Add top_queries API verbose param ([#300](https://github.com/opensearch-project/query-insights/pull/300))


### Infrastructure
* Add more integ tests for exporter n reader ([#267](https://github.com/opensearch-project/query-insights/pull/267))


### Documentation
* 2.19.2 Release Notes ([#323](https://github.com/opensearch-project/query-insights/pull/323))
