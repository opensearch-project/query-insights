## Version 3.10.0 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 3.10.0

### Features

* Show running PPL queries as single entries in Live Queries instead of listing each child search separately ([#699](https://github.com/opensearch-project/query-insights/pull/699))

### Bug Fixes

* Fix backend_roles filter dropping top query records received from other nodes due to array type mismatch after transport serialization ([#702](https://github.com/opensearch-project/query-insights/pull/702))
* Reject empty or blank exporter type at settings validation time to prevent cluster-manager wedge ([#700](https://github.com/opensearch-project/query-insights/pull/700))

### Enhancements

* Read historical top queries as the plugin subject instead of borrowing the caller's thread context ([#694](https://github.com/opensearch-project/query-insights/pull/694))

### Documentation

* Document and add integration test coverage for Top Queries API permissions ([#662](https://github.com/opensearch-project/query-insights/pull/662))
