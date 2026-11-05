## Version 3.10.0 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 3.10.0

### Features

* Show running PPL queries as single entries in Live Queries ([#699](https://github.com/opensearch-project/query-insights/pull/699))

### Enhancements

* Read historical top queries as the plugin subject instead of borrowing the caller's context ([#694](https://github.com/opensearch-project/query-insights/pull/694))

### Bug Fixes

* Reject empty or blank exporter type at settings validation time to prevent cluster-state wedge ([#700](https://github.com/opensearch-project/query-insights/pull/700))

### Documentation

* Document and test Top Queries API authorization options ([#662](https://github.com/opensearch-project/query-insights/pull/662))
