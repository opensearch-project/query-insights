## Version 3.8.0 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 3.8.0

### Enhancements

* Derive reader index names from date range instead of cluster state, avoiding cluster state calls ([#614](https://github.com/opensearch-project/query-insights/pull/614))
* Capture user identity (username, roles, backend roles) in query insights records Live Queries ([#645](https://github.com/opensearch-project/query-insights/pull/645))

### Bug Fixes

* Fix testNestedQueryType expectation for recursive nested query traversal ([#628](https://github.com/opensearch-project/query-insights/pull/628))
* Fix flaky grouper integration tests by waiting for settings propagation before searching ([#644](https://github.com/opensearch-project/query-insights/pull/644))

### Infrastructure

* Fix GitHub Actions SHA-pinning policy failures for code-hygiene gradle action and opensearch-build ref ([#621](https://github.com/opensearch-project/query-insights/pull/621))
* Onboard new backport-pr reusable GitHub workflow ([#627](https://github.com/opensearch-project/query-insights/pull/627))
* Onboard code diff analyzer/reviewer and issue dedupe workflows ([#632](https://github.com/opensearch-project/query-insights/pull/632))
* Bump httpclient5 to 5.6.1 to address CVE-2026-40542 ([#633](https://github.com/opensearch-project/query-insights/pull/633))
* Update maven2 mirror repository URL order ([#638](https://github.com/opensearch-project/query-insights/pull/638))
