## Version 2.19.6 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 2.19.6

### Enhancements

* Update Maven repository mirror URL priority for dependency resolution ([#639](https://github.com/opensearch-project/query-insights/pull/639))

### Infrastructure

* Add CI mirror to plugin and dependency repositories to avoid Maven Central throttling ([#629](https://github.com/opensearch-project/query-insights/pull/629))
* Fix 2.19 CI matrix, security integration tests, and Eclipse formatter configuration ([#565](https://github.com/opensearch-project/query-insights/pull/565))
* Disable validatePluginZipPom to fix preexisting JDK 11 build failure on 2.19 ([#631](https://github.com/opensearch-project/query-insights/pull/631))
* Pin GitHub Actions to commit SHAs for improved supply-chain security ([#626](https://github.com/opensearch-project/query-insights/pull/626))

### Maintenance

* Add release notes for 2.19.6 ([#625](https://github.com/opensearch-project/query-insights/pull/625))
