## Version 2.16.0.0 Release Notes
Compatible with OpenSearch 2.16.0

### Maintenance
* Bootstrap query insights plugin repo with maintainers ([#2](https://github.com/opensearch-project/query-insights/pull/2))
* Fix linux ci build failure when upgrade Actions runner to use node 20 ([#15](https://github.com/opensearch-project/query-insights/pull/15))
* Move query categorization changes to plugin ([#16](https://github.com/opensearch-project/query-insights/pull/16))
* Fix build error in NodeRequest class for 2.x ([#18](https://github.com/opensearch-project/query-insights/pull/18))
* Fix query insights zip versioning ([#34](https://github.com/opensearch-project/query-insights/pull/34))
* Fix integration test failures when running with security plugin ([#45](https://github.com/opensearch-project/query-insights/pull/45))

### Bug Fixes
* Validate lower bound for top n size ([#13](https://github.com/opensearch-project/query-insights/pull/13))
* Fix stream serialization issues for complex data structures ([#13](https://github.com/opensearch-project/query-insights/pull/13))

### Enhancements
* Increment latency, cpu and memory histograms for multiple query types ([#30](https://github.com/opensearch-project/query-insights/pull/30))
* Always populate resource usage metrics for categorization ([#41](https://github.com/opensearch-project/query-insights/pull/41))

### Infrastructure
* Configure Mend for query insights repo [#1](https://github.com/opensearch-project/query-insights/pull/1)
* Set up gradle and CI for query insights [#4](https://github.com/opensearch-project/query-insights/pull/4)
* Add build script to query insights plugin  [#14](https://github.com/opensearch-project/query-insights/pull/14)
* Add backport GitHub actions [#17](https://github.com/opensearch-project/query-insights/pull/17)
* Add maven publish workflow [#24](https://github.com/opensearch-project/query-insights/pull/24)
* Add GitHub action for security enabled integration tests [#48](https://github.com/opensearch-project/query-insights/pull/48)
* Add code hygiene checks for query insights  ([#51](https://github.com/opensearch-project/query-insights/pull/51))

### Documentation
* Update Readme file with user guide ([#5](https://github.com/opensearch-project/query-insights/pull/5))
* Added 2.16 release notes ([#52](https://github.com/opensearch-project/query-insights/pull/52))
