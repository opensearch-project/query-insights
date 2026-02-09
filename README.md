# OpenSearch Query Insights

## Introduction
OpenSearch stands as a versatile, scalable, open-source solution designed for diverse data exploration needs, ranging from interactive log analytics to real-time application monitoring. Despite its capabilities, OpenSearch users and administrators often encounter challenges in ensuring optimal search performance due to limited expertise or OpenSearch's current constraints in providing comprehensive data points on query executions. Common questions include:

* “What are the top queries with the highest latency/CPU usages in the last 1 hour” (Identification of top queries by certain resource usages within a specific timeframe).
* “How do I associate queries to users” (Profiling users with the highest search query volumes).
* “Why my search queries are so slow” (Concerns about slow search queries).
* “Why there was a spike in my search latency chart” (Spikes in query latency).

The overarching objective of the Query Insights initiative is to address these issues by building frameworks, APIs, and dashboards, with minimal performance impact, to offer profound insights, metrics and recommendations into query executions, empowering users to better understand search query characteristics, patterns, and system behavior during query execution stages. Query Insights will facilitate enhanced detection, diagnosis, and prevention of query performance issues, ultimately improving query processing performance, user experience, and overall system resilience.

Query Insights and this plugin project was originally proposed in the [OpenSearch Query Insights RFC](https://github.com/opensearch-project/OpenSearch/issues/11429).

## Get Started
### Installing the Plugin

Starting with version 2.16, the Query Insights plugin is bundled with the OpenSearch distribution and available by default.

For earlier versions, you can install the plugin manually using the following command:

```
bin/opensearch-plugin install query-insights
```
For information about installing plugins, see [Installing plugins](https://opensearch.org/docs/latest/install-and-configure/plugins/).

### Enabling top N query monitoring

When you install the `query-insights` plugin, top N query monitoring is enabled by default. To disable top N query monitoring, update the dynamic cluster settings for the desired metric types. For example, to disable monitoring top N queries by latency, update the `search.insights.top_queries.latency.enabled` setting:

```
PUT _cluster/settings
{
  "persistent" : {
    "search.insights.top_queries.latency.enabled" : false
  }
}
```
### Monitoring the top N queries

You can use the Insights API endpoint to obtain top N queries:

```
GET /_insights/top_queries
```

### Export top N query data

You can configure your desired exporter to export top N query data to different sinks, allowing for better monitoring and analysis of your OpenSearch queries.

A local index exporter allows you to export the top N queries to local OpenSearch indexes. To configure the local index exporter for the top N queries by latency, send the following request:

```
PUT _cluster/settings
{
  "persistent" : {
    "search.insights.top_queries.exporter.type" : "local_index"
  }
}
```

A remote repository exporter allows you to export the top N queries to blob store repositories supported by OpenSearch. Note: The remote repository exporter requires async multi-stream blob upload support, currently available only in the repository-s3 plugin. You must first configure a blob store repository with OpenSearch to use the remote repository exporter. Then, to export top N queries to remote repository exporter, send the following request:

```
PUT _cluster/settings
{
  "persistent" : {
    "search.insights.top_queries.exporter.remote.repository" : "my-s3-repository",
    "search.insights.top_queries.exporter.remote.path" : "query-insights",
    "search.insights.top_queries.exporter.remote.enabled" : true
  }
}
```

You can refer to the [official document](https://opensearch.org/docs/latest/observing-your-data/query-insights/index/) for more detailed usage of query-insights plugin.

## Development
If you find bugs or want to request a feature, please create [a new issue](https://github.com/opensearch-project/query-insights/issues/new/choose). For questions or to discuss how Query Insights works, please find us in the [OpenSearch Slack](https://opensearch.org/slack.html) in the `#plugins` channel.

### Building and Testing

The plugin can be built using Gradle:

```
./gradlew build
```

To test and debug, run the plugin with OpenSearch in debug mode:

```
./gradlew run --debug-jvm
```

## Project Style Guidelines

The [OpenSearch Project style guidelines](https://github.com/opensearch-project/documentation-website/blob/main/STYLE_GUIDE.md) and [OpenSearch terms](https://github.com/opensearch-project/documentation-website/blob/main/TERMS.md) documents provide style standards and terminology to be observed when creating OpenSearch Project content.

## Getting Help

* For questions or help getting started, please find us in the [OpenSearch Slack](https://opensearch.org/slack.html) in the `#plugins` channel.
* For bugs or feature requests, please create [a new issue](https://github.com/opensearch-project/query-insights/issues/new/choose).

## Code of Conduct

This project has adopted the [Amazon Open Source Code of Conduct](CODE_OF_CONDUCT.md). For more information see the [Code of Conduct FAQ](https://aws.github.io/code-of-conduct-faq), or contact [opensource-codeofconduct@amazon.com](mailto:opensource-codeofconduct@amazon.com) with any additional questions or comments.

## License

This project is licensed under the [Apache v2.0 License](LICENSE.txt).

## Copyright

Copyright OpenSearch Contributors. See [NOTICE](NOTICE.txt) for details.
