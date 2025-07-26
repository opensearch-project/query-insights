# Testing Query Insights CSV Export

## Steps to Enable and Test

1. **Start OpenSearch with Query Insights enabled:**
   ```bash
   ./bin/opensearch
   ```

2. **Enable Query Insights features via API:**
   ```bash
   # Enable top queries by latency
   curl -X PUT "localhost:9200/_cluster/settings" -H 'Content-Type: application/json' -d'
   {
     "persistent": {
       "search.insights.top_queries.latency.enabled": true,
       "search.insights.top_queries.cpu.enabled": true,
       "search.insights.top_queries.memory.enabled": true
     }
   }'
   ```

3. **Check if Query Insights is enabled:**
   ```bash
   curl -X GET "localhost:9200/_cluster/settings?include_defaults=true&filter_path=**.search.insights**"
   ```

4. **Run some search queries to generate data:**
   ```bash
   # Create a test index
   curl -X PUT "localhost:9200/test-index" -H 'Content-Type: application/json' -d'
   {
     "mappings": {
       "properties": {
         "title": { "type": "text" },
         "content": { "type": "text" }
       }
     }
   }'

   # Index some test data
   curl -X POST "localhost:9200/test-index/_doc" -H 'Content-Type: application/json' -d'
   {
     "title": "Test Document 1",
     "content": "This is a test document for query insights"
   }'

   # Run search queries
   for i in {1..10}; do
     curl -X GET "localhost:9200/test-index/_search" -H 'Content-Type: application/json' -d'
     {
       "query": {
         "match": {
           "content": "test"
         }
       }
     }'
   done
   ```

5. **Check OpenSearch logs for CSV export messages:**
   ```bash
   tail -f logs/opensearch.log | grep "CSV Export"
   ```

6. **Look for the CSV file:**
   ```bash
   # Check current directory
   ls -la queryOutput.csv
   
   # Check OpenSearch home directory
   ls -la $OPENSEARCH_HOME/queryOutput.csv
   ```

## Troubleshooting

If no CSV is generated:

1. **Check if the listener is enabled:**
   - Look for "CSV Export: Starting CSV export processor" in logs
   - Look for "CSV Export: constructSearchQueryRecord called" in logs

2. **Verify Query Insights is active:**
   ```bash
   curl -X GET "localhost:9200/_insights/top_queries"
   ```

3. **Check thread pool:**
   ```bash
   curl -X GET "localhost:9200/_cat/thread_pool/query_insights_executor?v"
   ```

4. **Enable debug logging:**
   ```bash
   curl -X PUT "localhost:9200/_cluster/settings" -H 'Content-Type: application/json' -d'
   {
     "transient": {
       "logger.org.opensearch.plugin.insights.core.listener.QueryInsightsListener": "DEBUG"
     }
   }'
   ```