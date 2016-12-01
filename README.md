# Flink-examples (for Flink 1.1.2)

Collection of common Flink usage and utilities.
At the moment, there are only the following jobs:

* Csv2RowExample: shows how to generate a Flink __DataSet<Row>__ from a CSV file, using
    * __flink-table-api__ : doesn't hangle properly string fields containing double quoted tokens (see https://issues.apache.org/jira/browse/FLINK-4785)
    * __apache commons-csv__ : reads all fields as string
* ElastisearchHelper: shows how to create elasticsearch index templates and index mappings, allowing
	* number of shards configuration: needed in most cases (see https://issues.apache.org/jira/browse/FLINK-4491)
	* number of replicas configuration
	* stop words, filter and mappings configuration