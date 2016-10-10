# Flink-examples (for Flink 1.1.2)

Collection of common Flink usage.
At the moment, there is only the following job:

* Csv2RowExample: shows how to generate a Flink __DataSet<Row>__ from a CSV file, using
    * __flink-table-api__ : doesn't hangle properly string fields containing double quoted tokens (see https://issues.apache.org/jira/browse/FLINK-4785)
    * __apache commons-csv__ : reads all fields as string