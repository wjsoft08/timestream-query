# Timestream Query

This repository contains a Timestream Utility class to query data from Timestream to Object array easily.

# Example Usage
```
region = "us-west-2"
session = boto3.Session()
database = "YOUR_DATABASE"
table = "YOUR_TABLE"
query_client = session.client("timestream-query", config=Config(region_name=region))
query_util = QueryUtil(query_client, database, table)
# Get the latest location of base station
query_string = f"SELECT Id FROM {database}.{table} WHERE time >= ago(24h)"
id_list = query_util.run_query(query_string, True)

```
