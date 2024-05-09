This lists custom changes merged in Databricks fork of Vector.
1. Fix premature ack when data file is full. https://github.com/databricks/vector/pull/1
2. Retry s3 request even when observing ConstructionFailure to avoid data loss. https://github.com/databricks/vector/pull/2
3. Updating/adding INFO logs for when vector sends to cloud storage. https://github.com/databricks/vector/pull/5
4. Allow retries on all sink exceptions https://github.com/databricks/vector/pull/7
5. Setting the vector version for DB changes https://github.com/databricks/vector/pull/10
