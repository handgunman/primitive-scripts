Use the logs_download_simple.py script to unload Yandex Metrics logs. The data is saved in two files metrika_hits.csv and metrika_visits.csv.

```python3 logs_download_simple.py```

The unloading period and the list of data to be unloaded is set directly in the script.
The data access token is placed in a separate file. See comments in the script.

There is no data availability check in the script.

Use the logs_upload_simple.py script to upload data from files to clickhouse.

```python3 logs_upload_simple.py```

The connection parameters and data loading mode are specified directly in the script.

If necessary, specify the path to the certificate file in the SSL_VERIFY variable.

Loading mode 1 leads to deletion and creation of tables under the data. Mode 0 adds data to existing tables. When loading for the first time it is recommended to use mode 1 for automatic creation of tables.

The script does not check the availability of the database, the availability of the necessary tables and data for the same period.

Scripts can be merged, together with the exclusion of writing to a file



You can use the logs_upload_psql_simple.py script to upload to PostgreSQL instead of the ClickHouse script
