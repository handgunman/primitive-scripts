Use the logs_download_simple.py script to unload Yandex Metrics logs. The data is saved in two files metrika_hits.csv and metrika_visits.csv.

```python3 logs_download_simple.py```

The unloading period and the list of data to be unloaded is set directly in the script.
The data access token is placed in a separate file. See comments in the script.

There is no data availability check in the script.

Use the logs_upload_simple.py script to upload data from files to clickhouse.

```python3 logs_upload_simple.py```


The scripts can be combined with the exception of writing to the file.
