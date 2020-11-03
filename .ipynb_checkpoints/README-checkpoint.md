### Sparkify Music Streaming Database & ETL

##### Purpose and goals of the datawarehouse
The datawarehouse will enable Sparkify to analyze the songs and user activity through diverse Data Analysis techniques, with the end goal of understanding user patterns and tailoring the product to meet the market needs.
The data is stored in the data lake in S3, it will be loaded and transformed using spark and dtored in parquet files in a different S3 bucket ready for analysis.

##### Data pipeline

###### Source data
The data is sourced is from two different datasets:

1. A set of JSON files that include general information about a song and its corresponding artist. The files are partitioned by the first three letters of each song's track ID.  They sre stored in S3 (s3://udacity-dend/song_data)
2. A set of JSON files that include activity logs from the Sparkify app. They are stored in S3 (s3://udacity-dend/log_data)

###### Bucket creation

An S3 bucket was created with the end goal of storing parquet files that include the gold datasets (ready to consume) in the datalake.

The following is a screenshot of the bucket:

![s3bucket](/bucket.png)

###### ETL pipeline

The data pipeline include the following steps:

1. Read both datasets from S3 through a pyspark script
2. Transform the data using spark creating ready to consume datasets in a star schema
3. Save the data in the corresponding parquet files within S3

The ETL script is executed through the etl.py file

##### Data Quality results

The following screenshot shows that all files were created appropiately:

![s3bucketcomplete](/bucketcomplete.png)
