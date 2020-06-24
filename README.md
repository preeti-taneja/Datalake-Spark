# Datalake-Spark
  
  Developed a data lake for the analytics team at music streaminng company.After considerable growth in user base and song database it was time to 
  move the data warehouse to a data lake and enhance data processing through Spark.
  
  Built an ETL pipeline, extracted data from S3 buckets, processed it through Spark and transformed into a star schema stored in S3 buckets 
  with parquet formatting and efficient partitioning. The database and ETL pipeline were validated by running queries provided by the analytics team 
  and compared expected results. Skills included:

    - Building out an ETL pipeline using Spark, Python, Hadoop Clusters (EMR).
    - Fast-tracking the data lake buildout using (serverless) AWS Lambda and cataloging tables with an AWS Glue Crawler
    - Setting up IAM Roles, Hadoop Clusters, EMR, Config files and security groups.
    - Scaling up the data analysis process through the use of a data lake and Spark, in order to further optimize queries on song play analysis
    
    The primary file in this repo is the etl.py, which will read in files from S3 buckets,
    process them using Spark and store them as parquet files in S3 buckets, partitioned appropriately.
