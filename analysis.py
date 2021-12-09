from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from datetime import datetime, timedelta
import pyspark.sql.functions as sf
import time
import pandas as p
# import pyspark.pandas as pd

# pd.options.plotting.backend = 'matplotlib'

spark = SparkSession.builder.getOrCreate()

# define schema for reading in csv file
# casting to appropriate types when needed
schema = StructType([ \
    StructField("author_timestamp", IntegerType()), \
    StructField("commit_utc_offset_hours", IntegerType()), \
    StructField("commit_hash", StringType()), \
    StructField("subject", StringType()), \
    StructField("filename", StringType()), \
    StructField("n_additions", IntegerType()), \
    StructField("n_deletions", IntegerType()), \
    StructField("author_id", IntegerType())
  ])

# initial input
orig = spark.read.schema(schema)\
        .option("header", True)\
        .csv('/Users/tianwong/Dev/369/Linux_Kernel_Git_Revisions/linux_kernel_git_revlog.csv')

# clean up data
df = orig.filter('author_timestamp IS NOT NULL')\
        .filter('author_id IS NOT NULL')\
        .filter(orig['author_timestamp'] < int(time.time()))\
        .filter(orig['author_timestamp'] >= 1119037709)

# get date and time, adjusting for utc offset
def get_local_DT(timestamp, utc_offset_hours) -> datetime:
  offset = timedelta(hours=utc_offset_hours)
  local_dt = datetime.utcfromtimestamp(timestamp) + offset
  return local_dt

# extract hour from datetime
def get_local_hour(datetime) -> int:
  return datetime.hour

udf_get_local_DT = sf.udf(get_local_DT, TimestampType())
udf_get_local_hour = sf.udf(get_local_hour, IntegerType())

df = df.withColumn('local_datetime',\
    udf_get_local_DT('author_timestamp', 'commit_utc_offset_hours'),\
  )

df = df.withColumn('local_hour',\
    udf_get_local_hour('local_datetime'))

df = df.withColumn('n_changes',\
    sf.col('n_additions') + sf.col('n_deletions')\
  )

# df.printSchema()

# ---------

# first release under git
# df.count()
    # 2243768 commits following Linux 2.6.12

# major releases
# orig.filter(orig.filename == "Makefile")\
#   .filter(orig['subject'].rlike("Linux"))\
#   .orderBy('author_timestamp').show()

# file and line change numbers per commit
commit_metrics = df.groupBy('author_id', 'commit_hash', 'subject', 'author_timestamp', 'local_hour')\
  .agg(\
    sf.count(sf.lit(1)).alias('files_altered'),\
    sf.sum('n_changes').alias('line_changes')\
  )
author_activity = commit_metrics.groupBy('author_id')\
  .agg(\
    sf.count(sf.lit(1)).alias('total_commits'),\
    sf.sum('files_altered').alias('total_files_altered'),\
    sf.sum('line_changes').alias('total_line_changes')\
  )

f0 = commit_metrics.select('author_timestamp')\
  .toPandas().plot.hist(bins=20, figsize=(7,5), title='Commit Times')
f0.set_xlabel('Time')
f0.set_ylabel('Commits')
f0.figure.savefig("images/f0.png")

f1 = commit_metrics.select('files_altered')\
  .filter(commit_metrics['files_altered'] <= 15)\
  .toPandas().plot.hist(bins=15, figsize=(7,5), title='File Count Distribution Across Commits')
  # .to_pandas_on_spark().plot.hist(bins=15)
f1.set_xlabel('Files Edited')
f1.set_ylabel('Commit Count')
f1.figure.savefig("images/f1.png")
# f1.write_image("images/f1.png", format='png', scale=4, engine="kaleido")



# most frequently edited files, by frequency and line numbers
file_metrics = df.groupBy('filename')\
  .agg(\
    sf.count('filename').alias('commit_frequency'),\
    sf.sum('n_changes').alias('changes_over_lifetime')\
  )

f2 = file_metrics.select('commit_frequency')\
  .filter(file_metrics['commit_frequency'] <= 200)\
  .toPandas().plot.hist(bins=20, figsize=(7,5), title='File Revisions')
f2.set_xlabel('Revisions')
f2.set_ylabel('File Count')
f2.figure.savefig("images/f2.png")



# number of commits versus hour of local time
hourly_metrics = commit_metrics.groupBy('local_hour')\
  .agg(\
    sf.count(sf.lit(1)).alias('count')\
  ).orderBy('local_hour')

f3 = hourly_metrics.select('count')\
  .toPandas().plot.bar(figsize=(7,5), title='Hourly commits')
f3.set_xlabel('Hours')
f3.figure.savefig("images/f3.png")