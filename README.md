# Linux Kernel Git Revisions Project

## Tian Wong
## CSC 369, Paul Anderson

### Overview:
A data analysis of commits to the linux kernel

Original Dataset:
    https://www.kaggle.com/philschmidt/linux-kernel-git-revision-history
Steps to produce updated version:
    https://github.com/tdhd/kaggle-linux-git

process.py:
    Get information on Linux kernel

analysis.py
    Run analysis on data, exporting figures where relevant

--------------------------

Kaggle dataset reached from around 2005 to 2017
User documented their procedure for proccessing the data from the main branch of the linux git repo
Used their guide and scripts to generate an updated dataset
Had to alter their scripts a bit for use with python3
Some iso-8859-1 encoded characters in the git-log output

Filtered out entries without a timestamp or author_id 
2250827 entries left

1st major release with git:
    https://marc.info/?l=git-commits-head&m=111904216911731&w=2
Found out that TimestampType in pyspark is not tz aware like in Pandas
- Adding seperate column of local commit times

Looking beyond 2.6.12 release at timestamp 1119037709, which was when git was introduced into the project
- any logs before this as suspect, as git was integrated/released with this release
- eg: usb 3.0 hub changes in 2001 time frame?

Limitation on 'author_id'
- Linus is ID #31768, #30328, #6, etc
- Unique pairing of name and email
- Looking at original commit data, it is clear that Linus is using multiple emails

Tried using pandas on spark with pyspark 3.2
- to_pandas_on_spark for plotly
- switched to pandas for matplotlib for better plotting support

Utilities used:
- pyspark sql
- pyarrow
- pandas
- matplotlib


### Resources:
https://databricks.com/blog/2021/10/04/pandas-api-on-upcoming-apache-spark-3-2.html
https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.plot.html#pandas.DataFrame.plot