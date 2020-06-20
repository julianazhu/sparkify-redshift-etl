# sparkify-redshift-etl 
## Project Overview
This is the second project in the 
[Udacity Data Engineer Nanodegree](https://www.udacity.com/course/data-engineer-nanodegree--nd027).

The virtual startup 'Sparkify' provides a music streaming service. In this project we imagine that Sparkify has grown
and we are shifting to using cloud services for our data warehousing needs. 

Here we create ETL pipeline that extracts JSON log data of user activity & song metadata from s3 buckets and stages 
them on Redshift, transforming the data into a set of fact and dimension tables to optimize queries that have been 
specified by the analytics team. We support Sparkify's scalability goals by automating the ETL pipeline infrastructure 
management using the [AWS Python SDK (Boto3)](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html).

## How to Run
Clone this repository and replace the `dwh.cfg` configuration file with details of your own AWS account. 

```
pip3 install -r requirements.txt
python3 create_tables.py
python3 etl.py
```

#### DB Schema Design

#### ETL Pipeline

#### Project Files
* **etl.py** - The main script that runs the ETL Pipeline from S3 to Redshift.
* **create_tables.py** - Creates the tables on Redshift.
* **sql_queries.py** - Queries specified by the Sparkify Analytics team.
* **dwh.cfg** - Configuration file defining constants related to AWS resources.

## Data Source - The Million Song Dataset
Thierry Bertin-Mahieux, Daniel P.W. Ellis, Brian Whitman, and Paul Lamere.
[The Million Song Dataset](http://millionsongdataset.com/). In Proceedings of the 12th International Society
for Music Information Retrieval Conference (ISMIR 2011), 2011.