# sparkify-redshift-etl 
## Project Overview
This is the second project in the 
[Udacity Data Engineer Nanodegree](https://www.udacity.com/course/data-engineer-nanodegree--nd027).

The virtual startup 'Sparkify' provides a music streaming service. In this
 project we imagine that Sparkify has grown and we are shifting to using cloud 
 services for our data warehousing needs. 

Here we create ETL pipeline that extracts JSON log data of user activity & song 
metadata from s3 buckets and stage them on Redshift, transforming the data
into a set of fact and dimension tables to optimize queries that have been
specified by the analytics team. 

We also support Sparkify's IaC scalability goals by automating the ETL pipeline
infrastructure management using the 
[AWS Python SDK (Boto3)](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html).

## How to Run
1. Clone this repository and install the requirements.
```
$ pip3 install -r requirements.txt
```

2. Export your AWS Account credentials as environment variables in your terminal 
window:
```
$ export AWS_ACCESS_KEY_ID=<YOUR_AWS_ACCESS_KEY_ID>
$ export AWS_SECRET_ACCESS_KEY=<YOUR_AWS_ACCESS_KEY_ID>
```

3a. **OPTIONAL:** Run the Redshift setup script to create the necessary IAM 
role, permissions, Redshift cluster if you do not already have those set up.
```
$ python3 setup_redshift.py
```

3b. Replace the `dwh.cfg` configuration file with your AWS details for your 
Redshift Cluster & IAM.

4. Run the ETL scripts to load the data into the Redshift Cluster
```
python3 create_tables.py
python3 etl.py
```

5. **OPTIONAL** Run the teardown script to clean up your AWS resources
```
$ python3 cleanup_redshift.py
```

## DB Schema Design


## ETL Pipeline


## Sample Analytical Queries


#### Project Files
* _etl.py_ - The main script that runs the ETL Pipeline from S3 to Redshift.
* _setup_redshift.py_ - A script that sets up the required AWS resources
 including IAM role, permissions, Redshift cluster and DB.
* _cleanup_redshift.py_ - A script that removes up created AWS resources
 including IAM role, permissions, Redshift cluster and DB.
* _create_tables.py_ - Creates the tables on Redshift DB.
* _sql_queries.py_ - Queries specified by the Sparkify Analytics team.
* _dwh_config.yaml_ - Configuration file defining constants related to AWS
 resources.

## Data Source - The Million Song Dataset
Thierry Bertin-Mahieux, Daniel P.W. Ellis, Brian Whitman, and Paul Lamere.
[The Million Song Dataset](http://millionsongdataset.com/). In Proceedings of 
the 12th International Society for Music Information Retrieval Conference
 (ISMIR 2011), 2011.