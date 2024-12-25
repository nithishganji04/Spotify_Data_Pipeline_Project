# spotify-data-engineering-project
ETL pipeline using AWS: Extracts data from Spotify API with AWS Lambda, stores raw/processed data in S3, infers schema with Glue Crawler and catalogs it in AWS Glue. Analyze structured data using Athena and automate via CloudWatch triggers.
## Introduction :
In this project, we build an ETL (Extract, Transform, Load) pipeline using the Spotify API on AWS. The pipeline extracts data from the Spotify API, transforms it into the required format, and stores it in an AWS data store for analysis.

![Pipeline](https://datavidhya-static-content.s3.ap-south-1.amazonaws.com/architecture/DataVidhya+Projects+(1)_page-0001.jpg)
## Services Used

## S3 (Simple Storage Service):
### Amazon S3 is an object storage service that offers industry-leading scalability, data availability, security, and performance. S3 Stores and protects any amount of data for a range of use cases, such as data lakes, websites, cloud-native applications, backups, archive, machine learning, and analytics.

## AWS Lambda:
### AWS Lambda is a compute service that runs the code in response to events and automatically manages the compute resources, making it the fastest way to turn an idea into a modern, production, serverless applications

## Amazon CloudWatch:
### Amazon CloudWatch is a monitoring service for AWS resources and applications. It tracks metrics, monitors logs, and enables alarms to ensure efficient system performance.

## AWS Glue Crawler:
### An AWS Glue crawler is an automated tool used within the AWS environment to discover and catalog data. It scans various data sources, extracting schema information and storing metadata in the AWS Glue Data Catalog.

## AWS Glue Data Catalog:
### The Glue Data Catalog is a centralized metadata repository that simplifies data discovery and management. It integrates seamlessly with services like Amazon Athena for data querying.

## Amazon Athena:
### Amazon Athena is an interactive query service that makes it easy to analyze data directly in Amazon Simple Storage Service (Amazon S3) using standard SQL.
