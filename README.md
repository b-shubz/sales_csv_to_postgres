## Read retail sales from S3, transform and load to postgreSQL

This service will read data file in csv format from S3 bucket and do appropriate transformations and data cleaning and load to postgres table.
Records with invalid data will be dumped to different table with column mentioning the reason of invalid data.


## Working
1.File should be uploaded in s3 bucket with fixed name.
  Ex: today_sales.csv <br />
2.This file is downloaded from s3 bucket and stored locally. <br />
3.Pandas is used to read data from today_sales which was downloaded and stored locally. <br />
4.While reading data in pandas dataframe the column name and datatype are specified. <br />
5.Product code in received file is compared with products master data. If product code is not found or invalid then record is separated from final data and will be sent to validations table. <br />
6.Other validations are performed and inalid records are separated from final data, further put into validations table. <br />
7.validations table contains 2 columns  <br />
    - data: whole record dump comma separated <br />
    - validation: reason for invalid record <br />
8.Columns in final data are renamed to standard format and only required columns are selected to insert in postgres db. <br />
9.All invalid records are collected into one dataframe. <br />
10.final data and validations data in loaded to appropriate postgres tables. <br />

## config.py
This file contains all the configuration variables like table name, s3 bucket name etc.

