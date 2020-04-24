##  Sparkify Data Lake with spark.
***

## Table of contents
   * [Problem](#problem)
   * [Purpose](#Purpose)
   * [Goals Achieved](#Goals-Achieved)
   * [Project Structure](#Project-Structure)
   * [Instructions](#Instructions)
   * [ER Diagram](#ER-Diagram)
   * [Sample Queries](#Sample-Queries)
   
### Problem
Sparkify needs to do analysis on the song and user activity on their music streaming app for unserstanding what songs users are listening to.

### Purpose
Given songs and user activity data that resided in S3, user spark to perfome ETL on the data from 3 and then load the data back to s3 as a set of dimensional  tables optimized for song play **analysis**.


### Goals Achieved 
For the Pupose of this project a and ELT pipeline was created with sparks that loaded  data from and s3 bukes, **log** and **song**  data sets for songs and user activity, which then went through the ELT process to create 4 dimension tables: *users, songs, artists* and *time* linked to 1 facts table *songplays* to form a star schema that will allow for easy analysis.

Analytical queries can now be performed on the facts table songplays to answer questions like "How many" and analytic queries on the dimension tables aswell. 

### Project Structure

- dl.cfg: `Contain AWS Credentials (For the output s3 bucket)`
- etl.py: `Main file that retrives  data from S3 processes the data with spark and write parquet files to S3`
- ReadMe.md: `Documentation and discussions on this project`

### Instructions
In other to successfully run this python script follow the instructions below
1. Input your AWS **Access key id** and AWS **Secret Access key** into dl.cfg file

2. In the main funciton of the etl.py file, assign an S3 bucket e.g  *s3a://au-sparkify-datalake/*  to the output_data variable 
 
 `output_data = s3a://au-sparkify-datalake/`

  Note: the credential provided in the dl.cfg file should have access to the about s3 bucket.
  
3. Open a terminal to the directory containing etl.py file and run the command below

   `python etl.py`


4. If all runs well without an error check your S3 bucket to see the newly created parquet files.

5. Done!! 

### ER Diagram 
ER Diagram for relationship between fact and dimension tables.

![Spotify ER Diagram](er_diagram.png)

### Sample Queries
A sample query that can be performed for song play anylysis.

1. How many songs where being heard for each month for each year

##### Query
`songplays_table = spark.sql(''' SELECT month, year, COUNT(*) as total_views FROM songplays_table GROUP BY GROUPING SETS ((),month, year, (month, year) )    LIMIT 20 ''') `
