# Sample SCD2 with Apache Iceberg

this project require hive running on local machine,you can start hive with docker image 
in hiveDocker directory.

Sample SCD2

# Solution
1. Gen rowMd5 to detect changes
2. Gen rowKey with primary key columns 
3. do SCD2 and Gen Open Close and reject table


# Dependencies
1. Spark (version 3.2)
2. Iceberg-spark-runtime-3.2_2.12 ( version 1.1.0)

# Code walk through 

