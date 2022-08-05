# DATA MIGRATION ....

## UNDERSTANDING DATA MIGRATION  :
```
def readDataFromOracleDB(spark_session,dbtable,oracle_config):   
    df = spark_session.read.format("jdbc")\
                        .option("url", oracle_config['url'])\
                        .option("driver", oracle_config['driver'])\
                        .option("dbtable", dbtable)\
                        .option("lowerBound", oracle_config['lowerBound'] )\
                        .option("upperBound", oracle_config['upperBound'])\
                        .option("partitionColumn", "COLUMN_NAME")\
                        .option("numPartitions", oracle_config['numPartitions'])\
                        .option("user", oracle_config['user'])\
                        .option("password", oracle_config['password'])\
                        .option("fetchsize", oracle_config['fetchsize'])\
                        .load()
    return df 
  ```

If you don't specify either {partitionColumn, lowerBound, upperBound, numPartitions} or {predicates} Spark will use a single executor and create a single non-empty partition. All data will be processed using a single transaction and reads will be neither distributed nor parallelized.

Fetch Size :
By default, when Oracle JDBC runs a query, it retrieves a result set of 10 rows at a time from the database cursor. This is the default Oracle row fetch size value. You can change the number of rows retrieved with each trip to the database cursor by changing the row fetch size value.
Standard JDBC also enables you to specify the number of rows fetched with each database round-trip for a query, and this number is referred to as the fetch size. 


Batch SIZE:
from  lower Bound ,Upper bound , num partitions  batch size is formed
Upper – lower bound / num partitions --> batch size   
Ex: upper bound : 1000 lower : 0  num partions:200  
        then batch size : 5
so in query starts executing :    
select * from {db}.{table}   0th  - 5th  
select * from {db}.{table}   5th  - 10th  
select * from {db}.{table}  10th  - 15th  
select * from {db}.{table}  15th  - 20th  
.............
select * from {db}.{table}   990 -995
select * from {db}.{table}   995-1000      

Thaks:)
