# Part 1: Job Development

* [A Brief Introduction to Spark](https://github.com/pdefusco/CDE_Banking_HOL_MKT/blob/main/step_by_step_guides/english/part_01_spark.md#a-brief-introduction-to-spark)
* [Lab 1: Run PySpark Interactive Session](https://github.com/pdefusco/CDE_Banking_HOL_MKT/blob/main/step_by_step_guides/english/part_01_spark.md#lab-1-run-pyspark-interactive-session)
* [Lab 2: Create CDE Resources and Run CDE Spark Job](https://github.com/pdefusco/CDE_Banking_HOL_MKT/blob/main/step_by_step_guides/english/part_01_spark.md#lab-2-create-cde-resources-and-run-cde-spark-job)
* [Summary](https://github.com/pdefusco/CDE_Banking_HOL_MKT/blob/main/step_by_step_guides/english/part_01_spark.md#summary)
* [Useful Links and Resources](https://github.com/pdefusco/CDE_Banking_HOL_MKT/blob/main/step_by_step_guides/english/part_01_spark.md#useful-links-and-resources)

### A Brief Introduction to Spark

Apache Spark is an open-source, distributed processing system used for big data workloads. It has gained extreme popularity as the go-to engine for interactive Data Analysis and the deployment of Production Data Engineering and Machine Learning pipelines at scale.

In CDE you can use Spark to explore data interactively via CDE Sessions or deploy batch data engineering pipelines via CDE Jobs.


### Lab 1: Run PySpark Interactive Session

Navigate to the CDE Home Page and launch a PySpark Session. Leave default settings intact.

![alt text](../../img/part1-cdesession-1.png)

Once the Session is ready, open the "Interact" tab in order to enter your code.

![alt text](../../img/part1-cdesession-2.png)

You can copy and paste code from the instructions into the notebook by clicking on the icon at the top right of the code cell.

![alt text](../../img/part1-cdesession-3.png)

Copy the following cell into the notebook. Before running it, ensure that you have edited the "username" variable with your assigned user.

```
from os.path import exists
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *

storageLocation = "s3a://cde-innovation-buk-9e384927/data"
username = "user002"
```

![alt text](../../img/part1-cdesession-4.png)

No more code edits are required. Continue running each code snippet below in separate cells in the notebook.

```
### LOAD HISTORICAL TRANSACTIONS FILE FROM CLOUD STORAGE
transactionsDf = spark.read.json("{0}/mkthol/trans/{1}/rawtransactions".format(storageLocation, username))
transactionsDf.printSchema()
```

```
### CREATE PYTHON FUNCTION TO FLATTEN PYSPARK DATAFRAME NESTED STRUCTS
def flatten_struct(schema, prefix=""):
    result = []
    for elem in schema:
        if isinstance(elem.dataType, StructType):
            result += flatten_struct(elem.dataType, prefix + elem.name + ".")
        else:
            result.append(F.col(prefix + elem.name).alias(prefix + elem.name))
    return result
```

```
### RUN PYTHON FUNCTION TO FLATTEN NESTED STRUCTS AND VALIDATE NEW SCHEMA
transactionsDf = transactionsDf.select(flatten_struct(transactionsDf.schema))
transactionsDf.printSchema()
```

```
### RENAME COLUMNS
transactionsDf = transactionsDf.withColumnRenamed("transaction.transaction_amount", "transaction_amount")
transactionsDf = transactionsDf.withColumnRenamed("transaction.transaction_currency", "transaction_currency")
transactionsDf = transactionsDf.withColumnRenamed("transaction.transaction_type", "transaction_type")
transactionsDf = transactionsDf.withColumnRenamed("transaction_geolocation.latitude", "latitude")
transactionsDf = transactionsDf.withColumnRenamed("transaction_geolocation.longitude", "longitude")
```

```
### CAST COLUMN TYPES FROM STRING TO APPROPRIATE TYPE
transactionsDf = transactionsDf.withColumn("transaction_amount",  transactionsDf["transaction_amount"].cast('float'))
transactionsDf = transactionsDf.withColumn("latitude",  transactionsDf["latitude"].cast('float'))
transactionsDf = transactionsDf.withColumn("longitude",  transactionsDf["longitude"].cast('float'))
transactionsDf = transactionsDf.withColumn("event_ts", transactionsDf["event_ts"].cast("timestamp"))
```

```
### CALCULATE MEAN AND MEDIAN CREDIT CARD TRANSACTION AMOUNT
transactionsAmountMean = round(transactionsDf.select(F.mean("transaction_amount")).collect()[0][0],2)
transactionsAmountMedian = round(transactionsDf.stat.approxQuantile("transaction_amount", [0.5], 0.001)[0],2)

print("Transaction Amount Mean: ", transactionsAmountMean)
print("Transaction Amount Median: ", transactionsAmountMedian)
```

```
### CREATE SPARK TEMPORARY VIEW FROM DATAFRAME
transactionsDf.createOrReplaceTempView("trx")
spark.sql("SELECT * FROM trx LIMIT 10").show()
```

```
### CALCULATE AVERAGE TRANSACTION AMOUNT BY MONTH
spark.sql("SELECT MONTH(event_ts) AS month, \
          avg(transaction_amount) FROM trx GROUP BY month ORDER BY month").show()
```

```
### CALCULATE AVERAGE TRANSACTION AMOUNT BY DAY OF WEEK
spark.sql("SELECT DAYOFWEEK(event_ts) AS DAYOFWEEK, \
          avg(transaction_amount) FROM trx GROUP BY DAYOFWEEK ORDER BY DAYOFWEEK").show()
```

```
### CALCULATE NUMBER OF TRANSACTIONS BY CREDIT CARD
spark.sql("SELECT CREDIT_CARD_NUMBER, COUNT(*) AS COUNT FROM trx \
            GROUP BY CREDIT_CARD_NUMBER ORDER BY COUNT DESC LIMIT 10").show()
```

```
### LOAD CUSTOMER PII DATA FROM CLOUD STORAGE
piiDf = spark.read.options(header='True', delimiter=',').csv("{0}/mkthol/pii/{1}/pii".format(storageLocation, username))
piiDf.show()
piiDf.printSchema()
```

```
### CAST LAT LON TO FLOAT TYPE AND CREATE TEMPORARY VIEW
piiDf = piiDf.withColumn("address_latitude",  piiDf["address_latitude"].cast('float'))
piiDf = piiDf.withColumn("address_longitude",  piiDf["address_longitude"].cast('float'))
piiDf.createOrReplaceTempView("cust_info")
```

```
### SELECT TOP 100 CUSTOMERS WITH MULTIPLE CREDIT CARDS SORTED BY NUMBER OF CREDIT CARDS FROM HIGHEST TO LOWEST
spark.sql("SELECT name AS name, \
          COUNT(credit_card_number) AS CC_COUNT FROM cust_info GROUP BY name ORDER BY CC_COUNT DESC \
          LIMIT 100").show()
```

```
### SELECT TOP 100 CREDIT CARDS WITH MULTIPLE NAMES SORTED FROM HIGHEST TO LOWEST
spark.sql("SELECT COUNT(name) AS NM_COUNT, \
          credit_card_number AS CC_NUM FROM cust_info GROUP BY CC_NUM ORDER BY NM_COUNT DESC \
          LIMIT 100").show()
```

```
# SELECT TOP 25 CUSTOMERS WITH MULTIPLE ADDRESSES SORTED FROM HIGHEST TO LOWEST
spark.sql("SELECT name AS name, \
          COUNT(address) AS ADD_COUNT FROM cust_info GROUP BY name ORDER BY ADD_COUNT DESC \
          LIMIT 25").show()
```

```
### JOIN DATASETS AND COMPARE CREDIT CARD OWNER COORDINATES WITH TRANSACTION COORDINATES
joinDf = spark.sql("""SELECT i.name, i.address_longitude, i.address_latitude, i.bank_country,
          r.credit_card_provider, r.event_ts, r.transaction_amount, r.longitude, r.latitude
          FROM cust_info i INNER JOIN trx r
          ON i.credit_card_number == r.credit_card_number;""")
joinDf.show()
```

```
### CREATE PYSPARK UDF TO CALCULATE DISTANCE BETWEEN TRANSACTION AND HOME LOCATIONS
distanceFunc = F.udf(lambda arr: (((arr[2]-arr[0])**2)+((arr[3]-arr[1])**2)**(1/2)), FloatType())
distanceDf = joinDf.withColumn("trx_dist_from_home", distanceFunc(F.array("latitude", "longitude",
                                                                            "address_latitude", "address_longitude")))
```

```
### SELECT CUSTOMERS WHOSE TRANSACTION OCCURRED MORE THAN 100 MILES FROM HOME
distanceDf.filter(distanceDf.trx_dist_from_home > 100).show()
```


### Lab 2: Using Icberg with PySpark

#### Iceberg Merge Into

Create Transactions Iceberg table:

```
transactionsDf.writeTo("spark_catalog.HOL_DB_{}.TRANSACTIONS_{}".format(username)).createOrReplace()
```

Load New Batch of Transactions in Temp View:

```
trxBatchDf = spark.read.json("{0}/mkthol/trans/{1}/trx_batch_1".format(storageLocation, username))
trxBatchDf.createOrReplaceTempView("trx_batch")
```

Sample Merge Into Syntax:

```
MERGE INTO prod.db.target t   -- a target table
USING (SELECT ...) s          -- the source updates
ON t.id = s.id                -- condition to find updates for target rows
WHEN MATCHED AND s.op = 'delete' THEN DELETE -- updates
WHEN MATCHED AND t.count IS NULL AND s.op = 'increment' THEN UPDATE SET t.count = 0
WHEN MATCHED AND s.op = 'increment' THEN UPDATE SET t.count = t.count + 1
WHEN NOT MATCHED THEN INSERT *
```

Run MERGE INTO in order to load new batch into Transactions table:

Spark SQL Command:

```
# PRE-MERGE COUNT:
print(transactionsDf.count())

# MERGE OPERATION
spark.sql("""MERGE INTO spark_catalog.HOL_DB_{}.TRANSACTIONS_{} t   
USING (SELECT * FROM trx_batch) s          
ON t.credit_card_number = s.credit_card_number               
WHEN MATCHED AND t.transaction_amount < 100 AND t.transaction_currency != "CHF" THEN UPDATE SET t.transaction_type = "PURCHASE"
WHEN NOT MATCHED THEN INSERT *""".format(username))

# POST-MERGE COUNT:
print(transactionsDf.count())
```

#### Iceberg Time Travel / Incremental Read

Now that we have appended

```
# ICEBERG TABLE HISTORY (SHOWS EACH SNAPSHOT AND TIMESTAMP)
spark.sql("SELECT * FROM spark_catalog.HOL_DB_{}.TRANSACTIONS_{}.history".format(username)).show()

# ICEBERG TABLE SNAPSHOTS (USEFUL FOR INCREMENTAL QUERIES AND TIME TRAVEL)
spark.sql("SELECT * FROM spark_catalog.HOL_DB_{}.TRANSACTIONS_{}.snapshots".format(username)).show()

# STORE FIRST AND LAST SNAPSHOT ID'S FROM SNAPSHOTS TABLE
snapshots_df = spark.sql("SELECT * FROM spark_catalog.HOL_DB_{}.TRANSACTIONS_{}.snapshots;".format(username))
```

```
last_snapshot = snapshots_df.select("snapshot_id").tail(1)[0][0]
second_snapshot = snapshots_df.select("snapshot_id").collect()[1][0]
#first_snapshot = snapshots_df.select("snapshot_id").head(1)[0][0]

incReadDf = spark.read\
    .format("iceberg")\
    .option("start-snapshot-id", second_snapshot)\
    .option("end-snapshot-id", last_snapshot)\
    .load("spark_catalog.HOL_DB_{}.TRANSACTIONS_{}".format(username))

print("Incremental DF Schema:")
incReadDf.printSchema()

print("Incremental Report:")
incReadDf.show()
```










#### Create Iceberg Table from PySpark DF

```
transactionsDf.writeTo("spark_catalog.TRX_DB_{}.TRANSACTIONS_{}".format(username)).create()
```

#### Working with Iceberg Table Branches

##### Upsert Data into Branch with Iceberg Merge Into

```
# LOAD NEW TRANSACTION BATCH
#batchDf = spark.read.csv("/app/mount/cell_towers_2.csv", header=True, inferSchema=True)
batchDf = spark.read.json("{0}/mkthol/trans/{1}/trx_batch_2".format(storageLocation, username))
batchDf.printSchema()

# CREATE TABLE BRANCH
spark.sql("ALTER TABLE spark_catalog.TRX_DB_{}.TRANSACTIONS_{} CREATE BRANCH ingestion_branch".format(USERNAME))

# WRITE DATA OPERATION ON TABLE BRANCH
batchDf.write.format("iceberg").option("branch", "ingestion_branch").mode("append").save("spark_catalog.TRX_DB_{}.TRANSACTIONS_{}".format(USERNAME))
```

Notice that a simple SELECT query against the table still returns the original data.

```
spark.sql("SELECT * FROM spark_catalog.TRX_DB_{}.TRANSACTIONS_{};".format(USERNAME)).show()
```

If you want to access the data in the branch, you can specify the branch name in your SELECT query.

```
spark.sql("SELECT * FROM spark_catalog.TRX_DB_{}.TRANSACTIONS_{} VERSION AS OF 'ingestion_branch';".format(USERNAME)).show()
```

Track table snapshots post Merge Into operation:

```
# QUERY ICEBERG METADATA HISTORY TABLE
spark.sql("SELECT * FROM spark_catalog.TRX_DB_{}.TRANSACTIONS_{}.snapshots".format(USERNAME)).show(20, False)
```

### Cherrypicking Snapshots

The cherrypick_snapshot procedure creates a new snapshot incorporating the changes from another snapshot in a metadata-only operation (no new datafiles are created). To run the cherrypick_snapshot procedure you need to provide two parameters: the name of the table you’re updating as well as the ID of the snapshot the table should be updated based on. This transaction will return the snapshot IDs before and after the cherry-pick operation as source_snapshot_id and current_snapshot_id.

we will use the cherrypick operation to commit the changes to the table which were staged in the 'ingestion_branch' branch up until now.

```
# SHOW PAST BRANCH SNAPSHOT ID'S
spark.sql("SELECT * FROM spark_catalog.TRX_DB_{}.TRANSACTIONS_{}.refs;".format(USERNAME)).show()

# SAVE THE SNAPSHOT ID CORRESPONDING TO THE CREATED BRANCH
branchSnapshotId = spark.sql("SELECT snapshot_id FROM spark_catalog.TRX_DB_{}.TRANSACTIONS_{}.refs WHERE NAME == 'ingestion_branch';".format(USERNAME)).collect()[0][0]

# USE THE PROCEDURE TO CHERRY-PICK THE SNAPSHOT
# THIS IMPLICITLY SETS THE CURRENT TABLE STATE TO THE STATE DEFINED BY THE CHOSEN PRIOR SNAPSHOT ID
spark.sql("CALL spark_catalog.system.cherrypick_snapshot('spark_catalog.TRX_DB_{}.TRANSACTIONS_{}',{1})".format(USERNAME, branchSnapshotId))

# VALIDATE THE CHANGES
# THE TABLE ROW COUNT IN THE CURRENT TABLE STATE REFLECTS THE APPEND OPERATION - IT PREVIOSULY ONLY DID BY SELECTING THE BRANCH
spark.sql("SELECT COUNT(*) FROM spark_catalog.TRX_DB_{}.TRANSACTIONS_{};".format(USERNAME)).show()
```

### Working with Iceberg Table Tags

##### Create Table Tag

Tags are immutable labels for Iceberg Snapshot ID's and can be used to reference a particular version of the table via a simple tag rather than having to work with Snapshot ID's directly.   

```
spark.sql("ALTER TABLE spark_catalog.TRX_DB_{}.TRANSACTIONS_{} CREATE TAG businessOrg RETAIN 365 DAYS".format(USERNAME)).show()
```

Select your table snapshot as of a particular tag:

```
spark.sql("SELECT * FROM spark_catalog.TRX_DB_{}.TRANSACTIONS_{} VERSION AS OF 'businessOrg';".format(USERNAME)).show()
```









### Lab 2: Create CDE Resources and Run CDE Spark Job

Up until now you used Sessions to interactively explore data. CDE also allows you to run Spark Application code in batch with as a CDE Job. There are two types of CDE Jobs: Spark and Airflow. In this lab we will create a CDE Spark Job and revisit Airflow later in part 3.

The CDE Spark Job is an abstraction over the Spark Submit. With the CDE Spark Job you can create a reusable, modular Spark Submit definition that is saved in CDE and can be modified in the CDE UI (or via the CDE CLI and API) before every run according to your needs. CDE stores the job definition for each run in the Job Runs UI so you can go back and refer to it long after your job has completed.

Furthermore, CDE allows you to directly store artifacts such as Python files, Jars and other dependencies, or create Python environments and Docker containers in CDE as "CDE Resources". Once created in CDE, Resources are available to CDE Jobs as modular components of the CDE Job definition which can be swapped and referenced by a particular job run as needed.

These features dramatically reduce the amount of work and effort normally required to manage and monitor Spark Jobs in a Spark Cluster. By providing a unified view over all your runs along with the associated artifacts and dependencies, CDE streamlines CI/CD pipelines and removes the need for glue code in your Spark cluster.

In the next steps we will see these benefits in actions.

##### Create CDE Python Resource

Navigate to the Resources tab and create a Python Resource. Make sure to select the Virtual Cluster assigned to you if you are creating a Resource from the CDE Home Page, and to name the Python Resource after your username e.g. "fraud-prevention-py-user100" if you are "user100".

Upload the "requirements.txt" file located in the "cde_spark_jobs" folder. This can take up to a few minutes.

Please familiarize yourself with the contents of the "requirements.txt" file and notice that it contains a few Python libraries such as Pandas and PyArrow.

Then, move on to the next section even while the environment build is still in progress.

![alt text](../../img/part1-cdepythonresource-1.png)

![alt text](../../img/part1-cdepythonresource-2.png)

##### Create CDE Files Resource

From the Resources page create a CDE Files Resource. Upload all files contained in the "cde_spark_jobs" folder. Again, ensure the Resource is named after your unique workshop username and it is created in the Virtual Cluster assigned to you.

![alt text](../../img/part1-cdefilesresource-1.png)

![alt text](../../img/part1-cdefilesresource-2.png)

Before moving on to the next step, please familiarize yourself with the code in the "01_fraud_report.py", "utils.py", and "parameters.conf" files.

Notice that "01_fraud_report.py" contains the same PySpark Application code you ran in the CDE Session, with the exception that the column casting and renaming steps have been refactored into Python functions in the "utils.py" script.

Finally, notice the contents of "parameters.conf". Storing variables in a file in a Files Resource is one method used by CDE Data Engineers to dynamically parameterize scripts with external values.

##### Create CDE Spark Job

Now that the CDE Resources have been created you are ready to create your first CDE Spark Job.

Navigate to the CDE Jobs tab and click on "Create Job". The long form loaded to the page allows you to build a Spark Submit as a CDE Spark Job, step by step.

![alt text](../../img/part1-cdesparkjob-1.png)

Enter the following values without quotes into the corresponding fields. Make sure to update the username with your assigned user wherever needed:

* Job Type: Spark
* Name: 01_fraud_report_userxxx
* File: Select from Resource -> "01_fraud_report.py"
* Arguments: userxxx
* Configurations:
  - key: spark.sql.autoBroadcastJoinThreshold
  - value: 11M

The form should now look similar to this:

![alt text](../../img/part1-cdesparkjob-2.png)

Finally, open the "Advanced Options" section.

Notice that your CDE Files Resource has already been mapped to the CDE Job for you.

Then, update the Compute Options by increasing "Executor Cores" and "Executor Memory" from 1 to 2.

![alt text](../../img/part1-cdesparkjob-3.png)

Finally, run the CDE Job by clicking the "Create and Run" icon.

##### CDE Job Run Observability

Navigate to the Job Runs page in your Virtual Cluster and notice a new Job Run is being logged automatically for you.

![alt text](../../img/part1-cdesparkjob-4.png)

Once the run completes, open the run details by clicking on the Run ID integer in the Job Runs page.

![alt text](../../img/part1-cdesparkjob-5.png)

Open the logs tab and validate output from the Job Run in the Driver -> Stdout tab.

![alt text](../../img/part1-cdesparkjob-6.png)

### Summary

Open data lakehouse on CDP simplifies advanced analytics on all data with a unified platform for structured and unstructured data and integrated data services to enable any analytics use case from ML, BI to stream analytics and real-time analytics. Apache Iceberg is the secret sauce of the open lakehouse.

Apache Iceberg is an open table format designed for large analytic workloads. It supports schema evolution, hidden partitioning, partition layout evolution and time travel. Every table change creates an Iceberg snapshot, this helps to resolve concurrency issues and allows readers to scan a stable table state every time.

Iceberg lends itself well to a variety of use cases including Lakehouse Analytics, Data Engineering pipelines, and regulatory compliance with specific aspects of regulations such as GDPR (General Data Protection Regulation) and CCPA (California Consumer Privacy Act) that require being able to delete customer data upon request.

CDE Virtual Clusters provide native support for Iceberg. Users can run Spark workloads and interact with their Iceberg tables via SQL statements. The Iceberg Metadata Layer tracks Iceberg table versions via Snapshots and provides Metadata Tables with snapshot and other useful information. In this Lab we used Iceberg to access the credit card transactions dataset as of a particular timestamp.

In this section you first explored two datasets interactively with CDE Interactive sessions. This feature allowed you to run ad-hoc queries on large, structured and unstructured data, and prototype Spark Application code for batch execution.


### Useful Links and Resources

If you are curious to learn more about the above features in the context of more advanced use cases, please visit the following references:

### Useful Links and Resources

If you are curious to learn more about the above features in the context of more advanced use cases, please visit the following references:

* [Apache Iceberg in the Cloudera Data Platform](https://docs.cloudera.com/cdp-public-cloud/cloud/cdp-iceberg/topics/iceberg-in-cdp.html)
* [Exploring Iceberg Architecture](https://github.com/pdefusco/Exploring_Iceberg_Architecture)
* [Using Apache Iceberg in Cloudera Data Engineering](https://docs.cloudera.com/data-engineering/cloud/manage-jobs/topics/cde-using-iceberg.html)
* [Importing and Migrating Iceberg Table in Spark 3](https://docs.cloudera.com/data-engineering/cloud/manage-jobs/topics/cde-iceberg-import-migrate-table.html)
* [Getting Started with Iceberg and Spark](https://iceberg.apache.org/docs/latest/spark-getting-started/)
* [Iceberg SQL Syntax](https://iceberg.apache.org/docs/latest/spark-queries/)
