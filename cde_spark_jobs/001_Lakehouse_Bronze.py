#****************************************************************************
# (C) Cloudera, Inc. 2020-2023
#  All rights reserved.
#
#  Applicable Open Source License: GNU Affero General Public License v3.0
#
#  NOTE: Cloudera open source products are modular software products
#  made up of hundreds of individual components, each of which was
#  individually copyrighted.  Each Cloudera open source product is a
#  collective work under U.S. Copyright Law. Your license to use the
#  collective work is as provided in your written agreement with
#  Cloudera.  Used apart from the collective work, this file is
#  licensed for your use pursuant to the open source license
#  identified above.
#
#  This code is provided to you pursuant a written agreement with
#  (i) Cloudera, Inc. or (ii) a third-party authorized to distribute
#  this code. If you do not have a written agreement with Cloudera nor
#  with an authorized and properly licensed third party, you do not
#  have any rights to access nor to use this code.
#
#  Absent a written agreement with Cloudera, Inc. (“Cloudera”) to the
#  contrary, A) CLOUDERA PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY
#  KIND; (B) CLOUDERA DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED
#  WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT LIMITED TO
#  IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND
#  FITNESS FOR A PARTICULAR PURPOSE; (C) CLOUDERA IS NOT LIABLE TO YOU,
#  AND WILL NOT DEFEND, INDEMNIFY, NOR HOLD YOU HARMLESS FOR ANY CLAIMS
#  ARISING FROM OR RELATED TO THE CODE; AND (D)WITH RESPECT TO YOUR EXERCISE
#  OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, CLOUDERA IS NOT LIABLE FOR ANY
#  DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR
#  CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO, DAMAGES
#  RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF
#  BUSINESS ADVANTAGE OR UNAVAILABILITY, OR LOSS OR CORRUPTION OF
#  DATA.
#
# #  Author(s): Paul de Fusco
#***************************************************************************/

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
import sys, random, os, json, random, configparser
from utils import *

spark = SparkSession \
    .builder \
    .appName("BANK TRANSACTIONS BRONZE LAYER") \
    .getOrCreate()

config = configparser.ConfigParser()
config.read('/app/mount/parameters.conf')
storageLocation=config.get("general","data_lake_name")
print("Storage Location from Config File: ", storageLocation)

username = sys.argv[1]
print("PySpark Runtime Arg: ", sys.argv[1])

#---------------------------------------------------
#               CREATE PII TABLE
#---------------------------------------------------

### PII DIMENSION TABLE
piiDf = spark.read.options(header='True', delimiter=',').csv("{0}/mkthol/pii/{1}/pii".format(storageLocation, username))

### CAST LAT LON AS FLOAT
piiDf = piiDf.withColumn("address_latitude",  piiDf["address_latitude"].cast('float'))
piiDf = piiDf.withColumn("address_longitude",  piiDf["address_longitude"].cast('float'))

### STORE CUSTOMER DATA AS TABLE
piiDf.writeTo("spark_catalog.HOL_DB_{0}.CUST_TABLE_{0}".format(username)).using("iceberg").createOrReplace()

#---------------------------------------------------
#               CREATE REFINED CUSTOMER TABLE
#---------------------------------------------------

spark.sql("DROP TABLE IF EXISTS spark_catalog.HOL_DB_{0}.CUST_TABLE_REFINED_{0}".format(username))

spark.sql("""CREATE TABLE spark_catalog.HOL_DB_{0}.CUST_TABLE_REFINED_{0}
                USING iceberg
                AS SELECT NAME, EMAIL, BANK_COUNTRY, ACCOUNT_NO, CREDIT_CARD_NUMBER, ADDRESS_LATITUDE, ADDRESS_LONGITUDE
                FROM spark_catalog.HOL_DB_{0}.CUST_TABLE_{0}""".format(username))

#---------------------------------------------------
#               SCHEMA EVOLUTION
#---------------------------------------------------

# UPDATE TYPES: Updating Latitude and Longitude FROM FLOAT TO DOUBLE
spark.sql("""ALTER TABLE spark_catalog.HOL_DB_{0}.CUST_TABLE_REFINED_{0}
                ALTER COLUMN ADDRESS_LATITUDE TYPE double""".format(username))

spark.sql("""ALTER TABLE spark_catalog.HOL_DB_{0}.CUST_TABLE_REFINED_{0}
                ALTER COLUMN ADDRESS_LONGITUDE TYPE double""".format(username))

#---------------------------------------------------
#               VALIDATA TABLE
#---------------------------------------------------

spark.sql("""SELECT * FROM spark_catalog.HOL_DB_{0}.CUST_TABLE_REFINED_{0}""".format(username)).show()


#---------------------------------------------------
#               PROCESS BATCH DATA
#---------------------------------------------------

### TRANSACTIONS FACT TABLE
trxBatchDf = spark.read.json("{0}/mkthol/trans/{1}/trx_batch_2".format(storageLocation, username))

### TRX DF SCHEMA BEFORE CASTING
trxBatchDf.printSchema()

### CAST TYPES
cols = ["transaction_amount", "latitude", "longitude"]
trxBatchDf = castMultipleColumns(trxBatchDf, cols)
trxBatchDf = trxBatchDf.withColumn("event_ts", trxBatchDf["event_ts"].cast("timestamp"))

### TRX DF SCHEMA AFTER CASTING
trxBatchDf.printSchema()

#---------------------------------------------------
#               LOAD BATCH DATA IN BRANCH
#---------------------------------------------------

# CREATE TABLE BRANCH

spark.sql("ALTER TABLE spark_catalog.HOL_DB_{0}.TRANSACTIONS_{0} DROP BRANCH IF EXISTS ingestion_branch".format(username))
spark.sql("ALTER TABLE spark_catalog.HOL_DB_{0}.TRANSACTIONS_{0} CREATE BRANCH ingestion_branch".format(username))

# WRITE DATA OPERATION ON TABLE BRANCH
trxBatchDf.write.format("iceberg").option("branch", "ingestion_branch").mode("append").save("spark_catalog.HOL_DB_{0}.TRANSACTIONS_{0}".format(username))

#---------------------------------------------------
#               VALIDATE BATCH DATA IN BRANCH
#---------------------------------------------------

# Notice that a simple SELECT query against the table still returns the original data.
spark.sql("SELECT COUNT(*) FROM spark_catalog.HOL_DB_{0}.TRANSACTIONS_{0};".format(username)).show()

# If you want to access the data in the branch, you can specify the branch name in your SELECT query.
spark.sql("SELECT * FROM spark_catalog.HOL_DB_{0}.TRANSACTIONS_{0} VERSION AS OF 'ingestion_branch';".format(username)).show()
