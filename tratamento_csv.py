# Databricks notebook source
# MAGIC %md
# MAGIC Baixando Bibliotecas

# COMMAND ----------

pip install adal

# COMMAND ----------

pip install pandas


# COMMAND ----------

# MAGIC %md Impordanto as Bibliotecas

# COMMAND ----------

from pyspark.sql.types import *
import pandas as pd
from pyspark.sql.functions import*
from pyspark.sql import functions as f
import adal

# COMMAND ----------


Fazendo a leitura do blob com a montagem do diretório (mount) 
dbutils.fs.mount(source = "wasbs://pedro-avila@caminhodoblob.blob.core.windows.net",
   mount_point = "/mnt/pedro-avila",
   extra_configs = {"fs.azure.account.key.caminhodoblob.blob.core.windows.net":dbutils.secrets.get(scope = "scope-adb-estudos", key = "chave-pedro-avila")})
dbutils.fs.ls("wasbs://caminhodoblob.blob.core.windows.net/lab_03_azure/")


# COMMAND ----------

# MAGIC %md Leitura do arquivo CSV e verificando as tabelas

# COMMAND ----------

df = spark.read.option("header", True).option("inferSchema", True).option("delimiter", ";").csv("/mnt/pedro-avila/orders.csv")

df.show(10)

# COMMAND ----------

# MAGIC %md Verificando os tipos de dados

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md Começando o tratamento das colunas 

# COMMAND ----------

df = (df
    .withColumn('Unit Price', regexp_replace('Unit Price', "," , "."))
    )

# COMMAND ----------

df.show(10)

# COMMAND ----------

df = (df
    .withColumn('Unit Cost', regexp_replace('Unit Cost', "," , "."))
    .withColumn('Total Revenue', regexp_replace('Total Revenue', "," , "."))
    .withColumn('Total Cost', regexp_replace('Total Cost', "," , "."))
    .withColumn('Total Profit', regexp_replace('Total Profit', "," , "."))
    )

# COMMAND ----------

df.show(10)

# COMMAND ----------

df = df.withColumnRenamed('Region','region')

# COMMAND ----------

df.show(10)

# COMMAND ----------

df = df.withColumnRenamed('Country','country')\
        


df.show(10)
        
        

# COMMAND ----------

df = df.withColumnRenamed('Item Type', 'item_type')
       

# COMMAND ----------

df = df.withColumnRenamed('Sales Channel', 'sales_channel')


# COMMAND ----------

df = df.withColumnRenamed('Order Priority', 'order_priority')

# COMMAND ----------

df = df.withColumnRenamed('Order Date', 'order_date')

# COMMAND ----------

df = df.withColumnRenamed('Order ID', 'order_id')

# COMMAND ----------

df = df.withColumnRenamed('Ship Date', 'ship_date')

# COMMAND ----------

df = df.withColumnRenamed('Units Sold', 'units_sold')

# COMMAND ----------

df = df.withColumnRenamed('Unit Price', 'unit_price')

# COMMAND ----------

df = df.withColumnRenamed('Unit Cost', 'unit_cost')

# COMMAND ----------

df = df.withColumnRenamed('Total Revenue', 'total_revenue')

# COMMAND ----------

df = df.withColumnRenamed('Total Cost', 'total_cost')

# COMMAND ----------

df = df.withColumnRenamed('Total Profit', 'total_profit')

# COMMAND ----------

df = (df
    .withColumn('order_date', regexp_replace('order_date', "/" , "-"))
      .withColumn('ship_date', regexp_replace('ship_date', "/" , "-"))
     )

# COMMAND ----------

df = df.withColumn("total_profit",df["total_profit"].cast(DecimalType(12,2)))

# COMMAND ----------

df = df.withColumn("total_profit",df["total_profit"].cast('float'))

# COMMAND ----------

df.show(10)

# COMMAND ----------

df = (df.withColumn("total_revenue",df["total_revenue"].cast(DecimalType(12,2)))
       .withColumn("unit_price",df["unit_price"].cast(DecimalType(12,2)))
       .withColumn("unit_cost",df["unit_cost"].cast(DecimalType(12,2)))
       .withColumn("total_cost",df["total_cost"].cast(DecimalType(12,2)))
       .withColumn("total_profit",df["total_profit"].cast(DecimalType(12,2)))
                   )


# COMMAND ----------

#display(df)
df = (df.withColumn("order_date",to_date("order_date", "dd-MM-yyyy"))
        .withColumn("ship_date",to_date("ship_date", "dd-MM-yyyy")))

# COMMAND ----------

df.show(10)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

resource_app_id_url = "https://database.windows.net/"
service_principal_id = dbutils.secrets.get(scope = "scope-", key = "app-reg-adb")
service_principal_secret = dbutils.secrets.get(scope = "scope", key = "app-user-databricks")
tenant_id = "chave"
authority = "https://login.windows.net/" + tenant_id
azure_sql_url = "jdbc:sqlserver://sql-pedro.database.windows.net"
database_name = "db-pedro"
db_table = "STAGE_pedro_avila.db_STAGE"
encrypt = "true"
host_name_in_certificate = "*.database.windows.net"
context = adal.AuthenticationContext(authority)
token = context.acquire_token_with_client_credentials(resource_app_id_url, service_principal_id, service_principal_secret)
access_token = token["accessToken"]


# COMMAND ----------

df.write \
.format("jdbc")\
.mode("overwrite")\
.option("url", azure_sql_url) \
.option("dbtable", db_table) \
.option("databaseName", database_name) \
.option("accessToken", access_token) \
.option("encrypt", "true") \
.option("hostNameInCertificate", "*.database.windows.net") \
.save()

# COMMAND ----------

# MAGIC %md
# MAGIC dbutils.fs.unmount("/mnt/pedro-avila")

# COMMAND ----------

display(df)
