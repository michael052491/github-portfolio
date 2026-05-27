# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Define All variables here
# MAGIC ###### src_path = give Datalake File source path
# MAGIC ###### dest_path = give Datalake File destination path
# MAGIC ###### entity = give database and tablename..({business_unit}_creative_track.tablename). Tablename would be {zone}_{datasource}_{filename}
# MAGIC ###### formattype = give data format type (parquet)

# COMMAND ----------

# MAGIC %run /Workspace/Cirium/Utils/df_tools

# COMMAND ----------

# MAGIC %md
# MAGIC # Squad Data

# COMMAND ----------

# define variable 

src_path ='abfss://data@dlsfidoprod.dfs.core.windows.net/LandingZone/CreativeTrack/Cirium/TechOps/SquadData.csv'
dest_path ='abfss://data@dlsfidoprod.dfs.core.windows.net/RawZone/CreativeTrack/Cirium/TechOps/SquadData_processed'
entity ='Cirium_creative_track.Capacity_squad'
formattype = 'parquet'


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Below code delete unnecessary log file creation in the storage account 

# COMMAND ----------

# cleanse summary and log files
spark.conf.set("spark.sql.sources.commitProtocolClass",
                   "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")
spark.conf.set("parquet.enable.summary-metadata", "false")
spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanse header

# COMMAND ----------

# clean header
def cleanse_header(df):
    """
    cleanse data frame header
    Parameters
    ----------
    df : Dataframe
        dataframe
    Returns
    -------
    Dataframe : cleanse header dataframe
    """

    col_list = []
    for col in df.columns:
        col = col.strip().replace('\n', '').replace(" ", "_").replace("[^a-zA-Z\d\_]+", "")
        col_list.append(col)
    df = df.toDF(*col_list)
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading CSV file from the Lake

# COMMAND ----------

business_unit = 'Cirium'

# COMMAND ----------

# read csv
from pyspark.sql.types import IntegerType
# # READ CSV
df = spark.read\
  .option("header", True)\
  .option("wholeFile", True)\
  .option("multiline",True)\
  .option("inferSchema", True)\
  .csv(src_path,sep=',',header='true',inferSchema=True,quote='"',encoding='utf-8')

# cleanse header
df = cleanse_header(df)

df = df.withColumn("Available_Hours",df.Available_Hours.cast('int'))



# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Writing Files into the DataLake

# COMMAND ----------

# write parquet file

df \
   .write \
   .mode('overwrite') \
   .option("mergeSchema", "true") \
   .format('parquet') \
   .save(dest_path)

#df.write.mode('overwrite').option("mergeSchema", "true").format('parquet').save(dest_path)

# COMMAND ----------

# MAGIC %sql
# MAGIC refresh table Cirium_creative_track.Capacity_squad;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Cirium_creative_track.Capacity_squad

# COMMAND ----------

# MAGIC %md
# MAGIC # Cycles

# COMMAND ----------

# define variable 

src_path ='abfss://data@dlsfidoprod.dfs.core.windows.net/LandingZone/CreativeTrack/Cirium/A4G/Cycles'
dest_path ='abfss://data@dlsfidoprod.dfs.core.windows.net/RawZone/CreativeTrack/Cirium/A4G/Cycles'
entity ='Cirium_creative_track.A4G_Cycles'
formattype = 'parquet'


# COMMAND ----------

# MAGIC %md
# MAGIC Below code delete unnecessary log file creation in the storage account 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading CSV file from the Lake -  Writing Parquet File

# COMMAND ----------

# read csv

# # READ CSV
df1 = spark.read\
  .option("header", True)\
  .option("wholeFile", True)\
  .option("multiline",True)\
  .option("inferSchema", True)\
  .csv(src_path,sep=',',header='true',inferSchema=True,quote='"',encoding='utf-8')

# cleanse header
df1 = cleanse_header(df1)
# display(df1)


# # write parquet file
df1 \
   .write \
   .mode('overwrite') \
   .option("mergeSchema", "true") \
   .format('parquet') \
   .save(dest_path)

# COMMAND ----------

df1.count()

# COMMAND ----------

display(df1)

# COMMAND ----------

# MAGIC %sql
# MAGIC refresh table Cirium_creative_track.A4G_Cycles;
# MAGIC
# MAGIC describe Cirium_creative_track.A4G_Cycles

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cirium_creative_track.A4G_Cycles

# COMMAND ----------

# MAGIC %md
# MAGIC # Cycles Start Plan

# COMMAND ----------

# define variable 

src_path ='abfss://data@dlsfidoprod.dfs.core.windows.net/LandingZone/CreativeTrack/Cirium/A4G/CyclestartPlanRev'
dest_path ='abfss://data@dlsfidoprod.dfs.core.windows.net/RawZone/CreativeTrack/Cirium/A4G/CyclestartPlanRev'
entity ='Cirium_creative_track.A4G_Cycle_Start_PlanRev'
formattype = 'parquet'


# COMMAND ----------

# MAGIC %md
# MAGIC Below code delete unnecessary log file creation in the storage account 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading CSV file from the Lake

# COMMAND ----------

# DBTITLE 1,Cell 30
# read csv
from pyspark.sql.types import IntegerType, StringType

# # READ CSV
df2 = spark.read\
  .option("header", True)\
  .option("wholeFile", True)\
  .option("multiline",True)\
  .option("inferSchema", True)\
  .csv(src_path,sep=',',header='true',inferSchema=True,quote='"',encoding='utf-8')

# cleanse header
df2 = cleanse_header(df2)

# Cast columns to match table schema
int_columns = ['Progress_(%)', 'Progress_completed_(d)', 'Progress_remaining_(d)', 
               'Progress_(%)_work_item_count_(IC)', 'To_do_IC', 'In_progress_IC', 
               'Done_IC', 'Total_IC', 'Effort']
for col in int_columns:
    if col in df2.columns:
        df2 = df2.withColumn(col, df2[col].cast(IntegerType()))

# Cast capacity columns to string
string_columns = ['Capacity_-_SWE', 'Capacity-_SRE', 'Capacity_-_Data', 'Capacity-_QE']
for col in string_columns:
    if col in df2.columns:
        df2 = df2.withColumn(col, df2[col].cast(StringType()))

# # write parquet file
df2 \
   .write \
   .mode('overwrite') \
   .option("mergeSchema", "true") \
   .format('parquet') \
   .save(dest_path)

# COMMAND ----------

df2.count()

# COMMAND ----------

display(df2)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating Databricks table using the file in DataLake

# COMMAND ----------

# # #convert DataBricks Table

#spark.sql("DROP TABLE IF EXISTS " + entity)
#spark.sql("CREATE TABLE  " + entity + \
#               " USING " + formattype + \
#               " LOCATION '" + dest_path + "'")
#spark.sql("REFRESH TABLE " + entity)

# COMMAND ----------

# MAGIC %sql
# MAGIC refresh table Cirium_creative_track.A4G_Cycle_Start_PlanRev;
# MAGIC describe Cirium_creative_track.A4G_Cycle_Start_PlanRev

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cirium_creative_track.A4G_Cycle_Start_PlanRev

# COMMAND ----------

# MAGIC %md
# MAGIC # Product Asset Register

# COMMAND ----------

# define variable 

src_path ='abfss://data@dlsfidoprod.dfs.core.windows.net/LandingZone/CreativeTrack/Cirium/Techops_TCO/Product_Asset_Register'
dest_path ='abfss://data@dlsfidoprod.dfs.core.windows.net/RawZone/CreativeTrack/Cirium/Techops_TCO/Product_Asset_Register'
entity ='Cirium_creative_track.TechOps_TCO_Product_Asset_Register'
formattype = 'parquet'


# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading CSV file from the Lake

# COMMAND ----------

# read csv

# # READ CSV
df3 = spark.read\
  .option("header", True)\
  .option("wholeFile", True)\
  .option("multiline",True)\
  .option("inferSchema", True)\
  .csv(src_path,sep=',',header='true',inferSchema=True,quote='"',encoding='utf-8')

# cleanse header
df3 = cleanse_header(df3)

# # write parquet file
df3 \
   .write \
   .mode('overwrite') \
   .option("mergeSchema", "true") \
   .format('parquet') \
   .save(dest_path)




# COMMAND ----------

df3.count()

# COMMAND ----------

display(df3)

# COMMAND ----------


# Trigger once
# # # convert DataBricks Table

# spark.sql("DROP TABLE IF EXISTS " + entity)
# spark.sql("CREATE TABLE  " + entity + \
#               " USING " + formattype + \
#               " LOCATION '" + dest_path + "'")
# spark.sql("REFRESH TABLE " + entity)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating Databricks table using the file in DataLake

# COMMAND ----------

# MAGIC %sql
# MAGIC refresh table Cirium_creative_track.Techops_TCO_Product_Asset_Register;
# MAGIC select * from Cirium_creative_track.Techops_TCO_Product_Asset_Register

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Writing Files into the DataLake

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #Data Cost Register

# COMMAND ----------

# define variable 

src_path ='abfss://data@dlsfidoprod.dfs.core.windows.net/LandingZone/CreativeTrack/Cirium/Techops_TCO/Data_Cost_Register'
dest_path ='abfss://data@dlsfidoprod.dfs.core.windows.net/RawZone/CreativeTrack/Cirium/Techops_TCO/Data_Cost_Register'
entity ='Cirium_creative_track.Techops_TCO_Data_Cost_Register'
formattype = 'parquet'


# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading CSV file from the Lake

# COMMAND ----------

# read csv

# # READ CSV
df4 = spark.read\
  .option("header", True)\
  .option("wholeFile", True)\
  .option("multiline",True)\
  .option("inferSchema", True)\
  .csv(src_path,sep=',',header='true',inferSchema=True,quote='"',encoding='utf-8')

# cleanse header
df4 = cleanse_header(df4)

# # write parquet file
df4 \
   .write \
   .mode('overwrite') \
   .option("mergeSchema", "true") \
   .format('parquet') \
   .save(dest_path)




# COMMAND ----------

df4.count()

# COMMAND ----------

display(df4)

# COMMAND ----------


# # Trigger once
# # convert DataBricks Table

# spark.sql("DROP TABLE IF EXISTS " + entity)
# spark.sql("CREATE TABLE  " + entity + \
#               " USING " + formattype + \
#               " LOCATION '" + dest_path + "'")
# spark.sql("REFRESH TABLE " + entity)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating Databricks table using the file in DataLake

# COMMAND ----------

# MAGIC %sql
# MAGIC refresh table Cirium_creative_track.Techops_TCO_Data_Cost_Register;
# MAGIC select * from Cirium_creative_track.Techops_TCO_Data_Cost_Register

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC #Workforce Data

# COMMAND ----------

# define variable 

src_path ='abfss://data@dlsfidoprod.dfs.core.windows.net/LandingZone/CreativeTrack/Cirium/TechOps_Timesheet'
dest_path ='abfss://data@dlsfidoprod.dfs.core.windows.net/RawZone/CreativeTrack/Cirium/TechOps_Timesheet'
entity ='Cirium_creative_track.TechOps_Timesheet'
formattype = 'parquet'


# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ## Reading CSV file from the Lake

# COMMAND ----------

# read csv

# # READ CSV
df5 = spark.read\
  .option("header", True)\
  .option("wholeFile", True)\
  .option("multiline",True)\
  .option("inferSchema", True)\
  .csv(src_path,sep=',',header='true',inferSchema=True,quote='"',encoding='utf-8')

# cleanse header
df5 = cleanse_header(df5)

# # write parquet file
df5 \
   .write \
   .mode('overwrite') \
   .option("mergeSchema", "true") \
   .format('parquet') \
   .save(dest_path)




# COMMAND ----------

df5.count()

# COMMAND ----------

display(df5)

# COMMAND ----------


# # Trigger once
# # convert DataBricks Table

#spark.sql("DROP TABLE IF EXISTS " + entity)
#spark.sql("CREATE TABLE  " + entity + \
#               " USING " + formattype + \
#               " LOCATION '" + dest_path + "'")
# spark.sql("REFRESH TABLE " + entity)

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ## Creating Databricks table using the file in DataLake

# COMMAND ----------

# MAGIC %sql
# MAGIC refresh table Cirium_creative_track.TechOps_Timesheet;
# MAGIC select * from Cirium_creative_track.TechOps_Timesheet

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC #Monthly Squad

# COMMAND ----------

# define variable 

src_path ='abfss://data@dlsfidoprod.dfs.core.windows.net/LandingZone/CreativeTrack/Cirium/TechOps_Squad'
dest_path ='abfss://data@dlsfidoprod.dfs.core.windows.net/RawZone/CreativeTrack/Cirium/TechOps_Squad'
entity ='Cirium_creative_track.TechOps_Monthly_Squad'
formattype = 'parquet'


# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ## Reading CSV file from the Lake

# COMMAND ----------

# DBTITLE 1,Cell 69
# read csv

# Ensure cleanse_header function is available
if 'cleanse_header' not in dir():
    def cleanse_header(df):
        col_list = []
        for col in df.columns:
            col = col.strip().replace('\n', '').replace(" ", "_").replace("[^a-zA-Z\d\_]+", "")
            col_list.append(col)
        df = df.toDF(*col_list)
        return df

# # READ CSV
df6 = spark.read\
  .option("header", True)\
  .option("wholeFile", True)\
  .option("multiline",True)\
  .option("inferSchema", True)\
  .csv(src_path,sep=',',header='true',inferSchema=True,quote='"',encoding='utf-8')

# cleanse header
df6 = cleanse_header(df6)

# # write parquet file
df6 \
   .write \
   .mode('overwrite') \
   .option("mergeSchema", "true") \
   .format('parquet') \
   .save(dest_path)




# COMMAND ----------

df6.count()

# COMMAND ----------

display(df6)

# COMMAND ----------

# DBTITLE 1,Cell 72

# # Trigger once
# # convert DataBricks Table

# spark.sql("DROP TABLE IF EXISTS " + entity)
# spark.sql("CREATE TABLE  " + entity + \
#               " USING " + formattype + \
#               " LOCATION '" + dest_path + "'")
# spark.sql("REFRESH TABLE " + entity)

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ## Creating Databricks table using the file in DataLake

# COMMAND ----------

# %sql
# refresh table Cirium_creative_track.TechOps_Monthly_Squad;
# select * from Cirium_creative_track.TechOps_Monthly_Squad