from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import sha2
from pyspark.sql.functions import *
from pyspark.sql.functions import year, month, dayofmonth, date_format
from pyspark.sql.types import DecimalType
from pyspark.sql.functions import regexp_replace
import pyspark
import pyspark.sql.functions as f
import json

spark=SparkSession.builder.appName("Transformation").getOrCreate()
spark.sparkContext.addPyFile("s3://karthik-landingzone-07/Delta_file/delta-core_2.12-0.8.0.jar")
from delta import *

class transform:
    
    def __init__(self,landing_dataset,raw_dataset,stagging_dataset):
        self.landing=landing_dataset
        self.raw=raw_dataset
        self.stagging=stagging_dataset
    
    #read data from rawzone
    def read_rawzone_data(self):
        df=spark.read.option('inferSchema',True).parquet(self.raw)
        return df
    
    #masking columns
    def masking_columns(self,data,column1):
        for field in column1:
            data=data.withColumn("masked_"+field, sha2(field, 256).alias('s'))
        return data
    
    #casting columns 
    def casting_columns(self,df,cast_dict):
        items_list=[]
        for item in cast_dict.keys():
            items_list.append(item)
        
        for column in items_list:
            if cast_dict[column].split(",")[0] == "DecimalType":
                df = df.withColumn(column,df[column].cast(DecimalType(scale=int(cast_dict[column].split(",")[1]))))
            elif cast_dict[column].split(",")[0] == "ArrayType-StringType":
                df = df.withColumn(column,f.concat_ws(",",'city','state','location_category'))
        return df
        
    #writing data to staging location
    def write_data(self,data,partition_columns):
        data.write.mode('overwrite').partitionBy(partition_columns[0],partition_columns[1]).parquet(self.stagging)
    
        
    #lookup dataset
    def lookup(self,df,dataset,folder_path,columns):
       
        lookup_location = folder_path   
        print(lookup_location)
        pii_col = columns
        print(pii_col)
        Dataset_Name = 'lookup_table'        
        df_source = df.withColumn("Start_Date", f.current_date())        
        df_source = df_source.withColumn("Update_Date", f.lit("null"))
        pii_cols = []
        for i in pii_col:
            if i in df.columns:
                pii_cols.append(i)
        print(pii_cols)
        
        required_col = []

        for col in pii_cols:
            if col in df.columns:
                required_col += [col, "masked_" + col]
        print(required_col)
        source_columns_used = required_col + ['Start_Date', 'Update_Date']
        df_source = df_source.select(*source_columns_used)
        try:
            targetTable = DeltaTable.forPath(spark, lookup_location + dataset)
            delta_df = targetTable.toDF()
        except pyspark.sql.utils.AnalysisException:
            logging.info('Table does not exist')
            df_source = df_source.withColumn("Flag_Active", f.lit("true"))
            df_source.write.format("delta").mode("overwrite").save(lookup_location + dataset)
            logging.info('Table is Created Sucessfully...')
            targetTable = DeltaTable.forPath(spark, lookup_location + dataset)
            delta_df = targetTable.toDF()
            delta_df.show(100)
            
        new_dict = {}
        for i in required_col:
            new_dict[i] = "updates." + i
        new_dict['Start_Date'] = f.current_date()
        new_dict['Flag_Active'] = "True"
        new_dict['Update_Date'] = "null"
        condition = Dataset_Name + ".Flag_Active == true AND " + " OR ".join(["updates." + i + " <> " + Dataset_Name + "." + i for i in [x for x in required_col if x.startswith("masked_")]])
        column = ",".join([Dataset_Name + "." + i for i in [x for x in pii_cols]])
        updatedColumnsToInsert = df_source.alias("updates").join(targetTable.toDF().alias(Dataset_Name), pii_cols).where(condition)
        stagedUpdates = (
            updatedColumnsToInsert.selectExpr('NULL as mergeKey', *[f"updates.{i}" for i in df_source.columns]).union(
                df_source.selectExpr("concat(" + ','.join([x for x in pii_cols]) + ") as mergeKey", "*")))
        targetTable.alias(Dataset_Name).merge(stagedUpdates.alias("updates"), "concat(" + str(column) + ") = mergeKey").whenMatchedUpdate(
            condition=condition,
            set={  
                "Update_Date": f.current_date()
            }
        ).whenNotMatchedInsert(values=new_dict).execute()
        for i in pii_cols:
            df = df.drop(i).withColumnRenamed("masked_" + i, i)
        return df
       
    
configData = spark.sparkContext.textFile("s3://karthik-landingzone-07/config/app_conf.json").collect()
data = ''.join(configData)
jsonData = json.loads(data)


landingzone = jsonData['actives-source']['source']['data-location']
landingzone = "/".join(landingzone.split("/")[0:-1])
landingzone_path = landingzone+"/"

rawzone = jsonData['actives-destination']['source']['data-location']
rawzone = "/".join(rawzone.split("/")[0:-1])

stagingzone = jsonData['actives-destination']['destination']['data-location']
stagingzone = "/".join(stagingzone.split("/")[0:-1])

data = ['Actives','Viewership']
for i in data:
    
    if i == 'Actives':
        loc = 'actives-destination'
    elif i=='Viewership':
        loc = 'viewership-destination'
    masked_columns = jsonData[loc]['mask-columns']
    casting_column = jsonData[loc]['transform-columns']
    partition_columns = jsonData[loc]['partition-columns']
    lookup_location = jsonData['lookup-dataset']['data-location']
    columns = jsonData['lookup-dataset']['pii-cols']
    print(i)
    try:
        t = transform(landingzone+"/"+i.lower()+".parquet",rawzone+"/"+i+"/",stagingzone+"/"+i+"/")
        
        try:
            data = t.read_rawzone_data()
            print('read data')
        except:
            continue
            
        masked_data = t.masking_columns(data,masked_columns)
        casting_data = t.casting_columns(masked_data,casting_column)
        t.write_data(casting_data,partition_columns)
        final_df = t.lookup(casting_data,i,lookup_location,columns)
    except Exception as e:
        print(e)
        print('No data',i)
        
        