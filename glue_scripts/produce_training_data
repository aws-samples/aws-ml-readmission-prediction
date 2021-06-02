import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql.functions import col,sum,count,to_timestamp,unix_timestamp,to_utc_timestamp
from pyspark.sql.window import Window
import datetime
import os, shutil
## Spark ML pre-processing 
import pyspark
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorIndexer, OneHotEncoder, VectorAssembler, IndexToString
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql.functions import *
import mleap.pyspark
from mleap.pyspark.spark_support import SimpleSparkSerializer
import boto3

## @params: [JOB_NAME]
## @params: [sse_kms_id]
## @params: [s3_bucket]
args = getResolvedOptions(sys.argv,
                          ['JOB_NAME',
                          'sse_kms_id',
                          's3_bucket']
                          )

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

today = datetime.datetime.today()
year = today.year
month = today.month
day = today.day

sse_kms_id = args['sse_kms_id']
s3_bucket = args['s3_bucket']

s3_train_bucket_prefix = "train-data"
s3_validation_bucket_prefix = "validation-data"
s3_test_bucket_prefix = "test-data"
s3_model_bucket_prefix = "spark-ml-model" + "/"+ str(year) + "/" + str(month) + "/" + str(day)
output_dir = "s3://" + s3_bucket + "/" + s3_train_bucket_prefix + "/"+ str(year) + "/" + str(month) + "/" + str(day)
input_dir = "s3://"+ s3_bucket +"/output-dir"

## Important functions
def count_null(c):
    """Use conversion between boolean and integer
    - False -> 0
    - True -> 1
    """
    pred = col(c).isNull()
    return sum(pred.cast("integer")).alias(c)         
    
def null_perc (df):
    exprs = [((count_null(c) / count("*"))*100).alias(c) for c in df.columns]
    return df.agg(*exprs).collect()


## Creating glue dynamic frame, map respective columns, drop null value columns and drop fields for feature selection

patients = glueContext.create_dynamic_frame_from_options('s3',connection_options={'paths':[input_dir + '/patients/*/*/*'],},format="parquet",transformation_ctx = "patients_csv")
patients_dropcol = patients.drop_fields(['patient_county','patient_deathdate','patient_drivers','patient_ssn','patient_passport','patient_suffix','patient_birthplace','patient_prefix','patient_maiden','patient_state','patient_zip','patient_first','patient_last','patient_city','patient_address','patient_lon','patient_lat','partition_0','partition_1','partition_2'])

##Dropping fields for feature engineering
conditions = glueContext.create_dynamic_frame_from_options('s3',connection_options={'paths':[input_dir + '/conditions/*/*/*'],},format="parquet",transformation_ctx = "conditions_csv")
conditions_dropcol = conditions.drop_fields(['conditions_stop','conditions_start','conditions_description','partition_0','partition_1','partition_2'])

encounters = glueContext.create_dynamic_frame_from_options('s3',connection_options={'paths':[input_dir + '/encounters/*/*/*'],},format="parquet",transformation_ctx = "encounters_csv")
encounters_dropcol = encounters.drop_fields(['encounters_description','encounters_reasondescription','partition_0','partition_1','partition_2'])

immunizations = glueContext.create_dynamic_frame_from_options('s3',connection_options={'paths':[input_dir + '/immunizations/*/*/*'],},format="parquet",transformation_ctx = "immunizations_csv")
immunizations_dropcol = immunizations.drop_fields(['immunizations_patient','immunizations_date','immunizations_code','immunizations_description','immunizations_base_cost','partition_0','partition_1','partition_2'])

medications = glueContext.create_dynamic_frame_from_options('s3',connection_options={'paths':[input_dir + '/medications/*/*/*'],},format="parquet",transformation_ctx = "medications_csv")
medications_dropcol = medications.drop_fields(['medications_dispenses','medications_start','medications_payer','medications_code','medications_stop','medications_reasoncode','medications_reasondescription','medications_description','partition_0','partition_1','partition_2'])

procedures = glueContext.create_dynamic_frame_from_options('s3',connection_options={'paths':[input_dir + '/procedures/*/*/*'],},format="parquet",transformation_ctx = "procedures_csv")
procedures_dropcol = procedures.drop_fields(['procedures_description','procedures_date','procedures_reasoncodedescription','partition_0','partition_1','partition_2'])

providers = glueContext.create_dynamic_frame_from_options('s3',connection_options={'paths':[input_dir + '/providers/*/*/*'],},format="parquet",transformation_ctx = "providers_csv")
providers_dropcol = providers.drop_fields(['providers_lat','providers_lon','providers_gender','providers_zip','providers_id','providers_city','providers_name','providers_address','partition_0','partition_1','partition_2'])

organizations = glueContext.create_dynamic_frame_from_options('s3',connection_options={'paths':[input_dir + '/organizations/*/*/*'],},format="parquet",transformation_ctx = "organizations_csv")
organizations_dropcol = organizations.drop_fields(['organizations_utilization','organizations_name','organizations_zip','organizations_city','organizations_address','organizations_lat','organizations_lon','organizations_phone','organizations_state','partition_0','partition_1','partition_2'])


## Inner Join Patient and Encounters Table
## Glue join will do inner join only 
l_history = Join.apply(patients_dropcol, encounters_dropcol, 'patient_id', 'encounters_patient').drop_fields(['encounters_patient'])

## Identify missing data in the dynamic frame by converting into pandas data frame 
df_history = l_history.toDF() ## Convert to Spark Data Frame 
## Calculate percent of Null Values
print ('Null values percentage in each column:')
print (null_perc(df_history))

## Updating Encounter reason code with zero where missing 
df_history = df_history.na.fill({'encounters_reasoncode': 0})

## Converting to Spark Data Frame 
immunizations_dropcol = immunizations_dropcol.toDF()
conditions_dropcol = conditions_dropcol.toDF()
procedures_dropcol = procedures_dropcol.toDF()
medications_dropcol = medications_dropcol.toDF()
providers_dropcol = providers_dropcol.toDF()
organizations_dropcol = organizations_dropcol.toDF()

## Left Join Spark Data Frame with Immunizations, Procedures, Medications, Conditions, Imaging Studies, Providers and Organizations table
df_history = df_history.join(immunizations_dropcol, df_history.encounters_id == immunizations_dropcol.immunizations_encounter,how='left')
df_history = df_history.join(conditions_dropcol, df_history.encounters_id == conditions_dropcol.conditions_encounter,how='left')
df_history = df_history.join(procedures_dropcol, df_history.encounters_id == procedures_dropcol.procedures_encounter,how='left')
df_history = df_history.join(medications_dropcol, df_history.encounters_id == medications_dropcol.medications_encounter,how='left')
df_history = df_history.join(organizations_dropcol, df_history.encounters_provider == organizations_dropcol.organizations_id,how='left')
df_history = df_history.join(providers_dropcol, df_history.organizations_id == providers_dropcol.providers_organization,how='left')
## Calculate percent of Null Values
print ('Null values percentage in each column:')
print (null_perc(df_history))

## Dropping duplicate value columns
drop_column_list = ['procedures_patient','medications_patient','immunizations_encounter',
                       'conditions_encounter','procedures_encounter','medications_encounter','imaging_studies_encounter',
                       'providers_organization','medications_payer_coverage',
                       'conditions_patient','conditions_code','procedures_reasondescription','encounters_provider','organizations_revenue','medications_base_cost','medications_totalcost','procedures_reasoncode']
df_history = df_history.select([column for column in df_history.columns if column not in drop_column_list]) 

df_history = df_history.filter(df_history.encounters_encounterclass.isin('inpatient','ambulatory','urgentcare','outpatient','emergency'))
## Convert data type from string to timestamp
df_history = df_history.withColumn("encounters_stop", to_utc_timestamp("encounters_stop", "GMT"))
df_history = df_history.withColumn("encounters_start", to_utc_timestamp("encounters_start", "GMT"))
print('encounters time duration values')
df_history.show(truncate=100)


##Subtract encounter stop and encounter start to calculate admission duration for the patient
df_history = df_history.withColumn("admission_duration", unix_timestamp("encounters_stop")-unix_timestamp("encounters_start"))
#df_history = df_history.withColumn("admission_duration", col("encounters_stop")-col("encounters_start"))
df_history.printSchema()
print('admission duration values')
df_history.show(truncate=100)


##Replace empty strings with required values
df_history = df_history.replace('','NM','patient_marital')

# Fill missing values
## Imputation of missing data 
## There are entries in which procedures_code is missing, reason could be that no procedures were performed 
## so setting the value as zero for those outliers 
## Setting the procedures base cost to zero since there was no procedure performed and no charge shown

df_history = df_history.fillna({"patient_marital" : "NM", "procedures_code" : 0, "procedures_base_cost" : 0})

print ('Null values percentage in each column:')
print (null_perc(df_history))

## Sorting the data by Patient Id and Encounter Start Date so that we can calculate readmission within x number of days. 
##After looking at the data, it was found that there are multiple entries for encounter on the same start date so adding 
## encounter stop date to the sort
df_history = df_history.sort('patient_id', 'encounters_start', 'encounters_stop')
print('sorted data frame rows as needed')
df_history.show(truncate=100)

##Calculating readmission hours from the data
df_history = df_history.withColumn("readmission_days", datediff(df_history.encounters_start, lag(df_history.encounters_stop, 1)
    .over(Window.partitionBy("patient_id")
    .orderBy('patient_id', 'encounters_start', 'encounters_stop'))))
print ('calculated readmission days')
df_history.show(truncate=100)

##Creating new column readmission when readmission days is greater than 30
df_history = df_history.withColumn("readmission", when((df_history.readmission_days<30) & (df_history.readmission_days>0), 1).otherwise(0))
print ('calculated readmission value')
df_history.show(truncate=100) 

## Checking for over fitting vs under fitting training data
print('Number of positive samples: ', df_history.filter("readmission == '1'").count())
print('Number of negative samples: ', df_history.filter("readmission == '0'").count())
print('Count of total samples: ', df_history.count())

##Convert Birthdate into Age
## Convert BIRTH DATE INTO AGE factor
from pyspark.sql.functions import *
df_history = df_history.withColumn("current_date",current_date()).withColumn("age",(datediff(col("current_date"),df_history.patient_birthdate)/365))
df_history.show(truncate=100)

## Dropping duplicate value columns
drop_column_list = ['current_date','admission_duration','medications_totalcost','medications_base_cost',
                                          'patient_birthdate','encounters_id','patient_id','organizations_id',
                                         'encounters_start','encounters_stop',
                                            'encounters_payer','readmission_days','providers_speciality']
df_history = df_history.select([column for column in df_history.columns if column not in drop_column_list])


print ('Null values percentage in each column after imputation and dropping columns:')
print (null_perc(df_history))


## Schema
## |-- encounters_reasoncode: long (nullable = false)
## |-- patient_healthcare_expenses: double (nullable = true)
## |-- encounters_encounterclass: string (nullable = true)
## |-- patient_gender: string (nullable = true)
## |-- patient_healthcare_coverage: double (nullable = true)
## |-- patient_marital: string (nullable = false)
## |-- encounters_total_claim_cost: double (nullable = true)
## |-- patient_ethnicity: string (nullable = true)
## |-- encounters_code: long (nullable = true)
## |-- encounters_payer_coverage: double (nullable = true)
## |-- encounters_base_encounter_cost: double (nullable = true)
## |-- patient_race: string (nullable = true)
## |-- procedures_code: long (nullable = false)
## |-- procedures_base_cost: double (nullable = false)
## |-- providers_state: string (nullable = true)
## |-- providers_utilization: long (nullable = true)
## |-- readmission: integer (nullable = false)
## |-- age: double (nullable = true)

df_history.printSchema()

## Splitting the data into training and test for Spark ML model
(train_df_history, test_df_history) = df_history.randomSplit([0.8, 0.2])

### Categorical variables pre-processing using Spark ML
## Based on the machine learning model, you have to identify feature extraction strategy
## For Random forest model, we can use StringIndex and for models such as XGBoost, you can use OneHot Encoding
## More about feature extraction using Spark ML - https://spark.apache.org/docs/1.4.1/ml-features.html

from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler

categoricalColumns = ['encounters_encounterclass', 'patient_gender', 'patient_marital', 'patient_ethnicity', 'patient_race', 'encounters_reasoncode', 'encounters_code', 'procedures_code']
stages = []
for categoricalCol in categoricalColumns:
    stringIndexer = StringIndexer(inputCol = categoricalCol, outputCol = categoricalCol + 'Index')
    encoder = OneHotEncoder(inputCol=stringIndexer.getOutputCol(), outputCol = (categoricalCol + "classVec"))
    stages += [stringIndexer, encoder]
numericCols = ['patient_healthcare_expenses', 'patient_healthcare_coverage', 'encounters_total_claim_cost', 'encounters_payer_coverage', 'encounters_base_encounter_cost', 'procedures_base_cost','providers_utilization','age']
assemblerInputs = [c + "classVec" for c in categoricalColumns] + numericCols
assembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features")
stages += [assembler]
pipeline = Pipeline(stages = stages)
pipelineModel = pipeline.fit(train_df_history)
transform_df_history = pipelineModel.transform(train_df_history)
transform_df_history.printSchema()


# Delete previous data from output
s3 = boto3.resource('s3')
bucket = s3.Bucket(s3_bucket)

# Serialize and store via MLeap
import mleap.pyspark
from mleap.pyspark.spark_support import SimpleSparkSerializer
pipelineModel.serializeToBundle("jar:file:/tmp/model.zip", transform_df_history)

# Unzipping as SageMaker expects a .tar.gz file but MLeap produces a .zip file.
## Resources - https://github.com/combust/mleap/blob/master/python/README.md#pyspark-integration

import zipfile
with zipfile.ZipFile("/tmp/model.zip") as zf:
    zf.extractall("/tmp/model")

# Writing back the content as a .tar.gz file
import tarfile
with tarfile.open("/tmp/model.tar.gz", "w:gz") as tar:
    tar.add("/tmp/model/bundle.json", arcname='bundle.json')
    tar.add("/tmp/model/root", arcname='root')

s3 = boto3.resource('s3')
file_name = s3_model_bucket_prefix + '/' + 'model.tar.gz'
s3.Bucket(s3_bucket).upload_file('/tmp/model.tar.gz', file_name, ExtraArgs={"ServerSideEncryption": "aws:kms","SSEKMSKeyId":sse_kms_id})

os.remove('/tmp/model.zip')
os.remove('/tmp/model.tar.gz')
shutil.rmtree('/tmp/model')

## We need only features and original columns for training data
selectedCols = ['features'] + ['readmission']
transform_df_history = transform_df_history.select(selectedCols)
## Renaming the column as SageMaker XGBoost Algorithm is expecting features and label columns
transform_df_history = transform_df_history.withColumnRenamed("readmission","label")

## Splitting the data into test and validation data set for Training data set
train, validation = transform_df_history.randomSplit([0.7, 0.3], seed = 2018)
print("Training Dataset Count: " + str(train.count()))
print("Test Dataset Count: " + str(validation.count()))

## Converting Spark Data Frame to CSV and writing to S3
today = datetime.datetime.today()
year = today.year
month = today.month
day = today.day

def toCSVLine(data):
    r = ','.join(str(d) for d in data[1])
    return str(data[0]) + "," + r

## Write Spark Dataframe to S3
## Performance improvments through EMRFS S3-Optimized Committer - https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark-committer-reqs.html
## Blog - https://aws.amazon.com/blogs/big-data/improve-apache-spark-write-performance-on-apache-parquet-formats-with-the-emrfs-s3-optimized-committer/

# This is needed to write RDDs to file which is the only way to write nested Dataframes into CSV.
spark.sparkContext._jsc.hadoopConfiguration().set("mapred.output.committer.class",
                                                      "org.apache.hadoop.mapred.FileOutputCommitter")

# Save transformed training data to CSV in S3 by converting to RDD.
transformed_train_rdd = train.rdd.map(lambda x: (x.label, x.features))
lines = transformed_train_rdd.map(toCSVLine)
lines.saveAsTextFile('s3a://' + s3_bucket + '/' + s3_train_bucket_prefix +'/'+str(year)+'/'+str(month)+'/'+str(day)+'/')
    
# Similar data processing for validation dataset.
transformed_train_rdd = validation.rdd.map(lambda x: (x.label, x.features))
lines = transformed_train_rdd.map(toCSVLine)
lines.saveAsTextFile('s3a://' + s3_bucket + '/' + s3_validation_bucket_prefix +'/'+str(year)+'/'+str(month)+'/'+str(day)+'/')

test_df_history.write.format("parquet").save('s3a://' + s3_bucket + '/' + s3_test_bucket_prefix +'/'+str(year)+'/'+str(month)+'/'+str(day)+'/', header = 'true')

job.commit()
