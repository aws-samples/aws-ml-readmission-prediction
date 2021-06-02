import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import datetime

today = datetime.datetime.today()
year = today.year
month = today.month
day = today.day


## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME','s3_bucket','glue_crawler_db'])
db_name = args['glue_crawler_db']
des_bucket_name = args['s3_bucket']


sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = db_name, table_name = "patients", transformation_ctx = "patients_csv"]
## @return: patients_csv
## @inputs: []
patients = glueContext.create_dynamic_frame.from_catalog(database = db_name, table_name = "patients", transformation_ctx = "patients_csv")
## @type: ApplyMapping
## @args: [mapping = [("id", "string", "id", "string"), ("birthdate", "string", "birthdate", "string"), ("deathdate", "string", "deathdate", "string"), ("ssn", "string", "ssn", "string"), ("drivers", "string", "drivers", "string"), ("passport", "string", "passport", "string"), ("prefix", "string", "prefix", "string"), ("first", "string", "first", "string"), ("last", "string", "last", "string"), ("suffix", "string", "suffix", "string"), ("maiden", "string", "maiden", "string"), ("marital", "string", "marital", "string"), ("race", "string", "race", "string"), ("ethnicity", "string", "ethnicity", "string"), ("gender", "string", "gender", "string"), ("birthplace", "string", "birthplace", "string"), ("address", "string", "address", "string"), ("city", "string", "city", "string"), ("state", "string", "state", "string"), ("county", "string", "county", "string"), ("zip", "long", "zip", "long"), ("lat", "double", "lat", "double"), ("lon", "double", "lon", "double"), ("healthcare_expenses", "double", "healthcare_expenses", "double"), ("healthcare_coverage", "double", "healthcare_coverage", "double"), ("partition_0", "string", "partition_0", "string"), ("partition_1", "string", "partition_1", "string"), ("partition_2", "string", "partition_2", "string")], transformation_ctx = "patients_mapping"]
## @return: patients_mapping
## @inputs: [frame = patients]
patients_mapping = ApplyMapping.apply(frame = patients, mappings = [("id", "string", "patient_id", "string"), ("birthdate", "string", "patient_birthdate", "string"), ("deathdate", "string", "patient_deathdate", "string"), ("ssn", "string", "patient_ssn", "string"), ("drivers", "string", "patient_drivers", "string"), ("passport", "string", "patient_passport", "string"), ("prefix", "string", "patient_prefix", "string"), ("first", "string", "patient_first", "string"), ("last", "string", "patient_last", "string"), ("suffix", "string", "patient_suffix", "string"), ("maiden", "string", "patient_maiden", "string"), ("marital", "string", "patient_marital", "string"), ("race", "string", "patient_race", "string"), ("ethnicity", "string", "patient_ethnicity", "string"), ("gender", "string", "patient_gender", "string"), ("birthplace", "string", "patient_birthplace", "string"), ("address", "string", "patient_address", "string"), ("city", "string", "patient_city", "string"), ("state", "string", "patient_state", "string"), ("county", "string", "patient_county", "string"), ("zip", "bigint", "patient_zip", "bigint"), ("lat", "double", "patient_lat", "double"), ("lon", "double", "patient_lon", "double"), ("healthcare_expenses", "double", "patient_healthcare_expenses", "double"), ("healthcare_coverage", "double", "patient_healthcare_coverage", "double"), ("partition_0", "string", "partition_0", "string"), ("partition_1", "string", "partition_1", "string"), ("partition_2", "string", "partition_2", "string")], transformation_ctx = "patients_mapping")
## @type: ResolveChoice
## @args: [choice = "make_struct", transformation_ctx = "patients_resolvechoice"]
## @return: patients_resolvechoice
## @inputs: [frame = patients_mapping]
patients_resolvechoice = ResolveChoice.apply(frame = patients_mapping, choice = "make_struct", transformation_ctx = "patients_resolvechoice")
## @type: DropNullFields
## @args: [transformation_ctx = "patients_dropnull"]
## @return: patients_dropnull
## @inputs: [frame = patients_resolvechoice]
patients_dropnull = DropNullFields.apply(frame = patients_resolvechoice, transformation_ctx = "patients_dropnull")
patients_dropnull = patients_dropnull.drop_fields(['partition_0','partition_1','partition_2'])
## @type: DataSink
## @args: [connection_type = "s3", connection_options = {"path": "s3://"+des_bucket_name+"/output-dir/patients"}, format = "parquet", transformation_ctx = "patients_parquet"]
## @return: patients_parquet
## @inputs: [frame = patients_dropnull]
patients_parquet = glueContext.write_dynamic_frame.from_options(frame = patients_dropnull, connection_type = "s3", connection_options = {"path": "s3://"+des_bucket_name+"/output-dir/patients/"+str(year)+"/"+str(month)+"/"+str(day)}, format = "parquet", transformation_ctx = "patients_parquet")
patients_parquet.errorsCount( )
## @type: DataSource
## @args: [database = db_name, table_name = "allergies", transformation_ctx = "allergies_csv"]
## @return: allergies
## @inputs: []
allergies = glueContext.create_dynamic_frame.from_catalog(database = db_name, table_name = "allergies", transformation_ctx = "allergies_csv")
allergies_mapping = ApplyMapping.apply(frame = allergies, mappings = [("start", "string", "allergies_start", "string"), ("stop", "string", "allergies_stop", "string"), ("patient", "string", "allergies_patient", "string"), ("encounter", "string", "allergies_encounter", "string"), ("code", "bigint", "allergies_code", "bigint"), ("description", "string", "allergies_description", "string"),("partition_0", "string", "partition_0", "string"), ("partition_1", "string", "partition_1", "string"), ("partition_2", "string", "partition_2", "string")], transformation_ctx = "allergies_mapping")
allergies_resolvechoice = ResolveChoice.apply(frame = allergies_mapping, choice = "make_struct", transformation_ctx = "allergies_resolvechoice")
allergies_dropnull = DropNullFields.apply(frame = allergies_resolvechoice, transformation_ctx = "allergies_dropnull")
allergies_dropnull = allergies_dropnull.drop_fields(['partition_0','partition_1','partition_2'])
allergies_parquet = glueContext.write_dynamic_frame.from_options(frame = allergies_dropnull, connection_type = "s3", connection_options = {"path": "s3://"+des_bucket_name+"/output-dir/allergies/"+str(year)+"/"+str(month)+"/"+str(day)}, format = "parquet", transformation_ctx = "allergies_parquet")
allergies_parquet.errorsCount( )

## @type: DataSource
## @args: [database = db_name, table_name = "careplans", transformation_ctx = "careplans_csv"]
## @return: careplans
## @inputs: []
careplans = glueContext.create_dynamic_frame.from_catalog(database = db_name, table_name = "careplans",transformation_ctx = "careplans_csv")
careplans_mapping = ApplyMapping.apply(frame = careplans, mappings = [("id", "string", "careplan_id", "string"), ("start", "string", "careplans_start", "string"), ("stop", "string", "careplans_stop", "string"), ("patient", "string", "careplans_patient", "string"), ("encounter", "string", "careplans_encounter", "string"), ("code", "bigint", "careplans_code", "bigint"), ("description", "string", "careplans_description", "string"), ("reasoncode", "bigint", "careplans_reasoncode", "bigint"), ("reasondescription", "string", "careplans_reasondescription", "string"),("partition_0", "string", "partition_0", "string"), ("partition_1", "string", "partition_1", "string"), ("partition_2", "string", "partition_2", "string")], transformation_ctx = "careplans_mapping")
careplans_resolvechoice = ResolveChoice.apply(frame = careplans_mapping, choice = "make_struct", transformation_ctx = "careplans_resolvechoice")
careplans_dropnull = DropNullFields.apply(frame = careplans_resolvechoice, transformation_ctx = "careplans_dropnull")
careplans_dropnull = careplans_dropnull.drop_fields(['partition_0','partition_1','partition_2'])
careplans_parquet = glueContext.write_dynamic_frame.from_options(frame = careplans_dropnull, connection_type = "s3", connection_options = {"path": "s3://"+des_bucket_name+"/output-dir/careplans/"+str(year)+"/"+str(month)+"/"+str(day)}, format = "parquet", transformation_ctx = "careplans_parquet")
careplans_parquet.errorsCount( )
## @type: DataSource
## @args: [database = db_name, table_name = "conditions", transformation_ctx = "conditions_csv"]
## @return: conditions
## @inputs: []
conditions = glueContext.create_dynamic_frame.from_catalog(database = db_name, table_name = "conditions", transformation_ctx = "conditions_csv")
conditions_mapping = ApplyMapping.apply(frame = conditions, mappings = [("start", "string", "conditions_start", "string"), ("stop", "string", "conditions_stop", "string"), ("patient", "string", "conditions_patient", "string"), ("encounter", "string", "conditions_encounter", "string"), ("code", "bigint", "conditions_code", "bigint"), ("description", "string", "conditions_description", "string"),("partition_0", "string", "partition_0", "string"), ("partition_1", "string", "partition_1", "string"), ("partition_2", "string", "partition_2", "string")], transformation_ctx = "conditions_mapping")
conditions_resolvechoice = ResolveChoice.apply(frame = conditions_mapping, choice = "make_struct", transformation_ctx = "conditions_resolvechoice")
conditions_dropnull = DropNullFields.apply(frame = conditions_resolvechoice, transformation_ctx = "conditions_dropnull")
conditions_dropnull = conditions_dropnull.drop_fields(['partition_0','partition_1','partition_2'])
conditions_parquet = glueContext.write_dynamic_frame.from_options(frame = conditions_dropnull, connection_type = "s3", connection_options = {"path": "s3://"+des_bucket_name+"/output-dir/conditions/"+str(year)+"/"+str(month)+"/"+str(day)}, format = "parquet", transformation_ctx = "conditions_parquet")
conditions_parquet.errorsCount( )
## @type: DataSource
## @args: [database = db_name, table_name = "encounters", transformation_ctx = "encounters_csv"]
## @return: encounters
## @inputs: []
encounters = glueContext.create_dynamic_frame.from_catalog(database = db_name, table_name = "encounters", transformation_ctx = "encounters_csv")
encounters_mapping = ApplyMapping.apply(frame = encounters, mappings = [("id", "string", "encounters_id", "string"),("start", "string", "encounters_start", "string"), ("stop", "string", "encounters_stop", "string"), ("patient", "string", "encounters_patient", "string"), ("provider", "string", "encounters_provider", "string"),("payer", "string", "encounters_payer", "string"),("encounterclass", "string", "encounters_encounterclass", "string"), ("code", "bigint", "encounters_code", "bigint"), ("description", "string", "encounters_description", "string"),("base_encounter_cost", "double", "encounters_base_encounter_cost", "double"),("total_claim_cost", "double", "encounters_total_claim_cost", "double"),("payer_coverage", "double", "encounters_payer_coverage", "double"),("reasoncode", "bigint", "encounters_reasoncode", "bigint"),("reasondescription", "string", "encounters_reasondescription", "string"),("partition_0", "string", "partition_0", "string"), ("partition_1", "string", "partition_1", "string"), ("partition_2", "string", "partition_2", "string")], transformation_ctx = "encounters_mapping")
encounters_resolvechoice = ResolveChoice.apply(frame = encounters_mapping, choice = "make_struct", transformation_ctx = "encounters_resolvechoice")
encounters_dropnull = DropNullFields.apply(frame = encounters_resolvechoice, transformation_ctx = "encounters_dropnull")
encounters_dropnull = encounters_dropnull.drop_fields(['partition_0','partition_1','partition_2'])
encounters_parquet = glueContext.write_dynamic_frame.from_options(frame = encounters_dropnull, connection_type = "s3", connection_options = {"path": "s3://"+des_bucket_name+"/output-dir/encounters/"+str(year)+"/"+str(month)+"/"+str(day)}, format = "parquet", transformation_ctx = "encounters_parquet")
encounters_parquet.errorsCount( )

## @type: DataSource
## @args: [database = db_name, table_name = "imaging_studies", transformation_ctx = "imaging_studies_csv"]
## @return: imaging_studies
## @inputs: []
imaging_studies = glueContext.create_dynamic_frame.from_catalog(database = db_name, table_name = "imaging_studies", transformation_ctx = "imaging_studies_csv")
imaging_studies_mapping = ApplyMapping.apply(frame = imaging_studies, mappings = [("id", "string", "imaging_studies_id", "string"), ("date", "string", "imaging_studies_date", "string"), ("patient", "string", "imaging_studies_patient", "string"), ("encounter", "string", "imaging_studies_encounter", "string"), ("bodysite_code", "bigint", "imaging_studies_bodysite_code", "bigint"), ("bodysite_description", "string", "imaging_studies_bodysite_description", "string"),("modality_code", "string", "imaging_studies_modality_code", "string"),("modality_description", "string", "imaging_studies_modality_description", "string"),("sop_code", "string", "imaging_studies_sop_code", "string"),("sop_description", "string", "imaging_studies_sop_description", "string"),("partition_0", "string", "partition_0", "string"), ("partition_1", "string", "partition_1", "string"), ("partition_2", "string", "partition_2", "string")], transformation_ctx = "imaging_studies_mapping")
imaging_studies_resolvechoice = ResolveChoice.apply(frame = imaging_studies_mapping, choice = "make_struct", transformation_ctx = "imaging_studies_resolvechoice")
imaging_studies_dropnull = DropNullFields.apply(frame = imaging_studies_resolvechoice, transformation_ctx = "imaging_studies_dropnull")
imaging_studies_dropnull = imaging_studies_dropnull.drop_fields(['partition_0','partition_1','partition_2'])
imaging_studies_parquet = glueContext.write_dynamic_frame.from_options(frame = imaging_studies_dropnull, connection_type = "s3", connection_options = {"path": "s3://"+des_bucket_name+"/output-dir/imaging_studies/"+str(year)+"/"+str(month)+"/"+str(day)}, format = "parquet", transformation_ctx = "imaging_studies_parquet")
imaging_studies_parquet.errorsCount( )

## @type: DataSource
## @args: [database = db_name, table_name = "immunizations", transformation_ctx = "immunizations_csv"]
## @return: immunizations
## @inputs: []
immunizations = glueContext.create_dynamic_frame.from_catalog(database = db_name, table_name = "immunizations", transformation_ctx = "immunizations_csv")
immunizations_mapping = ApplyMapping.apply(frame = immunizations, mappings = [("date", "string", "immunizations_date", "string"), ("patient", "string", "immunizations_patient", "string"), ("encounter", "string", "immunizations_encounter", "string"), ("code", "bigint", "immunizations_code", "bigint"), ("description", "string", "immunizations_description", "string"), ("base_cost", "double", "immunizations_base_cost", "double"),("partition_0", "string", "partition_0", "string"), ("partition_1", "string", "partition_1", "string"), ("partition_2", "string", "partition_2", "string")], transformation_ctx = "immunizations_mapping")
immunizations_resolvechoice = ResolveChoice.apply(frame = immunizations_mapping, choice = "make_struct", transformation_ctx = "immunizations_resolvechoice")
immunizations_dropnull = DropNullFields.apply(frame = immunizations_resolvechoice, transformation_ctx = "immunizations_dropnull")
immunizations_dropnull = immunizations_dropnull.drop_fields(['partition_0','partition_1','partition_2'])
immunizations_parquet = glueContext.write_dynamic_frame.from_options(frame = immunizations_dropnull, connection_type = "s3", connection_options = {"path": "s3://"+des_bucket_name+"/output-dir/immunizations/"+str(year)+"/"+str(month)+"/"+str(day)}, format = "parquet", transformation_ctx = "immunizations_parquet")
immunizations_parquet.errorsCount( )

## @type: DataSource
## @args: [database = db_name, table_name = "medications", transformation_ctx = "medications_csv"]
## @return: medications
## @inputs: []
medications = glueContext.create_dynamic_frame.from_catalog(database = db_name, table_name = "medications", transformation_ctx = "medications_csv")
medications_mapping = ApplyMapping.apply(frame = medications, mappings = [("start", "string", "medications_start", "string"), ("stop", "string", "medications_stop", "string"), ("patient", "string", "medications_patient", "string"), ("payer", "string", "medications_payer", "string"), ("encounter", "string", "medications_encounter", "string"), ("code", "bigint", "medications_code", "bigint"),("description", "string", "medications_description", "string"),("base_cost", "double", "medications_base_cost", "double"),("payer_coverage", "double", "medications_payer_coverage", "double"),("dispenses", "bigint", "medications_dispenses", "bigint"),("totalcost", "double", "medications_totalcost", "double"),("reasoncode", "bigint", "medications_reasoncode", "bigint"),("reasondescription", "string", "medications_reasondescription", "string"),("partition_0", "string", "partition_0", "string"), ("partition_1", "string", "partition_1", "string"), ("partition_2", "string", "partition_2", "string")], transformation_ctx = "medications_mapping")
medications_resolvechoice = ResolveChoice.apply(frame = medications_mapping, choice = "make_struct", transformation_ctx = "medications_resolvechoice")
medications_dropnull = DropNullFields.apply(frame = medications_resolvechoice, transformation_ctx = "medications_dropnull")
medications_dropnull = medications_dropnull.drop_fields(['partition_0','partition_1','partition_2'])
medications_parquet = glueContext.write_dynamic_frame.from_options(frame = medications_dropnull, connection_type = "s3", connection_options = {"path": "s3://"+des_bucket_name+"/output-dir/medications/"+str(year)+"/"+str(month)+"/"+str(day)}, format = "parquet", transformation_ctx = "medications_parquet")
medications_parquet.errorsCount( )

## @type: DataSource
## @args: [database = db_name, table_name = "procedures", transformation_ctx = "procedures_csv"]
## @return: procedures
## @inputs: []
procedures = glueContext.create_dynamic_frame.from_catalog(database = db_name, table_name = "procedures", transformation_ctx = "procedures_csv")
procedures_mapping = ApplyMapping.apply(frame = procedures, mappings = [("date", "string", "procedures_date", "string"), ("patient", "string", "procedures_patient", "string"), ("encounter", "string", "procedures_encounter", "string"), ("code", "bigint", "procedures_code", "bigint"), ("description", "string", "procedures_description", "string"), ("base_cost", "double", "procedures_base_cost", "double"),("reasoncode", "bigint", "procedures_reasoncode", "bigint"),("reasondescription", "string", "procedures_reasondescription", "string"),("partition_0", "string", "partition_0", "string"), ("partition_1", "string", "partition_1", "string"), ("partition_2", "string", "partition_2", "string")], transformation_ctx = "procedures_mapping")
procedures_resolvechoice = ResolveChoice.apply(frame = procedures_mapping, choice = "make_struct", transformation_ctx = "procedures_resolvechoice")
procedures_dropnull = DropNullFields.apply(frame = procedures_resolvechoice, transformation_ctx = "procedures_dropnull")
procedures_dropnull = procedures_dropnull.drop_fields(['partition_0','partition_1','partition_2'])
procedures_parquet = glueContext.write_dynamic_frame.from_options(frame = procedures_dropnull, connection_type = "s3", connection_options = {"path": "s3://"+des_bucket_name+"/output-dir/procedures/"+str(year)+"/"+str(month)+"/"+str(day)}, format = "parquet", transformation_ctx = "procedures_parquet")
procedures_parquet.errorsCount( )

## @type: DataSource
## @args: [database = db_name, table_name = "providers", transformation_ctx = "providers_csv"]
## @return: procedures
## @inputs: []
providers = glueContext.create_dynamic_frame.from_catalog(database = db_name, table_name = "providers", transformation_ctx = "providers_csv")
providers_mapping = ApplyMapping.apply(frame = providers, mappings = [("id", "string", "providers_id", "string"), ("organization", "string", "providers_organization", "string"), ("name", "string", "providers_name", "string"),("gender", "string", "providers_gender", "string"), ("speciality", "string", "providers_speciality", "string"), ("address", "string", "providers_address", "string"), ("city", "string", "providers_city", "string"),("state", "string", "providers_state", "string"),("zip", "string", "providers_zip", "string"),("lat", "double", "providers_lat", "double"),("lon", "double", "providers_lon", "double"),("utilization", "bigint", "providers_utilization", "bigint"),("partition_0", "string", "partition_0", "string"), ("partition_1", "string", "partition_1", "string"), ("partition_2", "string", "partition_2", "string")], transformation_ctx = "providers_mapping")
providers_resolvechoice = ResolveChoice.apply(frame = providers_mapping, choice = "make_struct", transformation_ctx = "providers_resolvechoice")
providers_dropnull = DropNullFields.apply(frame = providers_resolvechoice, transformation_ctx = "providers_dropnull")
providers_dropnull = providers_dropnull.drop_fields(['partition_0','partition_1','partition_2'])
providers_parquet = glueContext.write_dynamic_frame.from_options(frame = providers_dropnull, connection_type = "s3", connection_options = {"path": "s3://"+des_bucket_name+"/output-dir/providers/"+str(year)+"/"+str(month)+"/"+str(day)}, format = "parquet", transformation_ctx = "providers_parquet")
providers_parquet.errorsCount( )

## @type: DataSource
## @args: [database = db_name, table_name = "observations", transformation_ctx = "observations_csv"]
## @return: observations
## @inputs: []
observations = glueContext.create_dynamic_frame.from_catalog(database = db_name, table_name = "observations", transformation_ctx = "observations_csv")
observations_mapping = ApplyMapping.apply(frame = observations, mappings = [("date", "string", "observations_date", "string"), ("patient", "string", "observations_patient", "string"), ("encounter", "string", "observations_encounter", "string"), ("code", "string", "observations_code", "string"), ("description", "string", "observations_description", "string"), ("value", "string", "observations_value", "string"),("units", "string", "observations_units", "string"),("type", "string", "observations_type", "string"),("partition_0", "string", "partition_0", "string"), ("partition_1", "string", "partition_1", "string"), ("partition_2", "string", "partition_2", "string")], transformation_ctx = "observations_mapping")
observations_resolvechoice = ResolveChoice.apply(frame = observations_mapping, choice = "make_struct", transformation_ctx = "observations_resolvechoice")
observations_dropnull = DropNullFields.apply(frame = observations_resolvechoice, transformation_ctx = "observations_dropnull")
observations_dropnull = observations_dropnull.drop_fields(['partition_0','partition_1','partition_2'])
observations_parquet = glueContext.write_dynamic_frame.from_options(frame = observations_dropnull, connection_type = "s3", connection_options = {"path": "s3://"+des_bucket_name+"/output-dir/observations/"+str(year)+"/"+str(month)+"/"+str(day)}, format = "parquet", transformation_ctx = "observations_parquet")
observations_parquet.errorsCount( )

## @type: DataSource
## @args: [database = db_name, table_name = "organizations", transformation_ctx = "organizations_csv"]
## @return: organizations
## @inputs: []
organizations = glueContext.create_dynamic_frame.from_catalog(database = db_name, table_name = "organizations", transformation_ctx = "organizations_csv")
organizations_mapping = ApplyMapping.apply(frame = organizations, mappings = [("id", "string", "organizations_id", "string"),("name", "string", "organizations_name", "string"),("address", "string", "organizations_address", "string"),("city", "string", "organizations_city", "string"),("state", "string", "organizations_state", "string"),("zip", "string", "organizations_zip", "string"),("lat", "double", "organizations_lat", "double"),("lon", "double", "organizations_lon", "double"),("phone", "string", "organizations_phone", "string"),("revenue", "string", "organizations_revenue", "string"),("utilization", "bigint", "organizations_utilization", "bigint"),("partition_0", "string", "partition_0", "string"), ("partition_1", "string", "partition_1", "string"), ("partition_2", "string", "partition_2", "string")], transformation_ctx = "organizations_mapping")
organizations_resolvechoice = ResolveChoice.apply(frame = organizations_mapping, choice = "make_struct", transformation_ctx = "organizations_resolvechoice")
organizations_dropnull = DropNullFields.apply(frame = organizations_resolvechoice, transformation_ctx = "organizations_dropnull")
organizations_dropnull = organizations_dropnull.drop_fields(['partition_0','partition_1','partition_2'])
organizations_parquet = glueContext.write_dynamic_frame.from_options(frame = organizations_dropnull, connection_type = "s3", connection_options = {"path": "s3://"+des_bucket_name+"/output-dir/organizations/"+str(year)+"/"+str(month)+"/"+str(day)}, format = "parquet", transformation_ctx = "organizations_parquet")
organizations_parquet.errorsCount( )

job.commit()