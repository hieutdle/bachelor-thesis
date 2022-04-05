from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

conf = SparkConf()

conf.set("spark.jars.packages","io.delta:delta-core:1.0.0")
conf.set("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
conf.set("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")


spark = SparkSession.builder.appName("delta_test").config(conf=conf).getOrCreate()


datasets= ['allergies', 'careplans', 'conditions', 'devices', 'encounters', 'imaging_studies', 'immunizations', 'medications',
            'observations', 'organizations', 'patients', 'payer_transitions','payers', 'procedures', 'providers','supplies']

phi_cols=['SSN','DRIVERS','PASSPORT','PREFIX','FIRST','LAST','SUFFIX','MAIDEN','BIRTHPLACE','ADDRESS']

for dataset in datasets:
    df = spark.read.csv("gs://raw_data_lake/%s.csv"%dataset,inferSchema=True, header=True, dateFormat="yyyy-MM-dd")

    if dataset in ['observations','supplies','procedures']:
        df = df.withColumn('DATE', f.to_date(f.col("DATE"),'yyyy-MM-dd'))
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("gs://optimized_data_lake/%s"%dataset)
    else:
        # drop rows with patient PHI
        if dataset == 'patients':
            df=df.drop(*phi_cols)

        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("gs://optimized_data_lake/%s"%dataset)


tablelist = ["CONCEPT","VOCABULARY","CONCEPT_ANCESTOR","CONCEPT_RELATIONSHIP","RELATIONSHIP","CONCEPT_SYNONYM","DOMAIN","CONCEPT_CLASS","DRUG_STRENGTH"]

for table in tablelist:
    df = spark.read.csv("gs://raw_data_lake/omop_vocab/%s.csv"%table,sep="\t",inferSchema=True, header=True, dateFormat="yyyy-MM-dd")
        
    if table in ["CONCEPT","CONCEPT_RELATIONSHIP","DRUG_STRENGTH"]:
        df = df.withColumn('valid_start_date', f.to_date(f.col("valid_start_date").cast("string"),'yyyyMMdd')).withColumn('valid_end_date', f.to_date(f.col("valid_end_date").cast("string"),'yyyyMMdd'))
    
    df.write.format('delta').mode('overwrite').option('overwriteSchema','true').save("gs://optimized_data_lake/%s"%table)



