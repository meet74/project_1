# Databricks notebook source
# MAGIC %run "./config"

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df = spark.read.option("header",True).parquet('/mnt/healthstorageaccount/health-project/processed/cancer/')

# COMMAND ----------

display(df)

# COMMAND ----------

df_agg_bmi_sleep = df.groupBy('patient_state').agg(round(avg('bmi')).alias("avg_bmi"),sum("sleep_hours").alias("total_sleep_hours"))
display(df_agg_bmi_sleep)


# COMMAND ----------

df_agg_hearattack_agegroup = df.groupBy("age_category").agg(count(when(col('had_heart_attack')=='Yes',1)).alias("heart_attack_count")).orderBy('age_category')
display(df_agg_hearattack_agegroup)

# COMMAND ----------

df_agg_physhealth_gender = df.groupBy('patient_gender').agg(sum('physical_health_days').alias("total_physical_health_days"))
display(df_agg_physhealth_gender)

# COMMAND ----------

df_agg_mentalhealth_gender = df.groupBy('patient_gender').agg(sum('mental_health_days').alias("total_mental_health_days"))
display(df_agg_mentalhealth_gender)

# COMMAND ----------

df_agg_bmi_sleep.write.mode('overwrite').parquet('/mnt/healthstorageaccount/health-project/transformed/Agg_bmi_sleep')
df_agg_physhealth_gender.write.mode('overwrite').parquet('/mnt/healthstorageaccount/health-project/transformed/Agg_physhealth_gender')
df_agg_mentalhealth_gender.write.mode('overwrite').parquet('/mnt/healthstorageaccount/health-project/transformed/Agg_mentalhealth_gender')
df_agg_hearattack_agegroup.write.mode('overwrite').parquet('/mnt/healthstorageaccount/health-project/transformed/Agg_hearattack_agegroup')

# COMMAND ----------

df.write.mode('overwrite').partitionBy('patient_state').parquet('/mnt/healthstorageaccount/health-project/transformed/Patient_State_Data')
df.write.mode('overwrite').partitionBy('patient_gender').parquet('/mnt/healthstorageaccount/health-project/transformed/Patient_Gender_Data')
df.write.mode('overwrite').partitionBy('age_category').parquet('/mnt/healthstorageaccount/health-project/transformed/Age_Category_Data')
df.write.mode('overwrite').partitionBy('had_heart_attack').parquet('/mnt/healthstorageaccount/health-project/transformed/HeartAttack_Data')


# COMMAND ----------


