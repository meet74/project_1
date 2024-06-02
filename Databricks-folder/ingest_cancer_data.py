# Databricks notebook source
# MAGIC %run "./config"

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

raw_cancer_data = f"{base_url}/raw/cancer_data.csv"

# COMMAND ----------

df = spark.read.option("header","True").option("inferSchema",True).csv(raw_cancer_data)

# COMMAND ----------

# Renaming Columns to follow a consistent naming convention
updated_column_names = df.withColumnRenamed('State', 'patient_state')\
       .withColumnRenamed('Sex', 'patient_gender')\
       .withColumnRenamed('GeneralHealth', 'general_health')\
       .withColumnRenamed('PhysicalHealthDays', 'physical_health_days')\
       .withColumnRenamed('MentalHealthDays', 'mental_health_days')\
       .withColumnRenamed('LastCheckupTime', 'last_checkup_time')\
       .withColumnRenamed('PhysicalActivities', 'physical_activities')\
       .withColumnRenamed('SleepHours', 'sleep_hours')\
       .withColumnRenamed('RemovedTeeth', 'removed_teeth')\
       .withColumnRenamed('HadHeartAttack', 'had_heart_attack')\
       .withColumnRenamed('HadAngina', 'had_angina')\
       .withColumnRenamed('HadStroke', 'had_stroke')\
       .withColumnRenamed('HadAsthma', 'had_asthma')\
       .withColumnRenamed('HadSkinCancer', 'had_skin_cancer')\
       .withColumnRenamed('HadCOPD', 'had_copd')\
       .withColumnRenamed('HadDepressiveDisorder', 'had_depressive_disorder')\
       .withColumnRenamed('HadKidneyDisease', 'had_kidney_disease')\
       .withColumnRenamed('HadArthritis', 'had_arthritis')\
       .withColumnRenamed('HadDiabetes', 'had_diabetes')\
       .withColumnRenamed('DeafOrHardOfHearing', 'deaf_or_hard_of_hearing')\
       .withColumnRenamed('BlindOrVisionDifficulty', 'blind_or_vision_difficulty')\
       .withColumnRenamed('DifficultyConcentrating', 'difficulty_concentrating')\
       .withColumnRenamed('DifficultyWalking', 'difficulty_walking')\
       .withColumnRenamed('DifficultyDressingBathing', 'difficulty_dressing_bathing')\
       .withColumnRenamed('DifficultyErrands', 'difficulty_errands')\
       .withColumnRenamed('SmokerStatus', 'smoker_status')\
       .withColumnRenamed('ECigaretteUsage', 'ecigarette_usage')\
       .withColumnRenamed('ChestScan', 'chest_scan')\
       .withColumnRenamed('RaceEthnicityCategory', 'race_ethnicity_category')\
       .withColumnRenamed('AgeCategory', 'age_category')\
       .withColumnRenamed('HeightInMeters', 'height_in_meters')\
       .withColumnRenamed('WeightInKilograms', 'weight_in_kilograms')\
       .withColumnRenamed('BMI', 'bmi')\
       .withColumnRenamed('AlcoholDrinkers', 'alcohol_drinkers')\
       .withColumnRenamed('HIVTesting', 'hiv_testing')\
       .withColumnRenamed('FluVaxLast12', 'flu_vax_last_12')\
       .withColumnRenamed('PneumoVaxEver', 'pneumo_vax_ever')\
       .withColumnRenamed('TetanusLast10Tdap', 'tetanus_last_10_tdap')\
       .withColumnRenamed('HighRiskLastYear', 'high_risk_last_year')\
       .withColumnRenamed('CovidPos', 'covid_pos')

# COMMAND ----------

display(updated_column_names)

# COMMAND ----------

#Removing Missing Values

mode_gender = updated_column_names.groupBy('patient_gender').count().orderBy(desc("count")).first()[0]
mode_state = updated_column_names.groupBy('patient_state').count().orderBy(desc("count")).first()[0]
mode_general_health = updated_column_names.groupBy('general_health').count().orderBy(desc("count")).first()[0]
mode_race_ethnicity_category = updated_column_names.groupBy('race_ethnicity_category').count().orderBy(desc("count")).first()[0]
mode_race_age_category = updated_column_names.groupBy('age_category').count().orderBy(desc("count")).first()[0]

mean_height = updated_column_names.select(round(mean(col('height_in_meters')))).first()[0]
mean_weight = updated_column_names.select(round(mean(col('weight_in_kilograms')))).first()[0]
mean_bmi = updated_column_names.select(round(mean(col('bmi')))).first()[0]

removed_missing_data = updated_column_names.fillna({'patient_state':mode_state,'patient_gender':mode_gender,'general_health':mode_general_health,'physical_health_days':0,'mental_health_days':0,'last_checkup_time':'Unknown','physical_activities':'No','sleep_hours':7,'had_heart_attack':'No','had_angina':'No','had_stroke':'No','had_asthma':'No','had_diabetes':'No','had_skin_cancer':'No','had_copd':'No','had_depressive_disorder':'No','had_kidney_disease':'No','had_arthritis':'No','blind_or_vision_difficulty':'No','difficulty_concentrating':'No','difficulty_walking':'No','difficulty_dressing_bathing':'No','difficulty_errands':'No','chest_scan':'No','alcohol_drinkers':'No','hiv_testing':'No','flu_vax_last_12':'No','pneumo_vax_ever':'No','high_risk_last_year':'No','covid_pos':'No','smoker_status':'Never smoked','ecigarette_usage':'Never used e-cigarettes in my entire life','race_ethnicity_category':mode_race_ethnicity_category,'age_category':mode_race_age_category,'height_in_meters':mean_height,'weight_in_kilograms':mean_weight,'tetanus_last_10_tdap':'Unknown','deaf_or_hard_of_hearing':'No','bmi':mean_bmi})

# COMMAND ----------

display(removed_missing_data.groupBy("age_category").agg(count(when(col('had_heart_attack')=='Yes',1)).alias("heart_attack_count")))

# COMMAND ----------

removed_column_data = removed_missing_data.drop(col('removed_teeth'))

# COMMAND ----------

display(removed_column_data)

# COMMAND ----------

# Check for logical consistency in AgeCategory
valid_age_categories = ["Age 80 or older", "Age 75 to 79", "Age 70 to 74", "Age 65 to 69",
                        "Age 60 to 64", "Age 55 to 59", "Age 50 to 54", "Age 45 to 49",
                        "Age 40 to 44", "Age 35 to 39", "Age 30 to 34", "Age 25 to 29",
                        "Age 20 to 24", "Age 18 to 20"]
consistent_data = removed_column_data.withColumn('age_category',when(col('age_category').isin(valid_age_categories),col('age_category')).otherwise(mode_race_age_category)).withColumn('bmi',when((col('bmi') >= 10)& (col('bmi')<=60),col('bmi')).otherwise(mean_bmi) )

# COMMAND ----------

final_data = consistent_data.withColumn('ingested_data',current_timestamp())

# COMMAND ----------

display(final_data.groupBy("age_category").agg(count(when(col('had_heart_attack')=='Yes',1)).alias("heart_attack_count")))

# COMMAND ----------

final_data.write.mode('overwrite').parquet('/mnt/healthstorageaccount/health-project/processed/cancer')

# COMMAND ----------


