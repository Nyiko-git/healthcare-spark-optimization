# Databricks notebook source
# DBTITLE 1,1. Create Database/Schema
# ============================================================
# HEALTHCARE SPARK OPTIMIZATION PROJECT
# Database / Schema
# ============================================================

spark.sql("CREATE DATABASE IF NOT EXISTS spark_optimization")

spark.sql("USE spark_optimization")

# COMMAND ----------

# DBTITLE 1,2. Configuration
# ============================================================
# DATA VOLUME CONFIGURATION
# ============================================================

NUM_PATIENTS = 2_000_000
NUM_ENCOUNTERS = 30_000_000
NUM_DOCTORS = 20_000
NUM_HOSPITALS = 500
NUM_DIAGNOSES = 5_000

# Number of partitions used when initially generating data
PATIENT_PARTITIONS = 100
ENCOUNTER_PARTITIONS = 300
DOCTOR_PARTITIONS = 20
HOSPITAL_PARTITIONS = 10
DIAGNOSIS_PARTITIONS = 20

# COMMAND ----------

# DBTITLE 1,3. Patients Dataset
from pyspark.sql import functions as F
from pyspark.sql.types import *

# ============================================================
# PATIENTS
# ============================================================

patients = (
    spark.range(
        0,
        NUM_PATIENTS,
        1,
        PATIENT_PARTITIONS
    )
    .withColumnRenamed("id", "patient_id")

    .withColumn(
        "gender",
        F.when(
            F.pmod(F.col("patient_id"), 2) == 0,
            "Male"
        ).otherwise("Female")
    )

    .withColumn(
        "province",
        F.element_at(
            F.array(
                F.lit("Gauteng"),
                F.lit("KwaZulu-Natal"),
                F.lit("Western Cape"),
                F.lit("Eastern Cape"),
                F.lit("Limpopo"),
                F.lit("Mpumalanga"),
                F.lit("North West"),
                F.lit("Free State"),
                F.lit("Northern Cape")
            ),
            (F.pmod(F.col("patient_id"), 9) + 1).cast("int")
        )
    )

    .withColumn(
        "date_of_birth",
        F.date_sub(
            F.current_date(),
            (F.pmod(F.col("patient_id"), 25_000) + 7_000).cast("int")
        )
    )

    .withColumn(
        "registration_date",
        F.date_sub(
            F.current_date(),
            F.pmod(F.col("patient_id"), 3_000).cast("int")
        )
    )
)

# COMMAND ----------

# DBTITLE 1,3.1 Inspect Patients
patients.printSchema()
patients.show(10)

# COMMAND ----------

# DBTITLE 1,3.2 Write Patients Table
patients.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("patients")

# COMMAND ----------

# DBTITLE 1,4. Hospitals Dataset
# ============================================================
# HOSPITALS
# ============================================================

hospitals = (
    spark.range(
        1,
        NUM_HOSPITALS + 1,
        1,
        HOSPITAL_PARTITIONS
    )
    .withColumnRenamed("id", "hospital_id")

    .withColumn(
        "hospital_name",
        F.concat(
            F.lit("Hospital_"),
            F.col("hospital_id")
        )
    )

    .withColumn(
        "province",
        F.element_at(
            F.array(
                F.lit("Gauteng"),
                F.lit("KwaZulu-Natal"),
                F.lit("Western Cape"),
                F.lit("Eastern Cape"),
                F.lit("Limpopo"),
                F.lit("Mpumalanga"),
                F.lit("North West"),
                F.lit("Free State"),
                F.lit("Northern Cape")
            ),
            (F.pmod(F.col("hospital_id"), 9) + 1).cast("int")
        )
    )

    .withColumn(
        "hospital_type",
        F.element_at(
            F.array(
                F.lit("Public"),
                F.lit("Private"),
                F.lit("Academic")
            ),
            (F.pmod(F.col("hospital_id"), 3) + 1).cast("int")
        )
    )
)

# COMMAND ----------

# DBTITLE 1,4.1 Write Hospitals Table
hospitals.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("hospitals")

# COMMAND ----------

# DBTITLE 1,5. Doctors Dataset
# ============================================================
# DOCTORS
# ============================================================

specialities = [
    "General Practitioner",
    "Cardiology",
    "Oncology",
    "Neurology",
    "Paediatrics",
    "Orthopaedics",
    "Dermatology",
    "Psychiatry",
    "Radiology",
    "Emergency Medicine"
]

speciality_expr = F.element_at(
    F.array(*[F.lit(x) for x in specialities]),
    (F.pmod(F.col("doctor_id"), len(specialities)) + 1).cast("int")
)

doctors = (
    spark.range(
        1,
        NUM_DOCTORS + 1,
        1,
        DOCTOR_PARTITIONS
    )
    .withColumnRenamed("id", "doctor_id")

    .withColumn(
        "doctor_name",
        F.concat(
            F.lit("Doctor_"),
            F.col("doctor_id")
        )
    )

    .withColumn(
        "speciality",
        speciality_expr
    )

    .withColumn(
        "hospital_id",
        F.pmod(
            F.col("doctor_id"),
            F.lit(NUM_HOSPITALS)
        ) + 1
    )
)

# COMMAND ----------

# DBTITLE 1,5.1 Write Doctors Table
doctors.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("doctors")

# COMMAND ----------

# DBTITLE 1,6. Diagnoses Dataset
# ============================================================
# DIAGNOSES
# ============================================================

diagnosis_categories = [
    "Cardiovascular",
    "Respiratory",
    "Neurological",
    "Infectious",
    "Oncology",
    "Orthopaedic",
    "Dermatological",
    "Gastrointestinal",
    "Endocrine",
    "General"
]

diagnoses = (
    spark.range(
        1,
        NUM_DIAGNOSES + 1,
        1,
        DIAGNOSIS_PARTITIONS
    )
    .withColumnRenamed("id", "diagnosis_id")

    .withColumn(
        "diagnosis_code",
        F.concat(
            F.lit("DX"),
            F.lpad(
                F.col("diagnosis_id").cast("string"),
                5,
                "0"
            )
        )
    )

    .withColumn(
        "diagnosis_description",
        F.concat(
            F.lit("Diagnosis_"),
            F.col("diagnosis_id")
        )
    )

    .withColumn(
        "category",
        F.element_at(
            F.array(*[F.lit(x) for x in diagnosis_categories]),
            (F.pmod(F.col("diagnosis_id"), len(diagnosis_categories)) + 1).cast("int")
        )
    )
)

# COMMAND ----------

# DBTITLE 1,6.1 Write Diagnoses Table
diagnoses.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("diagnoses")

# COMMAND ----------

# DBTITLE 1,7. Encounters Dataset (Deliberate Skew)
# ============================================================
# ENCOUNTERS
# ============================================================

encounters = (
    spark.range(
        0,
        NUM_ENCOUNTERS,
        1,
        ENCOUNTER_PARTITIONS
    )
    .withColumnRenamed("id", "encounter_id")

    # --------------------------------------------------------
    # Patient
    # --------------------------------------------------------
    .withColumn(
        "patient_id",
        F.pmod(
            F.col("encounter_id") * 17,
            F.lit(NUM_PATIENTS)
        )
    )

    # --------------------------------------------------------
    # DATE
    # --------------------------------------------------------
    .withColumn(
        "encounter_date",
        F.date_sub(
            F.current_date(),
            F.pmod(
                F.col("encounter_id"),
                1_000
            ).cast("int")
        )
    )

    # --------------------------------------------------------
    # Encounter type
    # --------------------------------------------------------
    .withColumn(
        "encounter_type",
        F.element_at(
            F.array(
                F.lit("Outpatient"),
                F.lit("Inpatient"),
                F.lit("Emergency"),
                F.lit("Follow-up")
            ),
            (F.pmod(F.col("encounter_id"), 4) + 1).cast("int")
        )
    )

    # --------------------------------------------------------
    # Doctor
    # --------------------------------------------------------
    .withColumn(
        "doctor_id",
        F.pmod(
            F.col("encounter_id") * 13,
            F.lit(NUM_DOCTORS)
        ) + 1
    )

    # --------------------------------------------------------
    # HOSPITAL - DELIBERATE SKEW
    # --------------------------------------------------------
    .withColumn(
        "hospital_id",
        F.when(
            F.pmod(F.col("encounter_id"), 100) < 55,
            F.lit(1)
        )
        .otherwise(
            F.pmod(
                F.col("encounter_id"),
                F.lit(NUM_HOSPITALS - 1)
            ) + 2
        )
    )

    # --------------------------------------------------------
    # Diagnosis
    # --------------------------------------------------------
    .withColumn(
        "diagnosis_code",
        F.concat(
            F.lit("DX"),
            F.lpad(
                (
                    F.pmod(
                        F.col("encounter_id") * 7,
                        F.lit(NUM_DIAGNOSES)
                    ) + 1
                ).cast("string"),
                5,
                "0"
            )
        )
    )

    # --------------------------------------------------------
    # Status
    # --------------------------------------------------------
    .withColumn(
        "status",
        F.element_at(
            F.array(
                F.lit("Completed"),
                F.lit("Cancelled"),
                F.lit("In Progress")
            ),
            (F.pmod(F.col("encounter_id"), 3) + 1).cast("int")
        )
    )
)

# COMMAND ----------

# DBTITLE 1,8. Verify Data Skew
encounters.groupBy("hospital_id") \
    .count() \
    .orderBy(F.desc("count")) \
    .show(20)

# COMMAND ----------

# DBTITLE 1,9. Write Encounters Table
encounters.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("encounters")

# COMMAND ----------

# DBTITLE 1,10. Check Tables
spark.sql("""
SHOW TABLES IN spark_optimization
""").show()

# COMMAND ----------

# DBTITLE 1,11. Check Dataset Sizes
for table in [
    "patients",
    "encounters",
    "doctors",
    "hospitals",
    "diagnoses"
]:
    print(f"\n===== {table} =====")
    spark.table(table).count()

# COMMAND ----------

# DBTITLE 1,12. Load Tables for Business Query
patients_df = spark.table("patients")
encounters_df = spark.table("encounters")
doctors_df = spark.table("doctors")
hospitals_df = spark.table("hospitals")
diagnoses_df = spark.table("diagnoses")

# COMMAND ----------

# DBTITLE 1,13. Version 1 — Deliberately Inefficient
# ============================================================
# VERSION 1 — DELIBERATELY INEFFICIENT
# ============================================================

result_v1 = (
    encounters_df

    .join(
        patients_df,
        "patient_id"
    )

    .withColumnRenamed("province", "patient_province")

    .join(
        doctors_df,
        "doctor_id"
    )

    .join(
        hospitals_df,
        "hospital_id"
    )

    .join(
        diagnoses_df,
        "diagnosis_code"
    )

    .filter(
        F.col("encounter_date") >= "2025-01-01"
    )

    .groupBy(
        "province",
        "hospital_name",
        "speciality",
        "diagnosis_description",
        "category"
    )

    .count()

    .orderBy(
        "province",
        "hospital_name"
    )
)

# COMMAND ----------

# DBTITLE 1,13.1 V1 — Explain and Show
result_v1.explain("formatted")

result_v1.show()

# COMMAND ----------

# DBTITLE 1,BAD QUERY 1 — Select All After All Joins
# ============================================================
# BAD QUERY 1
# Problem: Joins all tables then selects everything — no filtering, no pruning
# ============================================================

result = (
    encounters_df
    .join(patients_df, "patient_id")
    .join(doctors_df, "doctor_id")
    .join(hospitals_df, "hospital_id")
    .join(diagnoses_df, "diagnosis_code")
    .select("*")
)

# COMMAND ----------

# DBTITLE 1,BAD QUERY 2 — Filter Late After Joins
# ============================================================
# BAD QUERY 2
# Problem: Filter is applied AFTER all joins instead of before
# ============================================================

result = (
    encounters_df
    .join(patients_df, "patient_id")
    .join(doctors_df, "doctor_id")
    .join(hospitals_df, "hospital_id")
    .filter(
        F.col("encounter_date") >= "2026-01-01"
    )
)

# COMMAND ----------

# DBTITLE 1,BAD QUERY 3 — Excessive Repartitioning
# ============================================================
# BAD QUERY 3
# Problem: Unnecessary repartitions between every join — each one triggers a full shuffle
# ============================================================

df = encounters_df

df = df.repartition(500)

df = df.join(patients_df, "patient_id")

df = df.repartition(300)

df = df.join(doctors_df, "doctor_id")

df = df.repartition(200)

df = df.groupBy("hospital_id").count()

# COMMAND ----------

# DBTITLE 1,BAD QUERY 4 — Repartition Before Filter
# ============================================================
# BAD QUERY 4
# Problem: Repartitions the entire 30M-row dataset BEFORE filtering
# ============================================================

df = (
    encounters_df
    .repartition(500)
    .filter("status = 'Completed'")
)

# COMMAND ----------

# DBTITLE 1,BAD QUERY 5 — Distinct on Entire Dataset
# ============================================================
# BAD QUERY 5
# Problem: Calls distinct() on the entire 30M-row encounters table — full shuffle across all columns
# ============================================================

unique_encounters = encounters_df.distinct()

# COMMAND ----------

# DBTITLE 1,BAD QUERY 6 — Drop Duplicates on All Columns
# ============================================================
# BAD QUERY 6
# Problem: dropDuplicates on nearly all columns — expensive shuffle for no reason
# ============================================================

df = encounters_df.dropDuplicates([
    "encounter_id",
    "patient_id",
    "encounter_date",
    "encounter_type",
    "doctor_id",
    "hospital_id",
    "diagnosis_code",
    "status"
])

# COMMAND ----------

# DBTITLE 1,BAD QUERY 7 — Global Sort on Large Dataset
# ============================================================
# BAD QUERY 7
# Problem: Global orderBy on the entire 30M-row dataset — massive shuffle
# ============================================================

result = (
    encounters_df
    .orderBy("encounter_date")
)

# COMMAND ----------

# DBTITLE 1,BAD QUERY 8 — Collect Entire Dataset to Driver
# ============================================================
# BAD QUERY 8
# Problem: Collects 30M rows to the driver — will cause OOM
# ============================================================

data = encounters_df.collect()

# COMMAND ----------

# DBTITLE 1,BAD QUERY 9 — Convert to Pandas
# ============================================================
# BAD QUERY 9
# Problem: Converts 30M-row distributed dataset to a single Pandas DataFrame — driver OOM
# ============================================================

pdf = encounters_df.toPandas()

# COMMAND ----------

# DBTITLE 1,BAD QUERY 10 — Multiple Full Scans
# ============================================================
# BAD QUERY 10
# Problem: Scans the entire 30M-row table four separate times for each count
# ============================================================

print(encounters_df.count())
print(encounters_df.filter("status = 'Completed'").count())
print(encounters_df.filter("status = 'Cancelled'").count())
print(encounters_df.filter("status = 'In Progress'").count())

# COMMAND ----------

# DBTITLE 1,BAD QUERY 11 — Cache All Tables Unnecessarily
# ============================================================
# BAD QUERY 11
# Problem: Caches ALL tables including the 30M-row encounters table — wastes memory
# ============================================================

patients_df.cache()
encounters_df.cache()
doctors_df.cache()
hospitals_df.cache()
diagnoses_df.cache()

# COMMAND ----------

# DBTITLE 1,BAD QUERY 12 — Three Separate GroupBys
# ============================================================
# BAD QUERY 12
# Problem: Three separate groupBy operations on the same filtered data — three shuffles instead of one
# ============================================================

completed = encounters_df.filter(
    "status = 'Completed'"
)

result1 = completed.groupBy("hospital_id").count()

result2 = completed.groupBy("doctor_id").count()

result3 = completed.groupBy("diagnosis_code").count()

# COMMAND ----------

# DBTITLE 1,BAD QUERY 13 — Sort Merge Join on Small Table
# ============================================================
# BAD QUERY 13
# Problem: Joins 30M encounters with 500-row hospitals table using default sort-merge join instead of broadcast
# ============================================================

result = encounters_df.join(
    hospitals_df,
    "hospital_id"
)

# COMMAND ----------

# DBTITLE 1,BAD QUERY 14 — Filter and Select Late
# ============================================================
# BAD QUERY 14
# Problem: All four joins happen first, then filter and select — no predicate pushdown or column pruning
# ============================================================

result = (
    encounters_df
    .join(patients_df, "patient_id")
    .join(doctors_df, "doctor_id")
    .join(hospitals_df, "hospital_id")
    .join(diagnoses_df, "diagnosis_code")
    .filter("status = 'Completed'")
    .select(
        "province",
        "hospital_name",
        "speciality",
        "category"
    )
)

# COMMAND ----------

# DBTITLE 1,BAD QUERY 15 — GroupBy on All Columns
# ============================================================
# BAD QUERY 15
# Problem: GroupBy on nearly every column of the 30M-row table — massive shuffle with huge key cardinality
# ============================================================

result = (
    encounters_df
    .groupBy(
        "patient_id",
        "doctor_id",
        "hospital_id",
        "diagnosis_code",
        "encounter_type",
        "status",
        "encounter_date"
    )
    .count()
)

# COMMAND ----------

# DBTITLE 1,BAD QUERY 16 — Repartition by Column Only
# ============================================================
# BAD QUERY 16
# Problem: Repartitions by hospital_id which is heavily skewed — hospital_id=1 has ~55% of all rows
# ============================================================

df = encounters_df.repartition(
    "hospital_id"
)

# COMMAND ----------

# DBTITLE 1,BAD QUERY 17 — GroupBy with Skew
# ============================================================
# BAD QUERY 17
# Problem: GroupBy on skewed hospital_id column — one task handles ~55% of 30M rows
# ============================================================

result = (
    encounters_df
    .groupBy("hospital_id")
    .count()
)

# COMMAND ----------

# DBTITLE 1,BAD QUERY 30 — Everything Wrong at Once
# ============================================================
# BAD QUERY 30
# Problem: Combines excessive repartitioning, late filtering, dropDuplicates, and unnecessary joins
# ============================================================

spark.conf.set(
    "spark.sql.shuffle.partitions",
    "5000"
)

result = (
    encounters_df
    .repartition(1000)

    .join(
        patients_df,
        "patient_id"
    )

    .repartition(800)

    .join(
        doctors_df,
        "doctor_id"
    )

    .repartition(600)

    .join(
        hospitals_df,
        "hospital_id"
    )

    .repartition(500)

    .join(
        diagnoses_df,
        "diagnosis_code"
    )

    .filter(
        F.col("encounter_date") >= "2025-01-01"
    )

    .dropDuplicates()

    .groupBy(
        "province",
        "hospital_name",
        "doctor_name",
        "speciality",
        "diagnosis_description",
        "category"
    )

    .count()

    .orderBy(
        F.desc("count")
    )
)

# COMMAND ----------

# DBTITLE 1,CI/CD Pipeline Notes
# MAGIC %md
# MAGIC # CI/CD Pipeline — How to Add More Notebooks to GitHub
# MAGIC
# MAGIC ## Where is the pipeline code?
# MAGIC
# MAGIC The CI/CD pipeline lives in your GitHub repo:
# MAGIC
# MAGIC ```
# MAGIC GitHub Repo: https://github.com/Nyiko-git/healthcare-spark-optimization
# MAGIC
# MAGIC Pipeline file: .github/workflows/deploy-to-databricks.yml
# MAGIC README:       README.md
# MAGIC ```
# MAGIC
# MAGIC The Databricks Git folder is at:
# MAGIC ```
# MAGIC /Repos/Nyiko-git/healthcare-spark-optimization
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## How the CI/CD pipeline works
# MAGIC
# MAGIC 1. You push code to the `main` branch on GitHub
# MAGIC 2. GitHub Actions triggers automatically
# MAGIC 3. The Databricks CLI deploys all `.py` notebooks to your workspace
# MAGIC 4. Notebooks appear under `/Users/<your-user>/healthcare-spark-optimization/`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Required GitHub Secrets
# MAGIC
# MAGIC Set these in GitHub: **Settings → Secrets and variables → Actions → New repository secret**
# MAGIC
# MAGIC | Secret | Description |
# MAGIC |--------|-------------|
# MAGIC | `DATABRICKS_HOST` | Your Databricks workspace URL (e.g. `https://<workspace>.cloud.databricks.com`) |
# MAGIC | `DATABRICKS_TOKEN` | Databricks personal access token |
# MAGIC | `DATABRICKS_USER` | Your Databricks username/email |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Step-by-step: Add a new notebook to the repo
# MAGIC
# MAGIC 1. **Create the notebook** in the Databricks Git folder
# MAGIC    - Go to `/Repos/Nyiko-git/healthcare-spark-optimization/` in Databricks
# MAGIC    - Create a new notebook (e.g. `optimized_v2.py`)
# MAGIC
# MAGIC 2. **Write your code** in the notebook
# MAGIC
# MAGIC 3. **Commit and push** from Databricks:
# MAGIC    - Click the Git icon in the notebook toolbar
# MAGIC    - Enter a commit message
# MAGIC    - Click **Commit and push**
# MAGIC
# MAGIC 4. **GitHub Actions runs automatically**:
# MAGIC    - The workflow detects the new `.py` file
# MAGIC    - It deploys the notebook to your Databricks workspace
# MAGIC    - Check the Actions tab on GitHub for run status
# MAGIC
# MAGIC 5. **Verify** in Databricks:
# MAGIC    - Check `/Users/<your-user>/healthcare-spark-optimization/`
# MAGIC    - Your new notebook should be there
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Alternative: Add via Python code
# MAGIC
# MAGIC ```python
# MAGIC # From a Databricks notebook cell:
# MAGIC import os
# MAGIC
# MAGIC # Write a new notebook file in the Git folder
# MAGIC with open('/Workspace/Repos/Nyiko-git/healthcare-spark-optimization/optimized_v2.py', 'w') as f:
# MAGIC     f.write('# Databricks notebook source\n')
# MAGIC     f.write('# COMMAND ----------\n')
# MAGIC     f.write('# Your optimized V2 code here\n')
# MAGIC     f.write("print('V2 optimized query')\n")
# MAGIC ```
# MAGIC
# MAGIC Then commit and push using the Git panel in Databricks.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Directory structure in the repo
# MAGIC
# MAGIC ```
# MAGIC healthcare-spark-optimization/
# MAGIC ├── .github/
# MAGIC │   └── workflows/
# MAGIC │       └── deploy-to-databricks.yml    # CI/CD pipeline
# MAGIC ├── healthcare_spark_optimization_bad_queries.py   # Main notebook (bad queries)
# MAGIC ├── optimized_v2.py                     # (example: future optimized versions)
# MAGIC ├── optimized_v3.py                     # (example: column pruning)
# MAGIC ├── optimized_v4.py                     # (example: broadcast joins)
# MAGIC └── README.md                           # Project documentation
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Tips
# MAGIC
# MAGIC - The pipeline auto-deploys **any `.py` file** in the repo (excluding `.github/`)
# MAGIC - You can organize notebooks in subdirectories — the pipeline preserves the structure
# MAGIC - The pipeline only runs on pushes to `main` — feature branches won't trigger deployment
# MAGIC - You can also trigger the workflow manually from GitHub's Actions tab (`workflow_dispatch`)