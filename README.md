# Healthcare Spark Optimization Project

## Overview
This project contains Databricks notebooks for learning Spark optimization techniques using a healthcare dataset with deliberate data skew.

## Dataset
- **2M** patients
- **30M** encounters (with 55% skewed to hospital_id=1)
- **20K** doctors
- **500** hospitals
- **5K** diagnoses

## Structure
1. **Setup**: Database creation and data generation
2. **V1 Baseline**: Deliberately inefficient query
3. **Bad Queries (1-17, 30)**: 18 anti-pattern examples for optimization practice

## CI/CD Pipeline
The included GitHub Actions workflow (`.github/workflows/deploy-to-databricks.yml`) automatically deploys all notebooks to your Databricks workspace on every push to `main`.

### Required GitHub Secrets
Set these in your GitHub repo: **Settings → Secrets and variables → Actions → New repository secret**

| Secret | Description | Example |
|--------|-------------|---------|
| `DATABRICKS_HOST` | Your Databricks workspace URL | `https://<workspace>.cloud.databricks.com` |
| `DATABRICKS_TOKEN` | Databricks personal access token | `dapi...` |
| `DATABRICKS_USER` | Your Databricks username/email | `user@example.com` |

### How the pipeline works
1. You push code to the `main` branch
2. GitHub Actions triggers the workflow
3. The Databricks CLI deploys all `.py` notebooks to `/Users/<user>/healthcare-spark-optimization/`
4. Verify in Databricks under that workspace path

## Adding More Notebooks
1. Create a new `.py` file in this repo (or a subdirectory)
2. Commit and push to `main`
3. The CI/CD pipeline auto-deploys it on the next push
4. The notebook appears under `/Users/<user>/healthcare-spark-optimization/` in Databricks

## Optimization Techniques Covered
| Version | Technique |
|---------|-----------|
| V1 | Baseline (deliberately bad) |
| V2 | Filter early |
| V3 | Column pruning |
| V4 | Broadcast small dimensions |
| V5 | Tune shuffle partitions |
| V6 | Address data skew (salting) |
| V7 | AQE (Adaptive Query Execution) |
| V8 | Caching where appropriate |
| V9 | Optimize output layout |
