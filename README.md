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
The GitHub Actions workflow (`.github/workflows/deploy.yml`) checks Python syntax on pull requests targeting `main` and syncs every pushed branch to Databricks. `main` updates the production Git folder; each other branch gets its own Git folder so it cannot replace production. Pull requests do not deploy to Databricks.

### Required GitHub Secrets
Set these in your GitHub repo: **Settings → Secrets and variables → Actions → New repository secret**

| Secret | Description | Example |
|--------|-------------|---------|
| `DATABRICKS_HOST` | Your Databricks workspace URL | `https://<workspace>.cloud.databricks.com` |
| `DATABRICKS_TOKEN` | Databricks personal access token | `dapi...` |
| `DATABRICKS_REPO_ID` | ID of the Git folder linked to this GitHub repository | `123456789` |
| `DATABRICKS_USER` | Databricks username/email, used only by the manual import workflow | `user@example.com` |

Before the first automatic deployment, link this GitHub repository to a Git folder in Databricks and configure your Git provider credentials in Databricks. Get the folder ID with `databricks repos list` or from the Databricks workspace, then save it as `DATABRICKS_REPO_ID`.

### How the pipeline works
1. Push any branch to GitHub.
2. GitHub Actions checks Python syntax and syncs the branch to Databricks. `main` uses the production folder; other branches use separate folders under `/Repos/Nyiko-git/`.
3. Open a pull request targeting `main` to run validation before merging.

The separate `deploy-to-databricks.yml` workflow remains available for manually importing Python files into the Databricks workspace.

## Adding More Notebooks
1. Create a new `.py` file in this repo (or a subdirectory)
2. Push the change to a branch and open a pull request to `main`
3. Merge the pull request after checks pass
4. The branch's Databricks Git folder updates on push; after merge, the production folder updates from `main`

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
