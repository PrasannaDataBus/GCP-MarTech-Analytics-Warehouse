#!/bin/bash
# ==================================================================================================
# 🧱 dbt (Data Build Tool) — Workflow & Best Practices Guide
# --------------------------------------------------------------------------------------------------
# This guide explains how to run transformations manually within your Airflow Docker environment.
#
# 🏗️ Architecture Context:
# - dbt is installed INSIDE the Docker Container (airflow-scheduler).
# - Source Code lives in WINDOWS (GCP MarTech .../transform).
# - They are connected via Docker Volume Mount (/opt/airflow/dbt_project).
#
# 🔑 Key Rule: You cannot run 'dbt' directly in PowerShell. You must "enter" the container first.
#
# Author: Prasanna
# Stack: BigQuery + dbt Core + Airflow (Docker)
# ==================================================================================================

# ==================================================================================================
# 🧭 Navigate to Your Airflow Folder
# --------------------------------------------------------------------------------------------------
# ✅ What It Does:
#    Moves you into your Airflow Docker project directory.
# 📅 When to Use:
#    Always before running any docker-compose commands.
# ⚠️ When NOT to Use:
#    If you’re already in the correct directory.
# ==================================================================================================
cd "C:/Users/prasa/Root/Airflow-Docker-Dev"

# ==================================================================================================
# 🚪 Step 1: Enter the Container (The "Magic Portal")
# --------------------------------------------------------------------------------------------------
# ✅ What It Does:
#    Opens a Linux Bash terminal INSIDE your running Airflow container.
#    This is where dbt is installed and where it can see your secrets.
#
# 📅 When to Use:
#    - Anytime you want to develop, test, or debug SQL models.
#    - Before running any 'dbt' commands.
#
# ⚠️ Common Mistake:
#    - Trying to run 'dbt run' in Windows PowerShell (will fail).
# ==================================================================================================

# For Development Environment:
docker exec -it airflow-scheduler-dev bash

# For Production Environment (Rarely used manually):
# docker exec -it airflow-scheduler-prod bash

# ==================================================================================================
# 📂 Step 2: Navigate to Project Folder
# --------------------------------------------------------------------------------------------------
# ✅ What It Does:
#    Moves you to the folder where your Windows code is mapped.
#
# 📅 When to Use:
#    Immediately after entering the container.
# ==================================================================================================
cd /opt/airflow/dbt_project

# ==================================================================================================
# 🔌 Step 3: Check Connection (The "Pulse Check")
# --------------------------------------------------------------------------------------------------
# ✅ What It Does:
#    Verifies dbt can read 'dbt_project.yml' and connect to BigQuery using 'profiles.yml'.
#
# 📅 When to Use:
#    - When setting up a new environment.
#    - If you changed your GCP JSON Key.
#    - If you changed 'profiles.yml'.
#
# 🔎 Success Output: "Connection test: [OK connection]"
# ==================================================================================================
dbt debug --profiles-dir /opt/airflow/secrets

# ==================================================================================================
# 🚀 Step 4: Run Models (The Transformation)
# --------------------------------------------------------------------------------------------------
# Option A: Run EVERYTHING (The "Big Bang")
# ⚠️ Warning: This rebuilds every table/view. Slow and costly.
# --------------------------------------------------------------------------------------------------
dbt run --profiles-dir /opt/airflow/secrets

# Option B: Run Specific Model (Recommended)
# ✅ Use this when working on a single file (e.g., stg_meta_ads).
# --------------------------------------------------------------------------------------------------
dbt run --select stg_meta_ads_performance --profiles-dir /opt/airflow/secrets

# Option C: Run a Whole Folder
# ✅ Use this to run all "Silver" or "Gold" models at once.
# --------------------------------------------------------------------------------------------------
dbt run --select models/staging --profiles-dir /opt/airflow/secrets

# ==================================================================================================
# 🧪 Step 5: Test Data Quality
# --------------------------------------------------------------------------------------------------
# ✅ What It Does:
#    Runs the tests defined in your YAML files (e.g., unique, not_null).
#    Ensures you don't have duplicate Ad IDs or missing Dates.
#
# 📅 When to Use:
#    - After creating a new model.
#    - Before pushing code to GitHub.
# ==================================================================================================
dbt test --select stg_meta_ads_performance --profiles-dir /opt/airflow/secrets

# ==================================================================================================
# 🔄 Step 6: Full Refresh (The "Hard Reset")
# --------------------------------------------------------------------------------------------------
# ✅ What It Does:
#    Drops the existing table/view in BigQuery and recreates it from scratch.
#
# 📅 When to Use:
#    - You changed a column data type (String -> Integer).
#    - You removed a column.
#    - You see "Schema mismatch" errors.
# ==================================================================================================
dbt run --full-refresh --select stg_meta_ads_performance --profiles-dir /opt/airflow/secrets

# ==================================================================================================
# 🌱 Step 7: Seeding (Loading Static Data)
# --------------------------------------------------------------------------------------------------
# ✅ What It Does:
#    Loads CSV files from the 'seeds' folder into BigQuery.
#    (e.g., Geotarget mapping, Currency codes).
#
# 📅 When to Use:
#    - When you add a new .csv file to the 'seeds' folder.
#    - It is rarely run daily.
# ==================================================================================================
dbt seed --profiles-dir /opt/airflow/secrets

# ==================================================================================================
# 🚪 Step 8: Exit the Container
# --------------------------------------------------------------------------------------------------
# ✅ What It Does:
#    Closes the connection to the Docker container and returns you to Windows.
#
# 📅 When to Use:
#    - When you are finished running dbt commands.
#    - Before you need to run 'docker-compose' commands (like restart).
# ==================================================================================================
exit

# ==================================================================================================
# 🧩 Common Errors & Solutions
# --------------------------------------------------------------------------------------------------
# Error                                | Cause                                    | Solution
# -------------------------------------|------------------------------------------|----------------------------------------
# 'dbt' command not found              | You are in Windows, not Docker           | Run 'docker exec -it ...' first
# Profile 'marketing' not found        | Wrong profile name or dir                | Use '--profiles-dir /opt/airflow/secrets'
# Unrecognized name: date_start        | Column name mismatch in Raw              | Check BigQuery schema vs SQL file
# Service Account permission denied    | JSON key has no BigQuery access          | Check IAM roles in GCP Console
# Database Error: ALREADY_EXISTS       | View vs Table conflict                   | Run with '--full-refresh'
# ==================================================================================================

# ==================================================================================================
# 🧠 The Daily "Muscle Memory" Workflow
# --------------------------------------------------------------------------------------------------
# 1. Open Terminal in 'Airflow-Docker-Dev'
# 2. Enter Container:
#    > docker exec -it airflow-scheduler-dev bash
#
# 3. Go to Project:
#    > cd /opt/airflow/dbt_project
#
# 4. Run your specific model:
#    > dbt run --select stg_google_ads_performance --profiles-dir /opt/airflow/secrets
#
# 5. Check BigQuery Console for results.
# 6. Exit:
#    > exit
# ==================================================================================================