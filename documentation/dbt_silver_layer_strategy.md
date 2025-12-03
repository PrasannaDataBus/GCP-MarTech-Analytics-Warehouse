📘 Architecture Doc: The Silver Layer (Staging)

**1. The Goal of the Silver Layer**

The Silver Layer (also called "Staging") is the Normalization Zone.

- Input: Raw, messy data from BigQuery (marketing_raw).


- Action: Renames columns, fixes data types (String $\to$ Integer), and generates IDs.


- Output: Clean, consistent data ready for joining (marketing_dev_silver).


- Rule: 1:1 Mapping. One Raw Table $\to$ One Silver View. No aggregations (SUM/COUNT) happen here.


**2. Deep Dive: Materialization (View vs. Table)**

This is the most critical architectural decision I made.

In dbt, Materialization defines how the data is stored in BigQuery. We configured this in dbt_project.yml:

YAML\
`
staging:
  +materialized: view  # <--- The Strategy
`

### 🆚 Comparison: View vs. Table

| Feature | 👓 View (Silver Strategy) | 📦 Table (Gold Strategy) |
| :--- | :--- | :--- |
| **What is it?** | A saved **SQL Query**. It is "Virtual." | A physical **file** containing data. |
| **Storage Cost** | **$0** (Free). | **$$** (You pay for GBs stored). |
| **Build Cost** | **$0** (Airflow pays nothing to run it). | **$$** (Pays for compute to calculate rows). |
| **Query Cost** | **$$** (Scanning Raw data every time you read). | **$** (Cheap/Free to scan small results). |
| **Freshness** | **Instant** (Always sees latest raw data). | **Static** (Only fresh as of last Airflow run). |
| **Best For...** | Light cleaning, renaming, type casting. | Heavy math, joins, dashboards. |



**💡 The "Recipe vs. Cake" Analogy**

- Silver (View) is a Recipe Card: It is just a set of instructions ("Take column 'spend', rename it to 'cost'"). It takes up no space in the kitchen. When you want the data, you "cook" it on the fly.


- Gold (Table) is a Baked Cake: You cooked it once. It sits on the counter. It is ready to eat immediately (fast), but if new ingredients arrive, the cake doesn't magically update. You have to bake a new one.

**3. Why we chose "Views" for Silver**

We selected `materialized='view'` for stg_google_ads and stg_meta_ads for three specific reasons:

**1. Zero Latency (CDC)**

- Your Airflow Python scripts load data into marketing_raw at 07:00 AM.


- Because Silver is a View, if you query it at 07:01 AM, you automatically see the new data.


- There is no need to "refresh" a view to see new rows. It creates a direct window into the raw data.

**2. Cost Efficiency (The "Lazy" Approach)**

- Marketing data is often huge. If we created a Physical Table for Silver, we would be paying to write that data twice (once in Raw, once in Silver).


- By using a View, we pay Storage costs only once (on the Raw layer).

**3. Agility**

- If you realize you mapped spend to the wrong column, you change the SQL and hit dbt run. The View updates in 1 second.


- If it were a Table, you would have to delete the old table and wait 10 minutes to reload 5 years of history (--full-refresh).

**4. The Configuration Breakdown**

A. `dbt_project.yml` (The Rulebook)

This file tells dbt: "Anything inside the staging folder must be a View."

YAML

`models:
  gcp_martech_warehouse:
    staging:
      +materialized: view  # Forces 'CREATE OR REPLACE VIEW'
      +schema: silver      # Appends '_silver' to your dataset`

B. `src_marketing.yml` (The Inventory)

This file tells dbt: "Here is the list of Raw Tables I am allowed to touch."

- It creates the "Source Node" (Green) in your lineage graph.


- It allows us to use {{ source('marketing_raw', '...') }} in SQL, so if the project name changes, we only update this one YAML file, not 50 SQL files.

C. `stg_meta_ads_xxxxx.sql` and `stg_google_ads_xxxxx`(The Transformation)

This file contains the logic.

- `FARM_FINGERPRINT(...)`: Creates a unique Primary Key (Surrogate Key) for testing duplicates.


- `SAFE_CAST(...)`: Prevents the pipeline from crashing if Meta sends a weird text string in a number column. It turns errors into NULL instead of failing.

**5. When do we switch to Tables? (The Gold Layer)**

You will notice your config says:

YAML

    `marts:
      +materialized: table  # <--- Gold Layer`

Why switch? In Phase 3 (Gold), we will UNION Google and Meta data and JOIN it with CRM data.

- Joining tables is expensive and slow.

- We do not want to calculate that join every time you open Looker.

- Therefore, we calculate it once (at 7:00 AM) and save the result as a Table.

- Looker then reads the "Pre-baked Cake" (Table) instantly.

** 🏁 Summary Checklist**

[x] Silver Layer = materialized: view

[x] Location = marketing_dev_silver (in BigQuery)

[x] Cost = $0 to build; Standard query rate to read.

[x] Update Frequency = Updates instantly when Raw data arrives.