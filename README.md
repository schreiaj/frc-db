
# 🤖 frc-db.dev
**A SQL-first companion to The Blue Alliance.**

`frc-db.dev` provides a community-maintained **Parquet-based archive** of the data from [The Blue Alliance](https://www.thebluealliance.com/). It is a supplemental tool designed for mentors, scouts, and data scientists who want to perform complex analytical SQL queries across the entire history of the FRC without the overhead of bulk API scraping.

## 🚀 Quick Start (Interactive)

To explore the data, open the DuckDB shell and attach the remote archive.

1. **Open DuckDB**:
   ```bash
   duckdb
   ```

2. **Run the setup block**:
   Copy and paste this into your DuckDB prompt:
   ```sql
   -- Enable network access and attach the archive
   INSTALL httpfs; 
   LOAD httpfs;
   ATTACH 'https://frc-db.dev/data.db' AS frc;
   ```

3. **Query the archive**:
   ```sql
   -- Find the average score per year
   SELECT year, avg(red_score) as avg_score 
   FROM frc.matches 
   GROUP BY year 
   ORDER BY year DESC;
   ```

---

## 🏗️ How it Works

This project provides a **Serverless SQL Interface** to the historical records of the FRC.

### The Tech Stack
* **Extraction:** A Python/Polars pipeline (running on GitHub Actions) translates TBA records into flat, analytical Parquet files.
* **Storage:** Data is hosted on **Cloudflare R2**. 
* **Organization:** Matches and events are **Hive-partitioned** by `year` (e.g., `/matches/year=2026/data.parquet`).
* **The Engine:** **DuckDB** uses **HTTP Range Requests** to download only the specific columns or rows required for your query, making it incredibly bandwidth-efficient.

### When to use this vs. the TBA API:
* **Aggregations:** If you need to calculate "all-time" stats or trends over decades.
* **Complex Joins:** Combine `matches`, `events`, and `teams` in a single SQL statement.
* **JSON Analysis:** Scoring rules change annually; the `score_breakdown` column is preserved as a JSON string for on-the-fly parsing:
  ```sql
  SELECT avg((score_breakdown->>'$.red.autoPoints')::INT) 
  FROM frc.matches 
  WHERE year = 2024;
  ```

---

## Exported Data

Currently we only provide Team, Event, and Match data and don't include things like Districts or Awards (though this is likely coming next) If you need District data or other things TBA provides, file an issue or better yet a PR. 


## 📊 Sample Queries

### Find all events for a specific team (e.g., Team 2930)
```sql
SELECT e.name, e.year, e.city
FROM frc.events e
JOIN frc.matches m ON e.key = m.event_key
WHERE list_contains(m.red_teams, 'frc2930') 
   OR list_contains(m.blue_teams, 'frc2930')
GROUP BY ALL;
```

### Get all teams in a specific city
```sql
SELECT team_number, nickname, rookie_year 
FROM frc.teams 
WHERE city = 'Boston' AND state_prov = 'MA';
```

---

## 🛠️ Project Structure
* `main.py`: The ETL pipeline logic.
* `generate_catalog.py`: Builds the `data.db` pointer file.
* `flake.nix`: Reproducible development environment.
* `.github/workflows/`: Daily syncs with the current TBA season.




## ❤️ Credits & Data
All data is sourced from **The Blue Alliance**. Please follow their [Developer Guidelines](https://www.thebluealliance.com/apidocs) and support their mission to provide the foundational infrastructure for FRC data.
