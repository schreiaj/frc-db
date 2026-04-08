
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
* **Organization:** Each dataset is a single consolidated Parquet file (e.g., `matches/data.parquet`) loaded into DuckDB via HTTP range requests.
* **The Engine:** **DuckDB** reads only the columns required for your query, making it bandwidth-efficient across the full history of FRC.

### When to use this vs. the TBA API:
* **Aggregations:** If you need to calculate "all-time" stats or trends over decades.
* **Complex Joins:** Combine `matches`, `events`, `teams`, and `awards` in a single SQL statement.
* **JSON Analysis:** Scoring rules change annually; `score_breakdown` is preserved as a JSON string in `frc.score_breakdowns` for on-the-fly parsing:
  ```sql
  SELECT avg((sb.score_breakdown->>'$.red.autoPoints')::INT)
  FROM frc.score_breakdowns sb
  WHERE sb.year = 2024;
  ```

---

## Exported Data

| Table | Description |
|---|---|
| `frc.matches` | All qualification and playoff match results |
| `frc.events` | Event metadata (name, location, type, dates) |
| `frc.teams` | Team roster (number, name, location, rookie year) |
| `frc.awards` | Award recipients per event (team and individual) |
| `frc.score_breakdowns` | Per-match scoring detail as JSON, joined to `matches` on `key` |
| `frc.award_types` | Enum mapping `award_type` integer to name |
| `frc.event_types` | Enum mapping `event_type` integer to label |


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

### Find every Chairman's Award winner
```sql
SELECT a.year, a.event_key, a.team_key
FROM frc.awards a
JOIN frc.award_types at ON a.award_type = at.award_type
WHERE at.name ILIKE '%chairman%'
ORDER BY a.year DESC;
```

### Count awards won by a team all-time
```sql
SELECT at.name, count(*) AS times_won
FROM frc.awards a
JOIN frc.award_types at ON a.award_type = at.award_type
WHERE a.team_key = 'frc254'
GROUP BY at.name
ORDER BY times_won DESC;
```

---

## 🛠️ Project Structure
* `main.py`: The ETL pipeline logic.
* `generate_catalog.py`: Builds the `data.db` pointer file.
* `flake.nix`: Reproducible development environment.
* `.github/workflows/`: Daily syncs with the current TBA season.




## ❤️ Credits & Data
All data is sourced from **The Blue Alliance**. Please follow their [Developer Guidelines](https://www.thebluealliance.com/apidocs) and support their mission to provide the foundational infrastructure for FRC data.
