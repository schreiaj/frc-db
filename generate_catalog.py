import os

import boto3
import duckdb
from dotenv import load_dotenv

load_dotenv()

# --- Configuration ---
R2_ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID")
R2_ACCESS_KEY = os.getenv("R2_ACCESS_KEY")
R2_SECRET_KEY = os.getenv("R2_SECRET_KEY")
BUCKET_NAME = os.getenv("R2_BUCKET_NAME")
BASE_URL = "https://frc-db.dev"

TABLE_DESCRIPTIONS = {
    "matches": "One row per match. All years from 2005 to present. red_teams and blue_teams are arrays of team keys.",
    "events": "One row per event (regional, district, championship, etc.). event_type is an integer enum — join frc.event_types for labels.",
    "teams": "One row per registered FRC team.",
    "awards": "One row per award recipient per event. team_key is NULL for individual awards (e.g. Dean's List); awardee is NULL for team awards. award_type is an integer enum — join frc.award_types for names.",
    "score_breakdowns": "Per-match scoring detail as a JSON string. Join to frc.matches on key. The schema of score_breakdown varies by year.",
    "award_types": "Enum table mapping award_type integer to award name.",
    "event_types": "Enum table mapping event_type integer to event label.",
}


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
        aws_access_key_id=R2_ACCESS_KEY,
        aws_secret_access_key=R2_SECRET_KEY,
    )


def upload_catalog(file_path):
    """Uploads the .db file to the root of the R2 bucket."""
    client = s3_client()
    print(f"Uploading {file_path} to R2...")
    client.upload_file(
        file_path,
        BUCKET_NAME,
        file_path,
        ExtraArgs={"ContentType": "application/octet-stream"},
    )
    print(f"{file_path} is now live at {BASE_URL}/{file_path}")


def generate_skill_md(con):
    lines = [
        "# FRC Data Lake — LLM Query Skill",
        "",
        "Use this skill to answer questions about FIRST Robotics Competition (FRC) history using SQL against a DuckDB-accessible Parquet archive covering 2005 to present.",
        "",
        "## Connection",
        "",
        "```sql",
        "INSTALL httpfs; LOAD httpfs;",
        f"ATTACH '{BASE_URL}/data.db' AS frc (READ_ONLY);",
        "```",
        "",
        "## Tables",
        "",
    ]

    for table, description in TABLE_DESCRIPTIONS.items():
        try:
            cols = con.execute(f"DESCRIBE {table}").fetchall()
        except Exception:
            continue
        col_list = ", ".join(f"`{c[0]}` ({c[1]})" for c in cols)
        lines += [
            f"### frc.{table}",
            description,
            "",
            f"**Columns:** {col_list}",
            "",
        ]

    lines += [
        "## Key Conventions",
        "",
        "- **Team keys** are strings like `'frc254'` — always lowercase `frc` + team number.",
        "- **Alliance membership:** use `list_contains(red_teams, 'frc254')` or `list_contains(blue_teams, 'frc254')`.",
        "- **comp_level values:** `'qm'` = qualification, `'sf'` = semifinal, `'f'` = final, `'qf'` = quarterfinal, `'ef'` = eighth final.",
        "- **event_type** is an integer — join `frc.event_types` for human-readable labels. Types 0–5 cover standard competition events.",
        "- **award_type** is an integer — join `frc.award_types` for award names. Type 0 = Chairman's Award (now FIRST Impact Award).",
        "- **score_breakdown** in `frc.score_breakdowns` is a JSON string whose schema varies by year. Extract fields with `->>` e.g. `sb.score_breakdown->>'$.red.autoPoints'`.",
        "- **time** in matches is a Unix timestamp and may be NULL for older matches.",
        "- **Joins:** `matches` ↔ `score_breakdowns` on `key`; `matches`/`awards` ↔ `events` on `event_key = key`; `awards` ↔ `award_types` on `award_type`; `events` ↔ `event_types` on `event_type`.",
        "",
        "## Example Queries",
        "",
        "### Average match score by year",
        "```sql",
        "SELECT year, avg((red_score + blue_score) / 2.0) AS avg_total_score",
        "FROM frc.matches",
        "GROUP BY year ORDER BY year DESC;",
        "```",
        "",
        "### Events a team competed at in a given year",
        "```sql",
        "SELECT DISTINCT e.name, e.city, e.state_prov",
        "FROM frc.matches m",
        "JOIN frc.events e ON m.event_key = e.key",
        "WHERE (list_contains(m.red_teams, 'frc254') OR list_contains(m.blue_teams, 'frc254'))",
        "  AND m.year = 2024;",
        "```",
        "",
        "### All Chairman's Award (FIRST Impact Award) winners",
        "```sql",
        "SELECT a.year, a.event_key, a.team_key",
        "FROM frc.awards a",
        "JOIN frc.award_types t ON a.award_type = t.award_type",
        "WHERE t.award_type == 0",
        "ORDER BY a.year DESC;",
        "```",
        "",
        "### Awards won by a specific team",
        "```sql",
        "SELECT at.name, count(*) AS times_won",
        "FROM frc.awards a",
        "JOIN frc.award_types t ON a.award_type = t.award_type",
        "WHERE a.team_key = 'frc254'",
        "GROUP BY t.name ORDER BY times_won DESC;",
        "```",
        "",
        "### Parse score breakdown JSON (schema varies by year)",
        "```sql",
        "SELECT m.key, (sb.score_breakdown->>'$.red.autoPoints')::INT AS red_auto",
        "FROM frc.matches m",
        "JOIN frc.score_breakdowns sb ON m.key = sb.key",
        "WHERE m.year = 2024 AND m.comp_level = 'qm'",
        "LIMIT 10;",
        "```",
    ]

    return "\n".join(lines)


def upload_skill_md(content):
    client = s3_client()
    print("Uploading skill.md to R2...")
    client.put_object(
        Bucket=BUCKET_NAME,
        Key="skill.md",
        Body=content.encode("utf-8"),
        ContentType="text/markdown",
    )
    print(f"skill.md is now live at {BASE_URL}/skill.md")


def create_pointer_db(output_path="data.db"):
    con = duckdb.connect(output_path)
    con.execute("INSTALL httpfs; LOAD httpfs;")

    print("Defining virtual views...")

    con.execute(
        f"CREATE OR REPLACE VIEW matches AS SELECT * FROM read_parquet('{BASE_URL}/matches/data.parquet');"
    )
    con.execute(
        f"CREATE OR REPLACE VIEW events AS SELECT * FROM read_parquet('{BASE_URL}/events/data.parquet');"
    )
    con.execute(
        f"CREATE OR REPLACE VIEW teams AS SELECT * FROM read_parquet('{BASE_URL}/teams/all_teams.parquet');"
    )

    for key, label in [
        ("score_breakdowns", "--migrate-breakdowns"),
        ("awards", "--start/--end"),
        ("award_types", "--start/--end"),
    ]:
        try:
            con.execute(
                f"CREATE OR REPLACE VIEW {key} AS SELECT * FROM read_parquet('{BASE_URL}/{key}/data.parquet');"
            )
        except Exception:
            print(f"{key}/data.parquet not found — skipping view (run {label} first)")

    con.execute("""
        CREATE OR REPLACE VIEW event_types AS
        SELECT * FROM (VALUES
            (0, 'Regional'),
            (1, 'District'),
            (2, 'District Championship'),
            (3, 'Championship Division'),
            (4, 'Championship Finals'),
            (5, 'District Championship Division'),
            (6, 'Festival of Champions'),
            (7, 'Remote')
        ) t(event_type, label);
    """)

    skill = generate_skill_md(con)
    con.close()
    print(f"Pointer database created: {output_path}")

    upload_catalog(output_path)
    upload_skill_md(skill)


if __name__ == "__main__":
    create_pointer_db()
