import argparse
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from io import BytesIO

import boto3
import httpx
import polars as pl
from dotenv import load_dotenv


def sanitize_str(value):
    """Ensure a string is valid UTF-8 by replacing bad bytes."""
    if value is None:
        return None
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value.encode("utf-8", errors="replace").decode("utf-8")

load_dotenv()

# --- Configuration ---
TBA_KEY = os.getenv("TBA_KEY")
R2_ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID")
R2_ACCESS_KEY = os.getenv("R2_ACCESS_KEY")
R2_SECRET_KEY = os.getenv("R2_SECRET_KEY")
BUCKET_NAME = os.getenv("R2_BUCKET_NAME")

# Explicit Polars Schemas (Option 2)
MATCH_SCHEMA = {
    "key": pl.String,
    "event_key": pl.String,
    "year": pl.Int32,
    "comp_level": pl.String,
    "match_number": pl.Int32,
    "red_teams": pl.List(pl.String),
    "blue_teams": pl.List(pl.String),
    "red_score": pl.Int32,
    "blue_score": pl.Int32,
    "time": pl.Int64,  # Use Int64 to prevent overflow/underflow
    "score_breakdown": pl.String,
}

EVENT_SCHEMA = {
    "key": pl.String,
    "name": pl.String,
    "event_code": pl.String,
    "event_type": pl.Int32,
    "city": pl.String,
    "state_prov": pl.String,
    "country": pl.String,
    "start_date": pl.String,
    "end_date": pl.String,
    "year": pl.Int32,
    "district": pl.String,
}

TEAM_SCHEMA = {
    "key": pl.String,
    "team_number": pl.Int32,
    "nickname": pl.String,
    "city": pl.String,
    "state_prov": pl.String,
    "country": pl.String,
    "rookie_year": pl.Int64,  # Int64 to match the 'time' safety pattern
}


# Initialize S3 client for R2
s3_client = boto3.client(
    "s3",
    endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
    aws_access_key_id=R2_ACCESS_KEY,
    aws_secret_access_key=R2_SECRET_KEY,
)

client = httpx.Client(headers={"X-TBA-Auth-Key": TBA_KEY}, timeout=30.0)


def upload_to_r2(df, key, sort_by=None, row_group_size=None):
    if sort_by:
        df = df.sort(sort_by)
    buffer = BytesIO()
    write_kwargs = {}
    if row_group_size:
        write_kwargs["row_group_size"] = row_group_size
    df.write_parquet(buffer, **write_kwargs)
    s3_client.put_object(Bucket=BUCKET_NAME, Key=key, Body=buffer.getvalue())
    print(f"Uploaded to {key}")


def download_from_r2(key: str) -> pl.DataFrame | None:
    buffer = BytesIO()
    try:
        s3_client.download_fileobj(Bucket=BUCKET_NAME, Key=key, Fileobj=buffer)
    except s3_client.exceptions.ClientError as e:
        if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
            print(f"No existing file at {key}, starting fresh.")
            return None
        raise
    buffer.seek(0)
    return pl.read_parquet(buffer)


def backfill_events(year):
    print(f"Fetching events for {year}...")
    res = client.get(f"https://www.thebluealliance.com/api/v3/events/{year}")
    events = res.json()

    event_list = []
    for e in events:
        event_list.append(
            {
                "key": e["key"],
                "name": sanitize_str(e["name"]),
                "event_code": e["event_code"],
                "event_type": e["event_type"],
                "city": sanitize_str(e.get("city")),
                "state_prov": sanitize_str(e.get("state_prov")),
                "country": sanitize_str(e.get("country")),
                "start_date": e.get("start_date"),
                "end_date": e.get("end_date"),
                "year": year,
                "district": sanitize_str(e["district"]["display_name"])
                if e.get("district")
                else None,
            }
        )

    if event_list:
        new_df = pl.DataFrame(event_list, schema=EVENT_SCHEMA)
        existing = download_from_r2("events/data.parquet")
        if existing is not None:
            existing = existing.filter(pl.col("year") != year)
            combined = pl.concat([existing, new_df], how="diagonal_relaxed")
        else:
            combined = new_df
        upload_to_r2(combined, "events/data.parquet", sort_by="year", row_group_size=100_000)


def fetch_event_matches(event_key, year):
    """Fetch and parse matches for a single event."""
    res = client.get(
        f"https://www.thebluealliance.com/api/v3/event/{event_key}/matches"
    )
    matches = res.json()
    if not matches:
        return []

    parsed = []
    for m in matches:
        # Data Hygiene (Option 3): Filter out the 1899 ghost timestamp
        m_time = m.get("time")
        if m_time and m_time < 0:
            m_time = None

        parsed.append(
            {
                "key": m["key"],
                "event_key": m["event_key"],
                "year": year,
                "comp_level": m["comp_level"],
                "match_number": m["match_number"],
                "red_teams": m["alliances"]["red"]["team_keys"],
                "blue_teams": m["alliances"]["blue"]["team_keys"],
                "red_score": m["alliances"]["red"]["score"],
                "blue_score": m["alliances"]["blue"]["score"],
                "time": m_time,
                "score_breakdown": json.dumps(
                    m.get("score_breakdown"), ensure_ascii=True
                )
                if m.get("score_breakdown")
                else None,
            }
        )
    return parsed


def backfill_matches(year, parallel=False, max_workers=4):
    print(f"Processing matches for {year}...")
    events_res = client.get(
        f"https://www.thebluealliance.com/api/v3/events/{year}/simple"
    )
    event_keys = [e["key"] for e in events_res.json()]

    all_year_matches = []

    if parallel:
        print(f"  Fetching {len(event_keys)} events with {max_workers} workers...")
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(fetch_event_matches, ek, year): ek
                for ek in event_keys
            }
            for future in as_completed(futures):
                all_year_matches.extend(future.result())
    else:
        for event_key in event_keys:
            all_year_matches.extend(fetch_event_matches(event_key, year))
            time.sleep(0.05)

    if all_year_matches:
        new_df = pl.DataFrame(all_year_matches, schema=MATCH_SCHEMA)
        existing = download_from_r2("matches/data.parquet")
        if existing is not None:
            existing = existing.filter(pl.col("year") != year)
            combined = pl.concat([existing, new_df], how="diagonal_relaxed")
        else:
            combined = new_df
        upload_to_r2(combined, "matches/data.parquet", sort_by="year", row_group_size=100_000)


def backfill_teams():
    """Iterates through TBA team pages until no more teams are found."""
    print("Fetching all teams from TBA...")
    all_teams = []
    page = 0

    while True:
        # TBA returns 500 teams per page
        res = client.get(f"https://www.thebluealliance.com/api/v3/teams/{page}")
        data = res.json()

        if not data or len(data) == 0:
            break

        for t in data:
            all_teams.append(
                {
                    "key": t["key"],
                    "team_number": t["team_number"],
                    "nickname": sanitize_str(t.get("nickname")),
                    "city": sanitize_str(t.get("city")),
                    "state_prov": sanitize_str(t.get("state_prov")),
                    "country": sanitize_str(t.get("country")),
                    "rookie_year": t.get("rookie_year"),
                }
            )

        print(f"  Processed page {page} ({len(all_teams)} teams total)...")
        page += 1
        time.sleep(0.5)  # Avoid hitting the rate limit during bulk fetch

    if all_teams:
        df = pl.DataFrame(all_teams, schema=TEAM_SCHEMA)
        upload_to_r2(df, "teams/all_teams.parquet")
    else:
        print("No teams found to upload.")


def consolidate_all():
    """One-time migration: merge all year-partitioned files into single consolidated files."""
    years = range(2005, datetime.now().year + 1)
    for kind in ("events", "matches"):
        print(f"Consolidating {kind}...")
        frames = []
        for year in years:
            df = download_from_r2(f"{kind}/year={year}/data.parquet")
            if df is not None:
                print(f"  Loaded {kind}/year={year}/data.parquet ({len(df)} rows)")
                frames.append(df)
            else:
                print(f"  Missing {kind}/year={year}/data.parquet, skipping")
        if frames:
            consolidated = pl.concat(frames, how="diagonal_relaxed")
            upload_to_r2(consolidated, f"{kind}/data.parquet", sort_by="year", row_group_size=100_000)
            print(f"Consolidated {len(consolidated)} {kind} rows.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", type=int, help="Start year for backfill")
    parser.add_argument("--end", type=int, help="End year for backfill")
    parser.add_argument(
        "--current-year-only", action="store_true", help="Only process the current year"
    )
    parser.add_argument("--teams", action="store_true", help="Export teams")
    parser.add_argument("--parallel", action="store_true", help="Fetch matches in parallel")
    parser.add_argument("--workers", type=int, default=4, help="Number of parallel workers (default: 4)")
    parser.add_argument("--consolidate", action="store_true", help="One-time migration: merge all year-partitioned files into single consolidated files")
    args = parser.parse_args()

    if args.consolidate:
        consolidate_all()
    elif args.teams:
        backfill_teams()

    elif args.current_year_only:
        current_year = datetime.now().year
        backfill_events(current_year)
        backfill_matches(current_year, parallel=args.parallel, max_workers=args.workers)
    elif args.start and args.end:
        for year in range(args.start, args.end + 1):
            backfill_events(year)
            backfill_matches(year, parallel=args.parallel, max_workers=args.workers)
    else:
        print("Please provide --start and --end, or use --current-year-only")
