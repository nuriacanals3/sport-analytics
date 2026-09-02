import os
import time
import json
import boto3
from nba_api.stats.endpoints import leaguedashteamstats
from dotenv import load_dotenv

from ingestion.nba.config import SEASONS, SEASON_TYPE

load_dotenv()

S3_ACCESS_KEY = os.getenv('B2_KEY_ID')
S3_SECRET_KEY = os.getenv('B2_APP_KEY')
S3_BUCKET = os.getenv('S3_BUCKET_NAME')
S3_REGION = os.getenv('S3_REGION') or None
S3_ENDPOINT_URL = os.getenv('S3_ENDPOINT_URL') or None
S3_PREFIX = 'bronze/nba_team_stats/'

# Two group of metrics for each season:
# - Base gives the box-score counting stats.
# - Advanced gives net/off/def rating and pace.
MEASURE_TYPES = ['Base', 'Advanced']


def get_team_stats(season, measure_type):
    """Fetch raw per-team season stats JSON for one season and measure type."""
    print(f"Fetching {measure_type} team stats for season {season}...")
    stats = leaguedashteamstats.LeagueDashTeamStats(
        season=season,
        season_type_all_star=SEASON_TYPE,
        measure_type_detailed_defense=measure_type,
        per_mode_detailed='PerGame',
    )
    time.sleep(1.5)

    return stats.get_dict()


def upload_to_s3(data, filename):
    """Uploads raw JSON data to the Bronze S3 bucket."""
    s3_client = boto3.client(
        's3',
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        endpoint_url=S3_ENDPOINT_URL,
        region_name=S3_REGION
    )

    s3_key = f"{S3_PREFIX}{filename}"
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=json.dumps(data),
        ContentType='application/json'
    )
    print(f"Uploaded {filename} to s3://{S3_BUCKET}/{s3_key}")


def main():
    for season in SEASONS:
        for measure_type in MEASURE_TYPES:
            try:
                raw_stats_data = get_team_stats(season, measure_type)

                filename = f"team_stats_{measure_type.lower()}_{season}.json"

                upload_to_s3(raw_stats_data, filename)

            except Exception as e:
                print(f"Error processing {measure_type} stats for season {season}: {e}")


if __name__ == "__main__":
    main()
