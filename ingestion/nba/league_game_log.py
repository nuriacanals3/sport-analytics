import os
import time
import json
import boto3
from nba_api.stats.endpoints import leaguegamelog
from dotenv import load_dotenv

from ingestion.nba.config import SEASONS, SEASON_TYPE

load_dotenv()

S3_ACCESS_KEY = os.getenv('B2_KEY_ID')
S3_SECRET_KEY = os.getenv('B2_APP_KEY')
S3_BUCKET = os.getenv('S3_BUCKET_NAME')
S3_REGION = os.getenv('S3_REGION') or None
S3_ENDPOINT_URL = os.getenv('S3_ENDPOINT_URL') or None
S3_PREFIX = 'bronze/nba_game_log/'


def get_game_log(season):
    """Fetch raw team-level game log JSON for a full season.

    player_or_team_abbreviation='T' returns one row per team per game
    (not one row per player), which is what the travel/fatigue features
    are built on -- see fatigue_features in the travel-logistics plan.
    """
    print(f"Fetching team game log for season {season}...")
    log = leaguegamelog.LeagueGameLog(
        season=season,
        season_type_all_star=SEASON_TYPE,
        player_or_team_abbreviation='T',
    )
    time.sleep(1.5)

    return log.get_dict()


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
        try:
            raw_log_data = get_game_log(season)

            filename = f"game_log_{season}.json"

            upload_to_s3(raw_log_data, filename)

        except Exception as e:
            print(f"Error processing season {season}: {e}")


if __name__ == "__main__":
    main()
