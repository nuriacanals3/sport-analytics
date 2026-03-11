import os
import time
import json
import boto3
from datetime import datetime, timedelta
from nba_api.stats.endpoints import scoreboardv2, playbyplayv3
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
S3_BUCKET = os.getenv('S3_BUCKET_NAME')
S3_PREFIX = 'bronze/nba_pbp/'


def get_games_for_date(target_date):
    """Fetch all game IDs for a specific date."""
    print(f"Fetching game schedule for {target_date}...")
    board = scoreboardv2.ScoreboardV2(game_date=target_date)
    print(board.get_normalized_dict())
    games = board.get_normalized_dict()['GameHeader']
    return [game['GAME_ID'] for game in games]


def get_play_by_play(game_id):
    """Fetch raw Play-By-Play JSON data for a specific game ID."""
    print(f"Fetching PbP for Game ID: {game_id}")
    pbp = playbyplayv3.PlayByPlayV3(game_id=game_id)
    time.sleep(1.5) 
    
    return pbp.get_dict()


def upload_to_s3(data, filename):
    """Uploads raw JSON data to the Bronze S3 bucket."""
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
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
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    game_ids = get_games_for_date(yesterday)
    
    if not game_ids:
        print(f"No games found for {yesterday}.")
        return

    for game_id in game_ids:
        try:
            raw_pbp_data = get_play_by_play(game_id)
            
            filename = f"pbp_{game_id}_{yesterday}.json"
            
            upload_to_s3(raw_pbp_data, filename)
            
        except Exception as e:
            print(f"Error processing Game ID {game_id}: {e}")


if __name__ == "__main__":
    main()