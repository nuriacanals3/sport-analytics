#!/usr/bin/env bash
# Opens an interactive DuckDB session against nba.duckdb with B2/httpfs
# credentials pre-loaded. Only needed to query the staging views
# (stg_nba__game_log, stg_nba__team_season_stats), which read B2 live --
# the mart tables need none of this, they're just data sitting in the file.
set -euo pipefail

cd "$(dirname "$0")"
export $(cat ../../.env | xargs)

# Credentials only ever touch a temp file, deleted the moment this exits --
# never written into the repo or left lying around.
init_file=$(mktemp)
trap 'rm -f "$init_file"' EXIT

cat <<EOF > "$init_file"
INSTALL httpfs;
LOAD httpfs;
SET s3_region='${S3_REGION}';
SET s3_access_key_id='${B2_KEY_ID}';
SET s3_secret_access_key='${B2_APP_KEY}';
SET s3_endpoint='${S3_ENDPOINT}';
SET s3_url_style='${S3_URL_STYLE}';
EOF

duckdb nba.duckdb -init "$init_file"
