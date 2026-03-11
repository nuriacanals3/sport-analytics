# Sports pipelines

Automatic pipeline with NBA data using Airflow.

## Data

1. NBA:
- API: https://github.com/swar/nba_api


## Install
```
pip install -r requirements.txt
```

## Airflow
Initialize
```
airflow standalone
```

## Arquitecture
MDS Architecture:
- Bronze layer: extracting NBA play-by-play data into S3.
