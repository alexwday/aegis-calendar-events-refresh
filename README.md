# AEGIS Calendar Events Refresh

3-stage ETL pipeline: FactSet API → Processing → PostgreSQL

## Setup

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# Edit .env with your credentials
```

## Run

```bash
PYTHONPATH=src python src/main.py                # Full pipeline
PYTHONPATH=src python src/main.py --dry-run      # Test DB connection without changes
PYTHONPATH=src python src/main.py --skip-upload  # Stages 1-2 only
```

## Run Individual Stages

```bash
PYTHONPATH=src python src/stage_1_data_acquisition/data_acquisition.py
PYTHONPATH=src python src/stage_2_data_processing/data_processing.py
PYTHONPATH=src python src/stage_3_database_upload/database_upload.py --dry-run
```
