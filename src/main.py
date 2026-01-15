#!/usr/bin/env python3
"""
AEGIS Calendar Events Refresh - Main Pipeline

Runs all three stages in sequence:
  1. Data Acquisition - Fetch events from FactSet API
  2. Data Processing  - Transform, filter, deduplicate
  3. Database Upload  - Upload to PostgreSQL

Usage:
    python src/main.py              # Run all stages (full upload)
    python src/main.py --dry-run    # Run stages 1-2, then dry-run stage 3
    python src/main.py --skip-upload # Run stages 1-2 only, skip database upload
"""

import argparse
import logging
import sys

from stage_1_data_acquisition.data_acquisition import main as stage_1
from stage_2_data_processing.data_processing import main as stage_2
from stage_3_database_upload.database_upload import main as stage_3


# Logging configuration with colors
class ColorFormatter(logging.Formatter):
    """Custom formatter with ANSI colors."""

    COLORS = {
        "DEBUG": "\033[36m",
        "INFO": "\033[32m",
        "WARNING": "\033[33m",
        "ERROR": "\033[31m",
        "CRITICAL": "\033[31;1m",
    }
    RESET = "\033[0m"
    BOLD = "\033[1m"

    def format(self, record):
        color = self.COLORS.get(record.levelname, "")
        record.levelname = f"{color}{record.levelname:7}{self.RESET}"
        record.msg = f"{self.BOLD}{record.msg}{self.RESET}"
        return super().format(record)


handler = logging.StreamHandler()
handler.setFormatter(
    ColorFormatter("%(asctime)s │ %(levelname)s │ %(message)s", "%H:%M:%S")
)
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)
log.addHandler(handler)


def main():
    """Run the full pipeline."""
    parser = argparse.ArgumentParser(
        description="AEGIS Calendar Events Refresh Pipeline"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run stage 3 in dry-run mode (no database changes)",
    )
    parser.add_argument(
        "--skip-upload",
        action="store_true",
        help="Skip stage 3 entirely (stages 1-2 only)",
    )
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("AEGIS CALENDAR EVENTS REFRESH")
    log.info("=" * 60)

    log.info("")
    log.info("Running Stage 1: Data Acquisition")
    log.info("-" * 40)
    stage_1()

    log.info("")
    log.info("Running Stage 2: Data Processing")
    log.info("-" * 40)
    stage_2()

    if args.skip_upload:
        log.info("")
        log.info("Skipping Stage 3 (--skip-upload)")
    else:
        log.info("")
        log.info("Running Stage 3: Database Upload")
        log.info("-" * 40)
        if args.dry_run:
            sys.argv = [sys.argv[0], "--dry-run"]
        else:
            sys.argv = [sys.argv[0]]
        stage_3()

    log.info("")
    log.info("=" * 60)
    log.info("PIPELINE COMPLETE")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
