"""
Main Entry Point for AmbitionBox Scraper.

Provides command-line interface for running test or full scraping modes.
"""

import argparse
import asyncio
import logging
import sys
from pathlib import Path

# Setup logging before importing modules
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/main.log"),
        logging.StreamHandler(),
    ],
)

logger = logging.getLogger(__name__)

# Create logs directory
Path("logs").mkdir(exist_ok=True)


def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="AmbitionBox Employee Review Scraper",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run test scrape (2 pages per company)
  python main.py --mode test
  
  # Run full production scrape with checkpoints
  python main.py --mode full --max_reviews 10000
  
  # Full run without checkpoint recovery
  python main.py --mode full --no-checkpoint
        """,
    )

    parser.add_argument(
        "--mode",
        choices=["test", "full"],
        default="test",
        help="Scraping mode: test (limited scope) or full (production scale)",
    )

    parser.add_argument(
        "--max_reviews",
        type=int,
        default=10000,
        help="Maximum reviews per company (full mode only)",
    )

    parser.add_argument(
        "--no-checkpoint",
        action="store_true",
        help="Disable checkpoint recovery (full mode only)",
    )

    parser.add_argument(
        "--reset-checkpoint",
        action="store_true",
        help="Reset checkpoint before running (full mode only)",
    )

    parser.add_argument(
        "--headless",
        action="store_true",
        default=True,
        help="Run browser in headless mode (default: True)",
    )

    parser.add_argument(
        "--no-headless",
        action="store_true",
        help="Disable headless mode (show browser window)",
    )

    parser.add_argument(
        "--config",
        default="config/companies.yaml",
        help="Path to companies configuration file",
    )

    return parser.parse_args()


async def run_test_mode(args):
    """Run test scraping mode."""
    from runs.test_run import TestRun

    logger.info("Starting TEST mode")
    logger.info(f"Config file: {args.config}")
    logger.info("Scope: 2 pages per company")

    try:
        test_run = TestRun(config_path=args.config)
        results = await test_run.run()

        # Build and export sample data
        if test_run.dataset_builder.company_data or test_run.dataset_builder.review_data:
            logger.info("\nBuilding sample datasets...")
            company_df = test_run.dataset_builder.build_company_dataframe()
            review_df = test_run.dataset_builder.build_review_dataframe()
            master_df = test_run.dataset_builder.build_master_dataframe(company_df, review_df)

            logger.info(f"Company DF: {len(company_df)} rows")
            logger.info(f"Review DF: {len(review_df)} rows")
            logger.info(f"Master DF: {len(master_df)} rows")

            # Export CSVs
            test_run.csv_writer.write_all_csvs(company_df, review_df, master_df)
            logger.info("Sample CSVs written to data/raw/")

        logger.info("\n✓ Test run completed successfully")
        return 0

    except Exception as e:
        logger.error(f"\n✗ Test run failed: {e}", exc_info=True)
        return 1


async def run_full_mode(args):
    """Run full production mode."""
    from runs.full_run import FullRun
    from scraper.checkpoint_manager import CheckpointManager

    logger.info("Starting FULL production mode")
    logger.info(f"Config file: {args.config}")
    logger.info(f"Max reviews per company: {args.max_reviews}")
    logger.info(f"Checkpoint recovery: {not args.no_checkpoint}")

    # Reset checkpoint if requested
    if args.reset_checkpoint:
        logger.info("Resetting checkpoint...")
        checkpoint_manager = CheckpointManager()
        checkpoint_manager.reset_checkpoint()
        logger.info("Checkpoint reset complete")

    try:
        full_run = FullRun(
            config_path=args.config,
            max_reviews_per_company=args.max_reviews,
        )

        results = await full_run.run(
            enable_checkpoint_recovery=not args.no_checkpoint
        )

        if results.get("error"):
            logger.error(f"Full run encountered error: {results['error']}")
            return 1

        logger.info("\n✓ Full run completed successfully")
        return 0

    except Exception as e:
        logger.error(f"\n✗ Full run failed: {e}", exc_info=True)
        return 1


def main():
    """Main entry point."""
    args = parse_arguments()

    logger.info("="*80)
    logger.info("AMBITIONBOX EMPLOYEE REVIEW SCRAPER")
    logger.info("="*80)

    try:
        if args.mode == "test":
            exit_code = asyncio.run(run_test_mode(args))
        elif args.mode == "full":
            exit_code = asyncio.run(run_full_mode(args))
        else:
            logger.error(f"Unknown mode: {args.mode}")
            exit_code = 1

        logger.info("="*80)
        sys.exit(exit_code)

    except KeyboardInterrupt:
        logger.warning("\n✗ Execution interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"\n✗ Unexpected error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
