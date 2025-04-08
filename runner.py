import importlib
import logging
import os
import sys
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


def run_etl_job(module_name, file_name):
    """Run a single ETL job and return its success/failure status"""
    try:
        logger.info(f"Starting ETL job: {module_name}")

        # Import the module dynamically
        module = importlib.import_module(f"src.cleaning.{module_name}")

        # Get the class name - convert snake_case to CamelCase and add 'ETL'
        # e.g., "etl_parking_meters" -> "ParkingMetersETL"
        class_name = ''.join(word.capitalize() for word in module_name.split('_') if word != 'etl')
        class_name += 'ETL'

        # Get the ETL class
        etl_class = getattr(module, class_name)

        # Initialize the ETL job
        current_dir = Path(__file__).parent
        raw_file_path = current_dir / "src" / "resources" / "raw" / file_name

        # Get the file category from the class name (remove 'ETL')
        file_category = class_name[:-3]

        etl_job = etl_class(
            file_path=raw_file_path,
            file_category=file_category,
        )

        # Run the ETL job
        etl_job.run()

        logger.info(f"Successfully completed ETL job: {module_name}")
        return module_name, True

    except Exception as e:
        logger.error(f"Error running ETL job {module_name}: {str(e)}")
        return module_name, False


def run_all_etl_jobs(parallel=True, max_workers=4):
    """Run all ETL jobs in the cleaning module"""
    # Define ETL module to filename mappings
    etl_config = {
        "etl_autism_patient": "autism_patient.csv",
        "etl_parking_meters": "ParkingMeters.csv",
        "etl_retail_data": "Retail_Data1.csv",
        "etl_social_media": "social_media_entertainment_data.csv",
        "etl_sports_betting": "SportsBettingUserBehavior.csv",
        "etl_traffic_crashes": "TrafficCrashes.csv"
    }

    # Run ETL jobs
    results = []
    if parallel:
        # Run ETL jobs in parallel using ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all jobs
            future_to_module = {
                executor.submit(run_etl_job, module_name, file_name): module_name
                for module_name, file_name in etl_config.items()
            }

            # Process results as they complete
            for future in as_completed(future_to_module):
                module_name = future_to_module[future]
                try:
                    results.append(future.result())
                except Exception as e:
                    logger.error(f"ETL job {module_name} generated an exception: {str(e)}")
                    results.append((module_name, False))
    else:
        # Run ETL jobs sequentially
        for module_name, file_name in etl_config.items():
            results.append(run_etl_job(module_name, file_name))

    # Report final status
    successful = [module for module, success in results if success]
    failed = [module for module, success in results if not success]

    logger.info(f"ETL Run Summary:")
    logger.info(f"  Total ETL jobs: {len(results)}")
    logger.info(f"  Successful: {len(successful)}")
    logger.info(f"  Failed: {len(failed)}")

    if failed:
        logger.error(f"Failed ETL jobs: {', '.join(failed)}")
        return False

    return True


if __name__ == "__main__":
    # Default runs in sequential mode - instead of parallel (parallel not recommended right now)
    parallel = False
    max_workers = 4

    if len(sys.argv) > 1:
        if sys.argv[1].lower() in ('--sequential', '-s'):
            parallel = False
        elif sys.argv[1].lower() in ('--parallel', '-p'):
            parallel = True
            # Check if max workers is specified
            if len(sys.argv) > 2 and sys.argv[2].isdigit():
                max_workers = int(sys.argv[2])

    mode = "parallel" if parallel else "sequential"
    logger.info(f"Starting ETL runner in {mode} mode" +
                (f" with {max_workers} workers" if parallel else ""))

    success = run_all_etl_jobs(parallel=parallel, max_workers=max_workers)

    if success:
        logger.info("All ETL jobs completed successfully")
        sys.exit(0)
    else:
        logger.error("Some ETL jobs failed")
        sys.exit(1)