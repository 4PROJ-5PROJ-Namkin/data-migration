import argparse
import logging
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv
import os

def run_job(job_path, job_args):
    """Executes a Python job with additional arguments."""
    try:
        subprocess.run([sys.executable, job_path] + job_args, check=True)
        logging.info(f"Executed {job_path} successfully with arguments {job_args}.")
    except subprocess.CalledProcessError as e:
        logging.error(f"Error executing {job_path}: {e}")
        return e

def execute_jobs_concurrently(jobs, job_args):
    """Executes jobs concurrently using ThreadPoolExecutor."""
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(run_job, job, job_args) for job in jobs]

        for future in futures:
            future.result()

if __name__ == "__main__":
    log_file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'logs', 'setup.log'))
    logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
                logging.FileHandler(log_file_path),
                logging.StreamHandler()
            ]
    )

    env_path = os.path.join(os.path.dirname(__file__), '.env')
    load_dotenv(dotenv_path=env_path)

    parser = argparse.ArgumentParser(description="Run child Python jobs with optional arguments using concurrent threading.")
    parser.add_argument('jobs', nargs='+', help="List of jobs to run concurrently.")
    parser.add_argument('--job_args', nargs='*', help="Optional arguments to pass to each job.", default=[])
    args = parser.parse_args()

    execute_jobs_concurrently(args.jobs, args.job_args)
