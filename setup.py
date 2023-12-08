import argparse
import logging
import subprocess
import sys

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

parser = argparse.ArgumentParser(description="Run child Python scripts with optional arguments.")
parser.add_argument('scripts', nargs='+', help="List of scripts to run in order.")
parser.add_argument('--script_args', nargs='*', help="Optional arguments to pass to each script.", default=[])
args = parser.parse_args()

def run_script(script_path, script_args):
    """Execute a Python script with additional arguments."""
    try:
        subprocess.run([sys.executable, script_path] + script_args, check=True)
        logging.info(f"Executed {script_path} successfully with arguments {script_args}.")
    except subprocess.CalledProcessError as e:
        logging.error(f"Error executing {script_path}: {e}")

if __name__ == "__main__":
    for script_path in args.scripts:
        run_script(script_path, args.script_args)
