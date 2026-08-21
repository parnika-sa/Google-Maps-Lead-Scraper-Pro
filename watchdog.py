import subprocess
import time
import sys
import os
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [WATCHDOG] - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("watchdog.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def run_scraper(keyword, city, max_results=None, headless=False, no_emails=False):
    """Run the scraper with automatic resume on failure"""
    
    cmd = [
        sys.executable, "maps_scraper.py",
        "--keyword", keyword,
        "--city", city
    ]
    
    if max_results:
        cmd.extend(["--max-results", str(max_results)])
    if headless:
        cmd.append("--headless")
    if no_emails:
        cmd.append("--no-emails")
        
    # First attempt
    attempt = 1
    max_attempts = 10
    backoff_time = 30 # seconds
    
    while attempt <= max_attempts:
        logger.info(f"Starting scraper attempt {attempt}/{max_attempts}...")
        
        # Add resume flag after the first attempt
        current_cmd = list(cmd)
        if attempt > 1:
            current_cmd.append("--resume")
            logger.info(f"Resuming from last checkpoint...")
            
        try:
            # Run the scraper process
            process = subprocess.Popen(current_cmd)
            
            # Wait for process to complete
            return_code = process.wait()
            
            if return_code == 0:
                logger.info("Scraper completed successfully.")
                break
            else:
                logger.error(f"Scraper exited with return code {return_code}.")
                
        except Exception as e:
            logger.exception(f"Watchdog encountered an error: {e}")
            
        # Exponential backoff before restart
        logger.warning(f"Restarting in {backoff_time} seconds...")
        time.sleep(backoff_time)
        backoff_time = min(backoff_time * 2, 300) # Max 5 minutes
        attempt += 1
        
    if attempt > max_attempts:
        logger.error("Maximum restart attempts reached. Watchdog stopping.")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Watchdog for Google Maps Scraper")
    parser.add_argument("--keyword", required=True)
    parser.add_argument("--city", required=True)
    parser.add_argument("--max-results", type=int)
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--no-emails", action="store_true")
    
    args = parser.parse_args()
    
    logger.info("Watchdog service initialized.")
    run_scraper(
        keyword=args.keyword,
        city=args.city,
        max_results=args.max_results,
        headless=args.headless,
        no_emails=args.no_emails
    )
