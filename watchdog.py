import subprocess
import time
import sys
import logging
import signal
from pathlib import Path

HEALTHY_RUN_RESET_SECONDS = 600
GRACEFUL_SHUTDOWN_TIMEOUT = 30
DEFAULT_SCRAPER_TIMEOUT = 86400
DEFAULT_MAX_ATTEMPTS = 10
INITIAL_BACKOFF = 30
MAX_BACKOFF = 300

script_dir = Path(__file__).resolve().parent
scraper_script = script_dir / "maps_scraper.py"
log_file_path = script_dir / "watchdog.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [WATCHDOG] - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(str(log_file_path)),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def _send_graceful_stop(process):
    if process.poll() is not None:
        return process.returncode
    try:
        if sys.platform == "win32":
            process.send_signal(signal.CTRL_C_EVENT)
        else:
            process.send_signal(signal.SIGINT)
    except (OSError, ValueError):
        pass
    try:
        return process.wait(timeout=GRACEFUL_SHUTDOWN_TIMEOUT)
    except subprocess.TimeoutExpired:
        logger.warning(
            f"Child PID {process.pid} did not exit within "
            f"{GRACEFUL_SHUTDOWN_TIMEOUT}s after SIGINT; forcing kill."
        )
        try:
            process.kill()
        except (OSError, ValueError):
            pass
        try:
            return process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            return None
    except (OSError, ValueError):
        return process.poll()


def _build_cmd(keyword, city, max_results, headless, no_emails, timeout):
    cmd = [
        sys.executable,
        str(scraper_script),
        "--keyword", keyword,
        "--city", city,
        "--timeout", str(timeout),
    ]
    if max_results:
        cmd.extend(["--max-results", str(max_results)])
    if headless:
        cmd.append("--headless")
    if no_emails:
        cmd.append("--no-emails")
    return cmd


def run_scraper(
    keyword,
    city,
    max_results=None,
    headless=False,
    no_emails=False,
    timeout=DEFAULT_SCRAPER_TIMEOUT,
    max_attempts=DEFAULT_MAX_ATTEMPTS,
    _popen=subprocess.Popen,
    _time=time.time,
    _sleep=time.sleep,
    _stop_fn=_send_graceful_stop,
):
    cmd = _build_cmd(keyword, city, max_results, headless, no_emails, timeout)

    attempt = 1
    backoff_time = INITIAL_BACKOFF
    last_process = None

    while attempt <= max_attempts:
        logger.info(f"Starting scraper attempt {attempt}/{max_attempts}...")

        current_cmd = list(cmd)
        if attempt > 1:
            current_cmd.append("--resume")
            logger.info("Resuming from last checkpoint...")

        start_ts = _time()
        process = None
        return_code = None
        elapsed = 0.0

        try:
            process = _popen(current_cmd, cwd=str(script_dir))
            last_process = process
            return_code = process.wait()
            elapsed = _time() - start_ts
        except KeyboardInterrupt:
            logger.info("Watchdog received interrupt; stopping scraper gracefully.")
            if process is not None:
                _stop_fn(process)
            raise
        except Exception as e:
            logger.exception(f"Watchdog encountered an error: {e}")
            if process is not None and process.poll() is None:
                _stop_fn(process)
            elapsed = _time() - start_ts

        if return_code == 0:
            logger.info("Scraper completed successfully.")
            return 0

        if return_code == 2:
            logger.error(
                "Scraper exited with code 2 (invalid arguments/configuration). "
                "Stopping without retrying."
            )
            return 2

        logger.error(
            f"Scraper exited with return code {return_code} after {elapsed:.1f}s."
        )

        healthy_reset = (
            return_code is not None
            and elapsed >= HEALTHY_RUN_RESET_SECONDS
        )
        if healthy_reset:
            old_attempt = attempt
            old_backoff = backoff_time
            attempt = 1
            backoff_time = INITIAL_BACKOFF
            logger.info(
                f"Healthy-run threshold ({HEALTHY_RUN_RESET_SECONDS}s) reached "
                f"after {elapsed:.1f}s. Resetting consecutive attempt counter "
                f"{old_attempt} -> {attempt} and backoff {old_backoff}s -> "
                f"{backoff_time}s before retrying."
            )

        will_retry = attempt < max_attempts
        if not will_retry:
            logger.error(
                f"Maximum restart attempts ({max_attempts}) reached. "
                "Watchdog stopping without further sleep."
            )
            return return_code if return_code is not None else 1

        logger.warning(f"Restarting in {backoff_time} seconds (attempt {attempt}/{max_attempts})...")
        _sleep(backoff_time)
        backoff_time = min(backoff_time * 2, MAX_BACKOFF)
        attempt += 1

    return 1


def build_parser():
    import argparse
    parser = argparse.ArgumentParser(
        description="Watchdog for Google Maps Scraper. "
        "Monitors child process exit, retries failures, forwards a long "
        "scraper timeout, handles graceful SIGINT shutdown, and uses "
        "script-relative paths. Heartbeat-based hang detection is not "
        "included (follow-up PR)."
    )
    parser.add_argument("--keyword", required=True)
    parser.add_argument("--city", required=True)
    parser.add_argument("--max-results", type=int)
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--no-emails", action="store_true")
    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_SCRAPER_TIMEOUT,
        help=f"Timeout in seconds forwarded to maps_scraper.py child. "
             f"Default: {DEFAULT_SCRAPER_TIMEOUT} (materially larger than "
             f"the scraper's own 600s default so a scraper-side timeout is "
             f"not mistaken for successful completion).",
    )
    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()

    logger.info("Watchdog service initialized.")
    logger.info(f"Scraper timeout forwarded to child: {args.timeout}s")
    logger.info(f"Resolved scraper script: {scraper_script}")
    logger.info(f"Working directory for child: {script_dir}")
    logger.info(f"Watchdog log file: {log_file_path}")

    return run_scraper(
        keyword=args.keyword,
        city=args.city,
        max_results=args.max_results,
        headless=args.headless,
        no_emails=args.no_emails,
        timeout=args.timeout,
    )


if __name__ == "__main__":
    sys.exit(main() or 0)
