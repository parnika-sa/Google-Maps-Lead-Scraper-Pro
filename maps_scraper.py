from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
import time
import csv
import os
import argparse
import re
import json
import logging
from datetime import datetime
from typing import Optional, List, Dict, Set
from urllib.parse import urlparse

# ============================================
# CONFIGURATION & CONSTANTS
# ============================================
LOG_DIR = "logs"
CHECKPOINT_DIR = "checkpoints"
OUTPUT_DIR = "output"

os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(CHECKPOINT_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Setup logging
log_file = os.path.join(LOG_DIR, f"scraper_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Fix console encoding for Windows
import sys
if sys.platform == 'win32':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
        sys.stderr.reconfigure(encoding='utf-8')
    except:
        pass

# CSS Selectors (with fallbacks)
SEARCH_BOX_SELECTORS = [
    "input#searchboxinput",
    "input[aria-label*='Search']",
    "input[name='q']",
    "input.searchboxinput",
]
RESULTS_PANEL_SELECTORS = [
    'div[role="feed"]',
    'div.m6QErb[aria-label]',
]
BUSINESS_NAME_SELECTORS = [
    'h1.DUwDvf',
    'h1.fontHeadlineLarge',
    'h1',
]
BUSINESS_CARD_SELECTOR = 'a.hfpxzc'

# Timeouts (in milliseconds)
SEARCH_TIMEOUT = 60000
BUSINESS_LOAD_TIMEOUT = 10000
WEBSITE_LOAD_TIMEOUT = 15000
ELEMENT_WAIT_TIMEOUT = 5000

# Email settings
SKIP_EMAIL_DOMAINS = {
    'facebook.com', 'instagram.com', 'twitter.com', 'youtube.com',
    'tiktok.com', 'linkedin.com', 'pinterest.com', 'google.com',
    'maps.google.com', 'yelp.com', 'tripadvisor.com'
}

# Rate limiting
DELAY_BETWEEN_REQUESTS = 2  # seconds
DELAY_BETWEEN_BUSINESS = 2.5  # seconds
DELAY_BETWEEN_SCROLL = 2  # seconds
DELAY_AFTER_EMAIL_EXTRACTION = 2  # seconds

# Retry settings
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds

# ============================================
# UTILITY FUNCTIONS
# ============================================

def safe_filename(text: str) -> str:
    """Convert text to safe filename by replacing special chars"""
    return re.sub(r'[^a-zA-Z0-9_-]', '_', text)[:100]

def normalize_phone(phone: str) -> str:
    """Normalize phone number by removing special characters"""
    if phone == "N/A":
        return phone
    return re.sub(r'[^\d+]', '', phone)

def validate_email(email: str) -> bool:
    """Validate email format more strictly"""
    pattern = r'^[a-zA-Z0-9][a-zA-Z0-9._%+-]*@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    if not re.match(pattern, email):
        return False
    
    # Exclude common false positives
    invalid_patterns = ['test@', 'example@', 'temp@', 'placeholder@', 'noreply@', 'no-reply@']
    invalid_extensions = ['.png', '.jpg', '.gif', '.svg', '.webp']
    
    email_lower = email.lower()
    if any(email_lower.startswith(p) for p in invalid_patterns):
        return False
    if any(email_lower.endswith(ext) for ext in invalid_extensions):
        return False
    
    return True

def extract_emails_from_text(text: str) -> Set[str]:
    """Extract and validate emails from text"""
    pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
    found = re.findall(pattern, text)
    return {e for e in found if validate_email(e)}

def should_skip_email_extraction(website: str) -> bool:
    """Check if website is in skip list"""
    try:
        domain = urlparse(website).netloc.lower()
        domain = domain.replace('www.', '')
        return any(skip in domain for skip in SKIP_EMAIL_DOMAINS)
    except:
        return True

def wait_for_selector(page, selectors: List[str], timeout: int = ELEMENT_WAIT_TIMEOUT) -> bool:
    """Try multiple selectors with fallback"""
    for i, selector in enumerate(selectors):
        try:
            logger.debug(f"Trying selector {i+1}/{len(selectors)}: {selector}")
            page.wait_for_selector(selector, timeout=timeout, state='visible')
            logger.debug(f"Success with selector: {selector}")
            return True
        except Exception as e:
            logger.debug(f"Selector failed: {selector} - {str(e)[:50]}")
            continue
    logger.debug(f"All {len(selectors)} selectors failed")
    return False

def get_selector(page, selectors: List[str]):
    """Get element using fallback selectors"""
    for selector in selectors:
        try:
            element = page.query_selector(selector)
            if element and element.is_visible():
                return element
        except:
            continue
    return None

def retry_action(action, max_retries: int = MAX_RETRIES, delay: float = RETRY_DELAY):
    """Retry an action with exponential backoff"""
    for attempt in range(max_retries):
        try:
            return action()
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            logger.warning(f"Attempt {attempt + 1} failed: {e}. Retrying in {delay}s...")
            time.sleep(delay)
            delay *= 1.5
    return None

def save_checkpoint(checkpoint_file: str, businesses: List[Dict], index: int):
    """Save progress checkpoint"""
    checkpoint = {
        "timestamp": datetime.now().isoformat(),
        "index": index,
        "businesses_count": len(businesses),
        "businesses": businesses
    }
    with open(checkpoint_file, "w", encoding="utf-8") as f:
        json.dump(checkpoint, f, indent=2, ensure_ascii=False)
    logger.info(f"Checkpoint saved: {index} businesses processed")

def load_checkpoint(checkpoint_file: str) -> Optional[Dict]:
    """Load progress checkpoint"""
    if os.path.exists(checkpoint_file):
        try:
            with open(checkpoint_file, "r", encoding="utf-8") as f:
                checkpoint = json.load(f)
            logger.info(f"Checkpoint loaded: {checkpoint['index']} businesses already processed")
            return checkpoint
        except Exception as e:
            logger.warning(f"Could not load checkpoint: {e}")
    return None

def deduplicate_businesses(businesses: List[Dict]) -> List[Dict]:
    """Deduplicate businesses with better matching"""
    seen = {}
    for business in businesses:
        # Create composite key: normalized name + normalized phone
        name_key = business["name"].lower().strip()
        phone_key = normalize_phone(business["phone"])
        
        # If phone is N/A, use address for deduplication
        if phone_key == "N/A":
            address_key = business["address"].lower().strip()
            key = (name_key, address_key)
        else:
            key = (name_key, phone_key)
        
        # If key exists, merge emails
        if key in seen:
            existing = seen[key]
            if business["emails"] != "N/A":
                existing_emails = set(existing["emails"].split(", ")) if existing["emails"] != "N/A" else set()
                new_emails = set(business["emails"].split(", "))
                merged_emails = existing_emails | new_emails
                existing["emails"] = ", ".join(sorted(merged_emails)) if merged_emails else "N/A"
        else:
            seen[key] = business
    
    return list(seen.values())

def scroll_results_panel(page, results_panel, timeout: int, start_time: float) -> int:
    """Scroll the results panel to load all businesses"""
    if not results_panel:
        logger.warning("Results panel is None, skipping scroll")
        return 0
    
    prev_height = 0
    scroll_count = 0
    no_change_count = 0
    
    for scroll_attempt in range(100):
        if time.time() - start_time > timeout:
            logger.warning(f"Global timeout reached ({timeout}s)")
            break
        
        try:
            # Scroll the results panel
            page.evaluate(
                "(panel) => panel.scrollTo(0, panel.scrollHeight)",
                results_panel
            )
            time.sleep(DELAY_BETWEEN_SCROLL)
            
            # Get current height
            curr_height = page.evaluate(
                "(panel) => panel.scrollHeight",
                results_panel
            )
            
            # Check if we've reached the end
            if curr_height == prev_height:
                no_change_count += 1
                if no_change_count >= 3:
                    logger.info("[OK] No more new results (confirmed after 3 checks)")
                    break
            else:
                no_change_count = 0
                scroll_count += 1
            
            prev_height = curr_height
            
            # Check for "You've reached the end" message
            try:
                end_text = page.query_selector('span:has-text("You\'ve reached the end")')
                if end_text:
                    logger.info("[OK] Reached end of list")
                    break
            except:
                pass
                
        except Exception as e:
            logger.debug(f"Scroll error: {e}")
            break
    
    return scroll_count

def extract_business_data(page) -> Dict:
    """Extract business data from the current detail view"""
    business = {
        "name": "N/A",
        "address": "N/A",
        "phone": "N/A",
        "website": "N/A",
        "emails": "N/A"
    }
    
    # Extract business name
    name_el = get_selector(page, BUSINESS_NAME_SELECTORS)
    if name_el:
        try:
            business["name"] = name_el.inner_text().strip()
        except Exception as e:
            logger.debug(f"Error extracting name: {e}")
    
    # Validate name
    if not business["name"] or business["name"].lower() in {"results", "overview", "about", "reviews", "n/a"}:
        return None
    
    # Extract contact info from buttons
    try:
        buttons = page.query_selector_all("button[data-item-id]")
        for btn in buttons:
            try:
                aria = btn.get_attribute("aria-label")
                if not aria:
                    continue
                
                aria_lower = aria.lower()
                
                if "address:" in aria_lower or "located at" in aria_lower:
                    business["address"] = aria.split(":", 1)[-1].strip()
                elif "phone:" in aria_lower or "call" in aria_lower:
                    phone_match = re.search(r'[\d\s\-\(\)\+]+', aria)
                    if phone_match:
                        business["phone"] = phone_match.group().strip()
                elif "website:" in aria_lower or aria.startswith("http"):
                    website_match = re.search(r'https?://[^\s]+', aria)
                    if website_match:
                        business["website"] = website_match.group().strip()
                    elif ":" in aria:
                        business["website"] = aria.split(":", 1)[-1].strip()
            except Exception as e:
                logger.debug(f"Error processing button: {e}")
                continue
    except Exception as e:
        logger.debug(f"Error extracting contact info: {e}")
    
    # Try alternative methods for contact info
    try:
        # Phone number
        if business["phone"] == "N/A":
            phone_selectors = [
                'button[data-item-id*="phone"]',
                'button[aria-label*="Phone"]',
                'div[data-tooltip*="phone" i]'
            ]
            for selector in phone_selectors:
                el = page.query_selector(selector)
                if el:
                    text = el.get_attribute("aria-label") or el.inner_text()
                    phone_match = re.search(r'[\d\s\-\(\)\+]{10,}', text)
                    if phone_match:
                        business["phone"] = phone_match.group().strip()
                        break
        
        # Website
        if business["website"] == "N/A":
            website_selectors = [
                'a[data-item-id*="authority"]',
                'button[data-item-id*="authority"]',
                'a[aria-label*="Website"]'
            ]
            for selector in website_selectors:
                el = page.query_selector(selector)
                if el:
                    href = el.get_attribute("href")
                    aria = el.get_attribute("aria-label")
                    
                    if href and href.startswith("http"):
                        business["website"] = href
                        break
                    elif aria:
                        website_match = re.search(r'https?://[^\s]+', aria)
                        if website_match:
                            business["website"] = website_match.group()
                            break
        
        # Address
        if business["address"] == "N/A":
            address_selectors = [
                'button[data-item-id*="address"]',
                'button[aria-label*="Address"]'
            ]
            for selector in address_selectors:
                el = page.query_selector(selector)
                if el:
                    aria = el.get_attribute("aria-label")
                    if aria and "address:" in aria.lower():
                        business["address"] = aria.split(":", 1)[-1].strip()
                        break
                    
    except Exception as e:
        logger.debug(f"Error in alternative extraction: {e}")
    
    return business

def extract_emails_from_website(page, website: str, business_name: str) -> Set[str]:
    """Extract emails from a business website"""
    emails = set()
    
    try:
        logger.debug(f"Extracting emails from: {website}")
        
        # Create new context for email extraction to avoid navigation issues
        new_page = page.context.new_page()
        
        try:
            new_page.goto(website, timeout=WEBSITE_LOAD_TIMEOUT, wait_until='domcontentloaded')
            time.sleep(2)
            
            # Get page content
            content = new_page.content()
            
            # Extract emails from HTML
            found_emails = extract_emails_from_text(content)
            
            # Try to find contact page
            contact_links = new_page.query_selector_all('a[href*="contact"], a[href*="about"]')
            
            for link in contact_links[:2]:  # Only check first 2 contact links
                try:
                    href = link.get_attribute("href")
                    if href and not href.startswith("javascript:") and not href.startswith("mailto:"):
                        if not href.startswith("http"):
                            href = website.rstrip('/') + '/' + href.lstrip('/')
                        
                        new_page.goto(href, timeout=10000, wait_until='domcontentloaded')
                        time.sleep(1)
                        
                        contact_content = new_page.content()
                        found_emails.update(extract_emails_from_text(contact_content))
                        
                        new_page.go_back(timeout=5000)
                        time.sleep(1)
                except Exception as e:
                    logger.debug(f"Error checking contact page: {e}")
                    continue
            
            emails = found_emails
            
        finally:
            new_page.close()
        
    except Exception as e:
        logger.debug(f"Email extraction failed for {business_name}: {e}")
    
    return emails

# ============================================
# MAIN SCRAPER
# ============================================

def main():
    # CLI Arguments
    parser = argparse.ArgumentParser(description="Google Maps Business Scraper (Fixed)")
    parser.add_argument("--keyword", required=True, help="Business keyword")
    parser.add_argument("--city", required=True, help="City / Area")
    parser.add_argument("--headless", action="store_true", help="Run browser in headless mode")
    parser.add_argument("--max-results", type=int, default=None, help="Maximum businesses to scrape")
    parser.add_argument("--no-emails", action="store_true", help="Skip email extraction")
    parser.add_argument("--timeout", type=int, default=600, help="Total timeout in seconds")
    parser.add_argument("--resume", action="store_true", help="Resume from last checkpoint")
    parser.add_argument("--verbose", action="store_true", help="Verbose logging")
    
    args = parser.parse_args()
    
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    
    KEYWORD = args.keyword
    CITY = args.city
    HEADLESS = args.headless
    MAX_RESULTS = args.max_results
    SKIP_EMAILS = args.no_emails
    TIMEOUT = args.timeout
    RESUME = args.resume
    
    query = f"{KEYWORD} in {CITY}"
    checkpoint_file = os.path.join(CHECKPOINT_DIR, f"{safe_filename(KEYWORD)}_{safe_filename(CITY)}.json")
    
    logger.info(f"Starting scraper for: {query}")
    logger.info(f"Config: headless={HEADLESS}, max_results={MAX_RESULTS}, skip_emails={SKIP_EMAILS}")
    
    start_time = time.time()
    businesses = []
    start_index = 0
    
    # Check for checkpoint
    if RESUME:
        checkpoint = load_checkpoint(checkpoint_file)
        if checkpoint:
            businesses = checkpoint["businesses"]
            start_index = checkpoint["index"]
    
    browser = None
    try:
        with sync_playwright() as p:
            logger.info(f"Browser launch params: headless={HEADLESS}")
            browser = p.chromium.launch(
                headless=HEADLESS,
                args=[
                    "--disable-dev-shm-usage",
                    "--disable-blink-features=AutomationControlled"
                ]
            )
            
            context = browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            )
            
            page = context.new_page()
            
            try:
                # Navigate to Google Maps
                logger.info("Loading Google Maps...")
                page.goto("https://www.google.com/maps", timeout=SEARCH_TIMEOUT, wait_until='domcontentloaded')
                
                # Wait for search box to appear (more reliable than networkidle)
                logger.info("Waiting for search box...")
                if not wait_for_selector(page, SEARCH_BOX_SELECTORS, 30000):
                    raise RuntimeError("Search box did not appear")
                
                time.sleep(2)
                
                # Search
                logger.info(f"Searching for: {query}")
                search_box = get_selector(page, SEARCH_BOX_SELECTORS)
                if not search_box:
                    raise RuntimeError("Could not find search box")
                
                # Clear any existing text and search
                search_box.click()
                time.sleep(0.5)
                page.keyboard.press("Control+A")
                time.sleep(0.2)
                search_box.fill(query)
                time.sleep(1.5)
                page.keyboard.press("Enter")
                
                # Wait for results panel
                logger.info("Waiting for results...")
                time.sleep(3)  # Give it time to start loading
                
                if not wait_for_selector(page, RESULTS_PANEL_SELECTORS, 20000):
                    # Try alternative: wait for business cards directly
                    logger.info("Results panel not found, checking for business cards...")
                    if not wait_for_selector(page, [BUSINESS_CARD_SELECTOR], 10000):
                        raise RuntimeError("No results found")
                
                results_panel = get_selector(page, RESULTS_PANEL_SELECTORS)
                if not results_panel:
                    logger.warning("Results panel not found, trying to proceed anyway...")
                    # Find results panel by looking for parent of business cards
                    first_card = page.query_selector(BUSINESS_CARD_SELECTOR)
                    if first_card:
                        results_panel = page.evaluate(
                            '(card) => card.closest(\'div[role="feed"]\') || card.closest(\'div[role="list"]\')',
                            first_card
                        )
                
                time.sleep(2)
                
                # Scroll to load all results
                logger.info("Scrolling results panel...")
                scroll_count = scroll_results_panel(page, results_panel, TIMEOUT, start_time)
                logger.info(f"[OK] Scrolling complete ({scroll_count} scrolls)")
                
                # Wait a bit for all cards to render
                time.sleep(2)
                
                # Collect all business links (hrefs) instead of elements
                all_cards = page.query_selector_all(BUSINESS_CARD_SELECTOR)
                business_hrefs = []
                
                for card in all_cards:
                    try:
                        href = card.get_attribute("href")
                        if href:
                            business_hrefs.append(href)
                    except:
                        continue
                
                total_businesses = len(business_hrefs)
                logger.info(f"[FOUND] Total businesses found: {total_businesses}")
                
                if MAX_RESULTS:
                    business_hrefs = business_hrefs[:MAX_RESULTS]
                    total_businesses = len(business_hrefs)
                
                # Process businesses by navigating to their URLs
                for index, href in enumerate(business_hrefs):
                    if time.time() - start_time > TIMEOUT:
                        logger.warning("Global timeout reached, saving progress...")
                        break
                    
                    if start_index > 0 and index < start_index:
                        continue
                    
                    try:
                        logger.info(f"\nProcessing {index + 1}/{total_businesses}...")
                        time.sleep(DELAY_BETWEEN_BUSINESS)
                        
                        # Navigate to business detail page
                        page.goto(href, timeout=BUSINESS_LOAD_TIMEOUT)
                        
                        # Wait for details to load
                        if not wait_for_selector(page, BUSINESS_NAME_SELECTORS, BUSINESS_LOAD_TIMEOUT):
                            logger.warning(f"Skipping business {index + 1}: details didn't load")
                            continue
                        
                        time.sleep(1.5)
                        
                        # Extract business data
                        business = extract_business_data(page)
                        
                        if not business:
                            logger.warning(f"Skipping invalid business at index {index + 1}")
                            continue
                        
                        # Extract emails if needed
                        emails = set()
                        if not SKIP_EMAILS and business["website"] != "N/A":
                            if not should_skip_email_extraction(business["website"]):
                                emails = extract_emails_from_website(page, business["website"], business["name"])
                                time.sleep(DELAY_AFTER_EMAIL_EXTRACTION)
                        
                        business["emails"] = ", ".join(sorted(emails)) if emails else "N/A"
                        
                        businesses.append(business)
                        logger.info(f"[OK] {index + 1}. {business['name']}")
                        logger.info(f"   Phone: {business['phone']}, Website: {business['website'][:50] if business['website'] != 'N/A' else 'N/A'}")
                        
                        # Save checkpoint every 10 businesses
                        if (index + 1) % 10 == 0:
                            save_checkpoint(checkpoint_file, businesses, index + 1)
                    
                    except Exception as e:
                        logger.error(f"❌ Failed at business {index + 1}: {e}")
                        continue
                
                logger.info(f"\nProcessing complete. Total collected: {len(businesses)}")
            
            finally:
                if browser:
                    browser.close()
        
        # Deduplication
        logger.info("Deduplicating businesses...")
        original_count = len(businesses)
        businesses = deduplicate_businesses(businesses)
        logger.info(f"[CLEANUP] Removed {original_count - len(businesses)} duplicates. Final count: {len(businesses)}")
        
        # Display sample
        if businesses:
            logger.info("\n[SAMPLE] Sample output (first 3):")
            for b in businesses[:3]:
                logger.info(f"  - {b['name']}")
                logger.info(f"    Address: {b['address']}")
                logger.info(f"    Phone: {b['phone']}")
                logger.info(f"    Website: {b['website']}")
                logger.info(f"    Emails: {b['emails']}")
        
        # Save CSV
        if businesses:
            csv_file = os.path.join(OUTPUT_DIR, f"{safe_filename(KEYWORD)}_{safe_filename(CITY)}_businesses.csv")
            with open(csv_file, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(
                    f,
                    fieldnames=["name", "address", "phone", "website", "emails"]
                )
                writer.writeheader()
                writer.writerows(businesses)
            
            logger.info(f"\n[SAVED] CSV saved -> {csv_file}")
            
            # Save JSON
            json_file = os.path.join(OUTPUT_DIR, f"{safe_filename(KEYWORD)}_{safe_filename(CITY)}_businesses.json")
            with open(json_file, "w", encoding="utf-8") as f:
                json.dump(businesses, f, indent=2, ensure_ascii=False)
            
            logger.info(f"[SAVED] JSON saved -> {json_file}")
            
            # Statistics summary
            with_phone = sum(1 for b in businesses if b["phone"] != "N/A")
            with_website = sum(1 for b in businesses if b["website"] != "N/A")
            with_email = sum(1 for b in businesses if b["emails"] != "N/A")
            
            logger.info("\n" + "="*50)
            logger.info("[SUMMARY] FINAL SUMMARY")
            logger.info("="*50)
            logger.info(f"Total businesses: {len(businesses)}")
            logger.info(f"With phone number: {with_phone} ({100*with_phone//len(businesses)}%)")
            logger.info(f"With website: {with_website} ({100*with_website//len(businesses)}%)")
            logger.info(f"With email: {with_email} ({100*with_email//len(businesses)}%)")
            logger.info("="*50)
        
            # Cleanup checkpoint on success
            if os.path.exists(checkpoint_file):
                os.remove(checkpoint_file)
                logger.info("Checkpoint cleaned up")
        else:
            logger.warning("No businesses were collected!")
        
        elapsed = time.time() - start_time
        logger.info(f"\n[TIME] Total time: {elapsed:.1f}s")
        logger.info(f"[LOG] Log file: {log_file}")
    
    except KeyboardInterrupt:
        logger.info("\n[WARNING] Scraper interrupted by user")
        if businesses:
            save_checkpoint(checkpoint_file, businesses, len(businesses))
            logger.info("Progress saved to checkpoint")
    except Exception as e:
        logger.exception(f"Fatal error: {e}")
        if businesses:
            save_checkpoint(checkpoint_file, businesses, len(businesses))
        raise

if __name__ == "__main__":
    main()