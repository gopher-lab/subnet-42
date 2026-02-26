#!/usr/bin/env python3
import json
import time
import os
import logging
import datetime
import platform
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.common.exceptions import WebDriverException
import random
import subprocess
import re
import shutil

from selenium.webdriver.common.keys import Keys
from dotenv import load_dotenv

# Setup logging first
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# Set output directory based on environment (robust to current working directory)
running_in_docker = os.environ.get("RUNNING_IN_DOCKER", "false").lower() == "true"
if running_in_docker:
    OUTPUT_DIR = "/app/cookies"
    logger.info(f"Docker environment detected, saving cookies to {OUTPUT_DIR}")
else:
    # Resolve project root as the parent of the directory containing this script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    OUTPUT_DIR = os.path.join(project_root, "cookies")
    logger.info(f"Local environment detected, saving cookies to {OUTPUT_DIR}")

# Ensure output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Cookie names we commonly observe from X sessions (informational).
COOKIE_NAMES = ["personalization_id", "kdt", "twid", "ct0", "auth_token", "att"]

# Twitter domains to handle - We will only use x.com
TWITTER_DOMAINS = ["x.com"]

# Twitter login URL
TWITTER_LOGIN_URL = "https://x.com/i/flow/login"

# Constants
POLLING_INTERVAL = 1  # Check every 1 second
WAITING_TIME = 300  # Wait up to 5 minutes for manual verification
CLICK_WAIT = 5  # Wait 5 seconds after clicking buttons
POST_LOGIN_COOKIE_WAIT = 15  # Wait up to 15s for session cookies
POST_LOGIN_COOKIE_POLL_INTERVAL = 1  # Poll every second
REQUIRED_SESSION_COOKIES = ["auth_token", "ct0"]

# Rate limit tracking
rate_limit_hits = {}  # Track rate limit hits per account for exponential backoff
MAX_BACKOFF_SECONDS = 300  # Max 5 minutes between attempts


def get_account_backoff(username):
    """Get current backoff time for an account based on recent rate limit hits."""
    hits = rate_limit_hits.get(username, 0)
    if hits == 0:
        return 0
    # Exponential backoff: 10s, 20s, 40s, 80s, 160s, 300s (max)
    backoff = min(10 * (2 ** (hits - 1)), MAX_BACKOFF_SECONDS)
    return backoff


def record_rate_limit_hit(username):
    """Record a rate limit hit for an account."""
    rate_limit_hits[username] = rate_limit_hits.get(username, 0) + 1
    backoff = get_account_backoff(username)
    logger.warning(
        f"Rate limit hit for {username}. This is attempt {rate_limit_hits[username]}. "
        f"Next backoff will be {backoff}s"
    )


def clear_rate_limit_hits(username):
    """Clear rate limit hits after successful login."""
    if username in rate_limit_hits:
        del rate_limit_hits[username]


def capture_screenshot(driver, username, reason="failure"):
    """Capture a screenshot for debugging when something goes wrong."""
    try:
        screenshots_dir = os.path.join(OUTPUT_DIR, "screenshots")
        os.makedirs(screenshots_dir, exist_ok=True)
        
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{username}_{reason}_{timestamp}.png"
        filepath = os.path.join(screenshots_dir, filename)
        
        driver.save_screenshot(filepath)
        logger.info(f"Screenshot saved: {filepath}")
        return filepath
    except Exception as e:
        logger.warning(f"Failed to capture screenshot: {str(e)}")
        return None


def is_rate_limited(driver):
    """Check if X/Twitter is showing rate limit or "too many attempts" messages."""
    try:
        rate_limit_indicators = [
            "rate limit",
            "too many attempts",
            "try again later",
            "unusual activity",
            "automated behavior",
            "suspicious activity",
            "temporarily locked",
            "account temporarily",
            "please wait",
            "slow down",
            "429",  # HTTP status sometimes shown
        ]
        
        page_text = driver.find_element(By.TAG_NAME, "body").text.lower()
        current_url = driver.current_url.lower()
        
        for indicator in rate_limit_indicators:
            if indicator in page_text or indicator in current_url:
                logger.warning(f"Rate limit detected: '{indicator}' found")
                return True
                
        # Check for specific error codes or states
        error_selectors = [
            '[data-testid="error"]', 
            '[role="alert"]',
            '.error-message',
            '[class*="error"]',
            '[class*="blocked"]',
        ]
        
        for selector in error_selectors:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                for elem in elements:
                    if elem.is_displayed():
                        text = elem.text.lower()
                        if any(ind in text for ind in rate_limit_indicators):
                            logger.warning(f"Rate limit element found: {text[:100]}")
                            return True
            except:
                pass
                
        return False
    except Exception as e:
        logger.debug(f"Error checking rate limit status: {str(e)}")
        return False


def get_future_date(days=7, hours=0, minutes=0, seconds=0):
    """
    Generate a slightly randomized ISO 8601 date string for a specified time in the future.

    Args:
        days: Number of days in the future
        hours: Number of hours to add
        minutes: Number of minutes to add
        seconds: Number of seconds to add

    Returns:
        ISO 8601 formatted date string with slight randomization
    """
    # Add slight randomization to make cookies appear more natural
    random_seconds = random.uniform(0, 3600)  # Random seconds (up to 1 hour)
    random_minutes = random.uniform(0, 60)  # Random minutes (up to 1 hour)

    future_date = datetime.datetime.now() + datetime.timedelta(
        days=days,
        hours=hours,
        minutes=minutes + random_minutes,
        seconds=seconds + random_seconds,
    )

    # Format in ISO 8601 format with timezone information
    return future_date.strftime("%Y-%m-%dT%H:%M:%SZ")


def create_cookie_template(name, value, domain="x.com", expires=None):
    """
    Create a standard cookie template with the given name and value.
    Note: Cookie values should not contain double quotes as they cause errors in Go's HTTP client.

    Args:
        name: Name of the cookie
        value: Value of the cookie
        domain: Domain for the cookie
        expires: Optional expiration date string in ISO 8601 format
    """
    # Ensure no quotes in cookie value to prevent HTTP header issues
    if value.startswith('"') and value.endswith('"'):
        value = value[1:-1]
    value = value.replace('"', "")

    # If no expiration date is provided, use the default "0001-01-01T00:00:00Z"
    if expires is None:
        expires = "0001-01-01T00:00:00Z"

    return {
        "Name": name,
        "Value": value,
        "Path": "",
        "Domain": domain,
        "Expires": expires,
        "RawExpires": "",
        "MaxAge": 0,
        "Secure": False,
        "HttpOnly": False,
        "SameSite": 0,
        "Raw": "",
        "Unparsed": None,
    }


def setup_realistic_profile(temp_profile):
    """Set up a more realistic browser profile with history and common extensions."""

    # Create history file structure
    history_dir = os.path.join(temp_profile, "Default")
    os.makedirs(history_dir, exist_ok=True)

    # Sample visited sites for history (just structure, not actual data)
    common_sites = [
        "google.com",
        "youtube.com",
        "facebook.com",
        "amazon.com",
        "wikipedia.org",
    ]

    # Create a dummy history file
    history_file = os.path.join(history_dir, "History")
    try:
        with open(history_file, "w") as f:
            # Just create an empty file to simulate history presence
            f.write("")

        # Create bookmark file with common sites
        bookmarks_file = os.path.join(history_dir, "Bookmarks")
        bookmarks_data = {
            "roots": {
                "bookmark_bar": {
                    "children": [
                        {"name": site, "url": f"https://{site}"}
                        for site in common_sites
                    ],
                    "date_added": str(int(time.time())),
                    "date_modified": str(int(time.time())),
                    "name": "Bookmarks Bar",
                    "type": "folder",
                }
            },
            "version": 1,
        }
        with open(bookmarks_file, "w") as f:
            json.dump(bookmarks_data, f)

        # Create preferences file with some realistic settings
        preferences_file = os.path.join(history_dir, "Preferences")
        preferences_data = {
            "browser": {
                "last_known_google_url": "https://www.google.com/",
                "last_prompted_google_url": "https://www.google.com/",
                "show_home_button": True,
                "custom_chrome_frame": False,
            },
            "homepage": "https://www.google.com",
            "session": {
                "restore_on_startup": 1,
                "startup_urls": [f"https://{random.choice(common_sites)}"],
            },
            "search": {"suggest_enabled": True},
            "translate": {"enabled": True},
        }
        with open(preferences_file, "w") as f:
            json.dump(preferences_data, f)

        logger.info("Created realistic browser profile with history and preferences")
    except Exception as e:
        logger.warning(f"Failed to create history files: {str(e)}")

    # Add a dummy extension folder to simulate common extensions
    ext_dir = os.path.join(temp_profile, "Default", "Extensions")
    os.makedirs(ext_dir, exist_ok=True)

    # Create dummy extension folders for common extensions
    common_extensions = [
        "aapbdbdomjkkjkaonfhkkikfgjllcleb",  # Google Translate
        "ghbmnnjooekpmoecnnnilnnbdlolhkhi",  # Google Docs
        "cjpalhdlnbpafiamejdnhcphjbkeiagm",  # uBlock Origin
    ]

    for ext_id in common_extensions:
        ext_path = os.path.join(ext_dir, ext_id)
        os.makedirs(ext_path, exist_ok=True)
        # Create a minimal manifest file
        manifest_path = os.path.join(ext_path, "manifest.json")
        try:
            with open(manifest_path, "w") as f:
                f.write("{}")
        except Exception as e:
            logger.warning(f"Failed to create extension manifest: {str(e)}")

    return temp_profile


def resolve_chrome_binary() -> str | None:
    """Resolve a usable Chrome binary path.

    Priority:
    1) CHROME_BINARY env var
    2) Known platform defaults
    3) First match on PATH
    """
    env_path = os.environ.get("CHROME_BINARY")
    if env_path and os.path.exists(env_path):
        return env_path

    # macOS default
    mac_path = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
    if os.path.exists(mac_path):
        return mac_path

    # Linux common paths
    for candidate in [
        shutil.which("google-chrome"),
        shutil.which("google-chrome-stable"),
        shutil.which("chromium"),
        shutil.which("chromium-browser"),
        shutil.which("chrome"),
    ]:
        if candidate and os.path.exists(candidate):
            return candidate

    # Windows typical locations (best-effort, user can set CHROME_BINARY)
    win_paths = [
        os.path.expandvars(r"%ProgramFiles%/Google/Chrome/Application/chrome.exe"),
        os.path.expandvars(r"%ProgramFiles(x86)%/Google/Chrome/Application/chrome.exe"),
        os.path.expandvars(r"%LocalAppData%/Google/Chrome/Application/chrome.exe"),
    ]
    for p in win_paths:
        if os.path.exists(p):
            return p

    return None


def get_chrome_major_version(chrome_binary: str | None) -> int | None:
    """Return local Chrome major version (e.g., 141) by invoking --version.

    If chrome_binary is None, attempts common commands on PATH.
    """
    candidates = []
    if chrome_binary:
        candidates.append([chrome_binary, "--version"])
    # Common CLI names
    for name in [
        "google-chrome",
        "google-chrome-stable",
        "chromium",
        "chromium-browser",
        "chrome",
    ]:
        exe = shutil.which(name)
        if exe:
            candidates.append([exe, "--version"])

    version_regex = re.compile(r"(Chrome|Chromium)\s+([0-9]+)\.")

    for cmd in candidates:
        try:
            out = subprocess.check_output(
                cmd, stderr=subprocess.STDOUT, text=True
            ).strip()
            m = version_regex.search(out)
            if m:
                return int(m.group(2))
        except Exception:
            continue

    return None


def kill_orphan_chrome_processes():
    """Kill any orphaned Chrome processes that might be locking the profile."""
    try:
        import signal
        # Find Chrome processes using undetected_chromedriver paths
        result = subprocess.run(
            ["pgrep", "-f", "undetected_chromedriver"],
            capture_output=True,
            text=True
        )
        if result.stdout.strip():
            pids = result.stdout.strip().split('\n')
            for pid in pids:
                try:
                    os.kill(int(pid), signal.SIGTERM)
                    logger.info(f"Killed orphaned chromedriver process: {pid}")
                except (ProcessLookupError, ValueError):
                    pass
    except Exception as e:
        logger.debug(f"Error checking for orphaned processes: {e}")


def clear_chromedriver_cache():
    """Clear undetected_chromedriver cache to force re-download of correct version."""
    try:
        import undetected_chromedriver as uc
        # Get the cache directory used by undetected_chromedriver
        cache_dir = os.path.expanduser("~/.undetected_chromedriver")
        if os.path.exists(cache_dir):
            logger.info(f"Clearing chromedriver cache: {cache_dir}")
            shutil.rmtree(cache_dir, ignore_errors=True)
        
        # Also clear the selenium webdriver cache
        selenium_cache = os.path.expanduser("~/.wdm")
        if os.path.exists(selenium_cache):
            logger.info(f"Clearing selenium webdriver cache: {selenium_cache}")
            shutil.rmtree(selenium_cache, ignore_errors=True)
            
        # Clear Library/Application Support path on macOS
        mac_cache = os.path.expanduser("~/Library/Application Support/undetected_chromedriver")
        if os.path.exists(mac_cache):
            logger.info(f"Clearing macOS chromedriver cache: {mac_cache}")
            shutil.rmtree(mac_cache, ignore_errors=True)
            
        logger.info("Chromedriver cache cleared successfully")
    except Exception as e:
        logger.warning(f"Could not clear chromedriver cache: {str(e)}")


def clear_profile_cache(profile_dir, clear_heavy_cache=False):
    """Clear profile lock files always; clear heavy caches only when requested."""
    cache_dirs = [
        os.path.join(profile_dir, "Default", "Cache"),
        os.path.join(profile_dir, "Default", "Code Cache"),
        os.path.join(profile_dir, "Default", "GPUCache"),
        os.path.join(profile_dir, "Default", "Service Worker"),
        os.path.join(profile_dir, "Default", "DawnCache"),
        os.path.join(profile_dir, "Default", "ShaderCache"),
        os.path.join(profile_dir, "GrShaderCache"),
        os.path.join(profile_dir, "ShaderCache"),
    ]
    
    # Also clear lock files that can cause issues
    lock_files = [
        os.path.join(profile_dir, "SingletonLock"),
        os.path.join(profile_dir, "SingletonCookie"),
        os.path.join(profile_dir, "SingletonSocket"),
    ]
    
    for lock_file in lock_files:
        try:
            if os.path.exists(lock_file):
                os.remove(lock_file)
                logger.debug(f"Removed lock file: {lock_file}")
        except Exception as e:
            logger.debug(f"Could not remove lock file {lock_file}: {e}")
    
    if clear_heavy_cache:
        for cache_dir in cache_dirs:
            try:
                if os.path.exists(cache_dir):
                    shutil.rmtree(cache_dir)
                    logger.debug(f"Cleared cache directory: {cache_dir}")
            except Exception as e:
                logger.debug(f"Could not clear cache {cache_dir}: {e}")


def setup_driver(username, aggressive_cleanup=False):
    """Set up and return an undetected Chrome driver with a persistent profile per account."""
    logger.info("Setting up undetected Chrome driver...")
    
    # Kill any orphaned Chrome processes first
    kill_orphan_chrome_processes()

    # Build a persistent per-account profile directory
    profile_dir = os.path.join(OUTPUT_DIR, "profiles", username)
    os.makedirs(profile_dir, exist_ok=True)

    # Always clear lock files; only clear heavy caches on recovery paths.
    clear_profile_cache(profile_dir, clear_heavy_cache=aggressive_cleanup)

    # Enhance profile with realistic history/bookmarks if it's a new profile
    if not os.path.exists(os.path.join(profile_dir, "Default")):
        try:
            setup_realistic_profile(profile_dir)
        except Exception as e:
            logger.warning(f"Failed to setup realistic profile bits: {e}")

    logger.info(f"Using persistent Chrome profile at: {profile_dir}")

    options = uc.ChromeOptions()

    # Essentials only; avoid suspicious flags
    options.add_argument(f"--user-data-dir={profile_dir}")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    
    # CRITICAL: Disable blink features that expose automation
    # This makes navigator.webdriver return undefined instead of true
    # Note: excludeSwitches doesn't work with undetected_chromedriver, 
    # so we rely on the CDP script injection below
    options.add_argument("--disable-blink-features=AutomationControlled")
    
    # Additional anti-detection measures via arguments
    # These are safer than experimental options with undetected_chromedriver
    options.add_argument("--disable-automation")
    options.add_argument("--disable-web-security")
    options.add_argument("--disable-features=IsolateOrigins,site-per-process")
    options.add_argument("--disable-blink-features=AutomationControlled")
    
    # Performance and stability improvements
    options.add_argument("--disable-background-networking")
    options.add_argument("--disable-background-timer-throttling")
    options.add_argument("--disable-backgrounding-occluded-windows")
    options.add_argument("--disable-renderer-backgrounding")
    options.add_argument("--disable-features=TranslateUI")
    options.add_argument("--disable-sync")
    options.add_argument("--disable-extensions-except=")
    options.add_argument("--disable-default-apps")
    options.add_argument("--no-first-run")
    options.add_argument("--no-default-browser-check")
    
    # Reduce memory/resource usage
    options.add_argument("--disable-component-update")
    options.add_argument("--disable-domain-reliability")

    # Resolve Chrome binary path
    chrome_binary = resolve_chrome_binary()
    if chrome_binary:
        options.binary_location = chrome_binary
        logger.info(f"Using Chrome binary: {chrome_binary}")
    else:
        logger.warning("Could not resolve Chrome binary. Relying on system default.")

    # Determine local Chrome major version
    env_force_version = os.environ.get("UC_FORCE_VERSION_MAIN")
    detected_major = None
    if env_force_version and env_force_version.isdigit():
        detected_major = int(env_force_version)
        logger.info(
            f"UC_FORCE_VERSION_MAIN set; forcing driver for Chrome {detected_major}"
        )
    else:
        detected_major = get_chrome_major_version(chrome_binary)
        if detected_major:
            logger.info(f"Detected local Chrome major version: {detected_major}")

    # Randomize viewport size a bit
    width = random.randint(1050, 1200)
    height = random.randint(800, 950)
    options.add_argument(f"--window-size={width},{height}")

    # Language header
    options.add_argument("--accept-lang=en-US,en;q=0.9")

    # Proxy support (VPN/backends)
    proxy_http = os.environ.get("http_proxy")
    proxy_https = os.environ.get("https_proxy")
    if proxy_http or proxy_https:
        proxy_to_use = proxy_http or proxy_https
        logger.info(f"Detected proxy settings: {proxy_to_use}")
        if proxy_to_use.startswith("http://"):
            proxy_to_use = proxy_to_use[7:]
        options.add_argument(f"--proxy-server={proxy_to_use}")
        options.add_argument("--ignore-certificate-errors")

    # Prefer a modern, real Chrome UA; CDP override will ensure full hints
    # We'll derive version from driver after launch for consistency
    driver = None
    version_retry_count = 0
    max_version_retries = 2
    
    while version_retry_count < max_version_retries:
        try:
            logger.info(f"Initializing undetected Chrome driver (attempt {version_retry_count + 1})...")
            
            # Force kill any existing Chrome processes before creating new driver
            kill_orphan_chrome_processes()
            
            if detected_major:
                logger.info(f"Requesting ChromeDriver for Chrome version {detected_major}")
                driver = uc.Chrome(options=options, version_main=detected_major)
            else:
                driver = uc.Chrome(options=options)
                
            logger.info("Successfully initialized undetected Chrome driver")
            break  # Success - exit the retry loop
            
        except TypeError as te:
            if "version_main" in str(te):
                logger.warning(
                    "Your undetected_chromedriver is outdated and lacks version_main. "
                    "Please upgrade: pip install -U undetected-chromedriver"
                )
                driver = uc.Chrome(options=options)
                break
            else:
                raise
                
        except WebDriverException as we:
            error_msg = str(we).lower()
            
            # Check for version mismatch error
            if "only supports chrome version" in error_msg or "session not created" in error_msg:
                version_retry_count += 1
                
                if version_retry_count < max_version_retries:
                    logger.warning(
                        f"ChromeDriver version mismatch detected. "
                        f"Clearing cache and retrying (attempt {version_retry_count + 1}/{max_version_retries})..."
                    )
                    
                    # Clear the chromedriver cache to force re-download
                    clear_chromedriver_cache()
                    
                    # Small delay before retry
                    time.sleep(2)
                    
                    # Continue to next iteration
                    continue
                else:
                    logger.error("ChromeDriver version mismatch persists after clearing cache")
                    # Try one more time without specifying version_main
                    try:
                        logger.info("Trying without version specification...")
                        driver = uc.Chrome(options=options)
                        logger.info("Successfully initialized undetected Chrome driver (fallback)")
                        break
                    except Exception as fallback_e:
                        logger.error(f"Fallback initialization also failed: {str(fallback_e)}")
                        raise
            else:
                # Not a version error - re-raise
                raise
    
    if driver is None:
        raise RuntimeError("Failed to initialize Chrome driver after all retries")

    # CRITICAL: Use CDP to hide navigator.webdriver flag
    # This is essential to bypass X/Twitter's bot detection
    try:
        driver.execute_cdp_cmd(
            "Page.addScriptToEvaluateOnNewDocument",
            {
                "source": """
                    Object.defineProperty(navigator, 'webdriver', {
                        get: () => undefined
                    });
                    
                    // Also hide other automation indicators
                    Object.defineProperty(navigator, 'plugins', {
                        get: () => [1, 2, 3, 4, 5]
                    });
                    
                    // Hide Chrome's automation extension
                    window.chrome = {
                        runtime: {}
                    };
                    
                    // Remove CDC properties that ChromeDriver adds
                    Object.defineProperty(window, 'cdc_adoQpoasnfa76pfcZLmcfl_Array', {
                        get: () => undefined
                    });
                    Object.defineProperty(window, 'cdc_adoQpoasnfa76pfcZLmcfl_Promise', {
                        get: () => undefined
                    });
                    Object.defineProperty(window, 'cdc_adoQpoasnfa76pfcZLmcfl_Symbol', {
                        get: () => undefined
                    });
                """
            },
        )
        logger.info("Successfully injected anti-detection script via CDP")
    except Exception as e:
        logger.warning(f"Failed to inject anti-detection script: {str(e)}")

    # Derive browser version to craft consistent Client Hints
    browser_version = None
    try:
        caps = getattr(driver, "capabilities", {}) or {}
        browser_version = caps.get("browserVersion") or caps.get("version")
    except Exception:
        pass
    if not browser_version:
        browser_version = "126.0.6478.61"
    major_version = browser_version.split(".")[0]

    # Compose realistic UA (macOS Sonoma-ish)
    ua_string = (
        f"Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) "
        f"AppleWebKit/537.36 (KHTML, like Gecko) "
        f"Chrome/{browser_version} Safari/537.36"
    )

    # Client Hints with brands and fullVersionList
    brands = [
        {"brand": "Not.A/Brand", "version": "24"},
        {"brand": "Chromium", "version": major_version},
        {"brand": "Google Chrome", "version": major_version},
    ]
    full_version_list = [
        {"brand": "Not.A/Brand", "version": "24.0.0.0"},
        {"brand": "Chromium", "version": browser_version},
        {"brand": "Google Chrome", "version": browser_version},
    ]
    cpu_arch = platform.machine().lower()
    if "arm" in cpu_arch or "aarch64" in cpu_arch:
        client_hint_arch = "arm"
    else:
        client_hint_arch = "x86"

    try:
        driver.execute_cdp_cmd(
            "Network.setUserAgentOverride",
            {
                "userAgent": ua_string,
                "platform": "macOS",
                "userAgentMetadata": {
                    "brands": brands,
                    "fullVersionList": full_version_list,
                    "platform": "macOS",
                    "platformVersion": "14.5.0",
                    "architecture": client_hint_arch,
                    "model": "",
                    "mobile": False,
                    "bitness": "64",
                },
            },
        )
    except Exception as e:
        logger.warning(f"Failed to set UA override via CDP: {str(e)}")

    # Timezone override only when explicitly provided to avoid proxy/locale mismatch.
    timezone_id = os.environ.get("TIMEZONE_ID")
    if timezone_id:
        try:
            driver.execute_cdp_cmd(
                "Emulation.setTimezoneOverride", {"timezoneId": timezone_id}
            )
        except Exception as e:
            logger.warning(f"Failed to set timezone override: {str(e)}")

    # Geolocation override only when explicitly provided to avoid mismatched signals.
    geo_lat = os.environ.get("GEO_LAT")
    geo_lon = os.environ.get("GEO_LON")
    if geo_lat and geo_lon:
        try:
            lat = float(geo_lat)
            lon = float(geo_lon)
            acc = float(os.environ.get("GEO_ACC", "100"))
            # Grant permission for Twitter origin
            try:
                driver.execute_cdp_cmd(
                    "Browser.grantPermissions",
                    {"permissions": ["geolocation"], "origin": "https://x.com"},
                )
            except Exception:
                pass
            driver.execute_cdp_cmd(
                "Emulation.setGeolocationOverride",
                {"latitude": lat, "longitude": lon, "accuracy": acc},
            )
        except Exception as e:
            logger.warning(f"Failed to set geolocation override: {str(e)}")

    return driver


def human_like_typing(element, text):
    """Simulate human-like typing with random delays between keypresses."""
    for char in text:
        element.send_keys(char)
        time.sleep(random.uniform(0.05, 0.25))  # Random delay between keypresses


def find_and_fill_input(driver, input_type, value):
    """Find and fill an input field of a specific type."""
    selectors = {
        "username": [
            'input[autocomplete="username"]',
            'input[name="text"]',
            'input[name="username"]',
            'input[placeholder*="username" i]',
            'input[placeholder*="phone" i]',
            'input[placeholder*="email" i]',
        ],
        "password": [
            'input[type="password"]',
            'input[name="password"]',
            'input[placeholder*="password" i]',
        ],
        "email": [
            'input[type="email"]',
            'input[name="email"]',
            'input[placeholder*="email" i]',
            'input[autocomplete="email"]',
        ],
        "phone": [
            'input[type="tel"]',
            'input[name="phone"]',
            'input[placeholder*="phone" i]',
            'input[autocomplete="tel"]',
        ],
        "code": [
            'input[autocomplete="one-time-code"]',
            'input[name="code"]',
            'input[placeholder*="code" i]',
            'input[placeholder*="verification" i]',
        ],
    }

    if input_type not in selectors:
        logger.warning(f"Unknown input type: {input_type}")
        return False

    input_found = False

    for selector in selectors[input_type]:
        try:
            inputs = driver.find_elements(By.CSS_SELECTOR, selector)
            for input_field in inputs:
                if input_field.is_displayed():
                    # Clear the field first (sometimes needed)
                    try:
                        input_field.clear()
                    except:
                        pass

                    # Type the value
                    human_like_typing(input_field, value)
                    logger.info(f"Filled {input_type} field with value: {value}")

                    # Add a small delay after typing
                    time.sleep(random.uniform(0.5, 1.5))
                    input_found = True
                    return True
        except Exception as e:
            logger.debug(
                f"Couldn't find or fill {input_type} field with selector {selector}: {str(e)}"
            )

    if not input_found:
        logger.info(f"No {input_type} input field found")

    return False


def click_next_button(driver):
    """Try to click a 'Next' or submit button."""
    button_clicked = False

    # Try buttons with "Next" text
    try:
        next_buttons = driver.find_elements(
            By.XPATH, '//*[contains(text(), "Next") or contains(text(), "next")]'
        )
        for button in next_buttons:
            if button.is_displayed():
                button.click()
                logger.info("Clicked Next button by text")
                button_clicked = True
                break
    except Exception as e:
        logger.debug(f"Couldn't click Next button by text: {str(e)}")

    # Try buttons with "Continue" text
    if not button_clicked:
        try:
            continue_buttons = driver.find_elements(
                By.XPATH,
                '//*[contains(text(), "Continue") or contains(text(), "continue")]',
            )
            for button in continue_buttons:
                if button.is_displayed():
                    button.click()
                    logger.info("Clicked Continue button by text")
                    button_clicked = True
                    break
        except Exception as e:
            logger.debug(f"Couldn't click Continue button by text: {str(e)}")

    # Try buttons with "Log in" or "Sign in" text
    if not button_clicked:
        try:
            login_buttons = driver.find_elements(
                By.XPATH,
                '//*[contains(text(), "Log in") or contains(text(), "Login") or contains(text(), "Sign in")]',
            )
            for button in login_buttons:
                if button.is_displayed():
                    button.click()
                    logger.info("Clicked Login button by text")
                    button_clicked = True
                    break
        except Exception as e:
            logger.debug(f"Couldn't click Login button by text: {str(e)}")

    # Try generic button elements by role
    if not button_clicked:
        try:
            buttons = driver.find_elements(By.CSS_SELECTOR, 'div[role="button"]')
            for button in buttons:
                if button.is_displayed():
                    button.click()
                    logger.info("Clicked button by role")
                    button_clicked = True
                    break
        except Exception as e:
            logger.debug(f"Couldn't click button by role: {str(e)}")

    # Try submitting the form with Enter key (last resort)
    if not button_clicked:
        try:
            active_element = driver.switch_to.active_element
            active_element.send_keys(Keys.ENTER)
            logger.info("Pressed Enter key on active element")
            button_clicked = True
        except Exception as e:
            logger.debug(f"Couldn't press Enter key: {str(e)}")

    return button_clicked


def is_logged_in(driver):
    """Check if user is logged in to Twitter."""
    try:
        current_url = driver.current_url.lower()
        logger.debug(f"Checking login status, current URL: {current_url}")

        # URL check (most reliable) - check for /home anywhere in URL
        if "/home" in current_url and ("twitter.com" in current_url or "x.com" in current_url):
            logger.info(f"Detected logged in via URL: {current_url}")
            return True
        
        # Also check if we're on the main feed (sometimes URL is just x.com/)
        if current_url.rstrip('/') in ["https://x.com", "https://twitter.com"]:
            # Check if we see home timeline elements
            pass  # Fall through to element checks

        # Home timeline check
        home_timeline = driver.find_elements(
            By.CSS_SELECTOR, 'div[aria-label="Timeline: Your Home Timeline"]'
        )
        if home_timeline and any(elem.is_displayed() for elem in home_timeline):
            logger.info("Detected logged in via home timeline element")
            return True

        # Tweet/Post button check - updated selectors for current X.com
        tweet_buttons = driver.find_elements(
            By.CSS_SELECTOR,
            'a[data-testid="SideNav_NewTweet_Button"], [data-testid="tweetButtonInline"], a[href="/compose/post"], [data-testid="SideNav_NewPost_Button"]',
        )
        if tweet_buttons and any(btn.is_displayed() for btn in tweet_buttons):
            logger.info("Detected logged in via tweet/post button")
            return True

        # Navigation elements check - updated selectors
        nav_elements = driver.find_elements(
            By.CSS_SELECTOR,
            'nav[role="navigation"], a[data-testid="AppTabBar_Home_Link"], a[href="/home"]',
        )
        if nav_elements and any(elem.is_displayed() for elem in nav_elements):
            logger.info("Detected logged in via navigation elements")
            return True
        
        # Check for the main timeline/feed area (generic check)
        main_content = driver.find_elements(
            By.CSS_SELECTOR,
            '[data-testid="primaryColumn"], main[role="main"]'
        )
        # Also check we're not on login flow
        login_elements = driver.find_elements(
            By.CSS_SELECTOR,
            'input[name="text"], input[name="password"], [data-testid="LoginForm"]'
        )
        if main_content and any(elem.is_displayed() for elem in main_content):
            if not (login_elements and any(elem.is_displayed() for elem in login_elements)):
                logger.info("Detected logged in via main content area (no login form visible)")
                return True

        return False
    except Exception as e:
        logger.error(f"Error checking login status: {str(e)}")
        return False


def needs_verification(driver):
    """Check if the page is showing a verification or authentication screen."""
    try:
        # Check for verification text
        verification_texts = [
            "Authenticate your account",
            "Enter your phone number",
            "Enter your email",
            "Check your phone",
            "Check your email",
            "Verification code",
            "verify your identity",
            "unusual login activity",
            "suspicious activity",
            "Help us keep your account safe",
            "Verify your identity",
            "keep your account safe",
        ]

        for text in verification_texts:
            try:
                elements = driver.find_elements(
                    By.XPATH, f"//*[contains(text(), '{text}')]"
                )
                if elements and any(elem.is_displayed() for elem in elements):
                    logger.info(f"Verification needed: Found text '{text}'")
                    return True
            except:
                pass

        # Check for verification URLs
        current_url = driver.current_url.lower()
        verification_url_patterns = [
            "verify",
            "challenge",
            "confirm",
            "auth",
            "login_challenge",
        ]

        for pattern in verification_url_patterns:
            if pattern in current_url:
                logger.info(f"Verification needed: URL contains '{pattern}'")
                return True

        return False
    except Exception as e:
        logger.error(f"Error checking for verification: {str(e)}")
        return False


def is_account_locked_or_suspended(driver):
    """Check if the account is locked, suspended, or disabled."""
    try:
        lockout_indicators = [
            "account suspended",
            "account locked",
            "account disabled",
            "permanently suspended",
            "temporarily locked",
            "unusual activity detected",
            "automated behavior",
            "captcha",
            "prove you're not a robot",
            "verify you're human",
            "phone verification required",
            "additional verification required",
            "security check",
        ]
        
        page_text = driver.find_element(By.TAG_NAME, "body").text.lower()
        current_url = driver.current_url.lower()
        
        for indicator in lockout_indicators:
            if indicator in page_text or indicator in current_url:
                logger.error(f"Account appears locked/suspended: '{indicator}' detected")
                return True
                
        # Check for specific locked account elements
        lockout_selectors = [
            '[data-testid="lockedAccount"]',
            '[data-testid="suspendedAccount"]',
            '[class*="suspended"]',
            '[class*="locked"]',
            '[class*="captcha"]',
            'iframe[src*="captcha"]',
            'iframe[src*="recaptcha"]',
        ]
        
        for selector in lockout_selectors:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                for elem in elements:
                    if elem.is_displayed():
                        logger.error(f"Lockout/suspension element found: {selector}")
                        return True
            except:
                pass
        
        return False
    except Exception as e:
        logger.debug(f"Error checking account lockout status: {str(e)}")
        return False


def extract_email_from_password(password):
    """Extract email from password assuming format 'himynameis<name>'."""
    # Get base email from environment variable - required
    base_email = os.environ.get("TWITTER_EMAIL")
    if not base_email:
        logger.error("TWITTER_EMAIL environment variable not set. This is required.")
        # Return a placeholder that will likely fail but doesn't expose personal info
        return "email_not_configured@example.com"

    # Extract the username part from base email for plus addressing
    base_username = base_email.split("@")[0]
    domain = base_email.split("@")[1]

    try:
        # Check if password starts with 'himynameis' or 'himynamewas'
        if password.startswith("himynameis"):
            name = password[10:]  # Extract everything after 'himynameis'
            return f"{base_username}+{name}@{domain}"
        elif password.startswith("himynamewas"):
            name = password[11:]  # Extract everything after 'himynamewas'
            return f"{base_username}+{name}@{domain}"
    except:
        pass

    # Fall back to the base email
    return base_email


def extract_cookies(driver, log_cookie_names=True):
    """Extract cookies from the browser."""
    logger.info("Extracting cookies")
    browser_cookies = driver.get_cookies()
    logger.info(f"Found {len(browser_cookies)} cookies total")

    cookie_values = {}
    # Always use x.com domain, no conditional check
    used_domain = "x.com"

    for cookie in browser_cookies:
        value = cookie["value"]
        if value.startswith('"') and value.endswith('"'):
            value = value[1:-1]  # Remove surrounding quotes
        value = value.replace('"', "")  # Replace any remaining quotes

        cookie_values[cookie["name"]] = value
        if log_cookie_names:
            logger.info(f"Found cookie: {cookie['name']}")

    # Log missing required cookies (critical for scraper auth).
    missing_cookies = [
        name for name in REQUIRED_SESSION_COOKIES if name not in cookie_values
    ]
    if missing_cookies:
        logger.warning(f"Missing required cookies: {', '.join(missing_cookies)}")
    else:
        logger.info("All required cookies found")

    return cookie_values, used_domain


def wait_for_required_session_cookies(driver, timeout_seconds=POST_LOGIN_COOKIE_WAIT):
    """Wait for required session cookies to appear after login."""
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        cookie_values, _ = extract_cookies(driver, log_cookie_names=False)
        missing_required = [
            name
            for name in REQUIRED_SESSION_COOKIES
            if not cookie_values.get(name)
        ]
        if not missing_required:
            logger.info(
                f"Required session cookies are present: {', '.join(REQUIRED_SESSION_COOKIES)}"
            )
            return cookie_values

        logger.info(
            f"Waiting for required session cookies: {', '.join(missing_required)}"
        )
        time.sleep(POST_LOGIN_COOKIE_POLL_INTERVAL)

    logger.warning(
        f"Timed out waiting for required session cookies: {', '.join(REQUIRED_SESSION_COOKIES)}"
    )
    cookie_values, _ = extract_cookies(driver)
    return cookie_values


def generate_cookies_json(cookie_values, domain="x.com"):
    """Generate the cookies JSON from the provided cookie values."""
    # Always use x.com domain regardless of what's passed in
    domain = "x.com"
    logger.info(f"Generating cookies JSON for domain: {domain}")

    # Determine expiration dates for different cookie types
    one_week_future = get_future_date(days=7)
    one_month_future = get_future_date(days=30)

    cookies = []
    
    # Process all found cookies
    for name, value in cookie_values.items():
        if value == "":
            logger.warning(f"Using empty string for cookie: {name}")

        # Set appropriate expiration date based on cookie type
        if name in ["personalization_id", "kdt"]:
            # 1 month expiration for these cookies
            expires = one_month_future
            logger.debug(f"Setting {name} cookie to expire in 1 month: {expires}")
        elif name in ["auth_token", "ct0"]:
            # 1 week expiration for these cookies
            expires = one_week_future
            logger.debug(f"Setting {name} cookie to expire in 1 week: {expires}")
        else:
            # Default 1 week for all other cookies
            expires = one_week_future
            logger.debug(
                f"Setting {name} cookie to default expiration (1 week): {expires}"
            )

        cookies.append(create_cookie_template(name, value, domain, expires))
    
    return cookies


def is_page_stuck_loading(driver):
    """Detect if the page is stuck showing a loading spinner."""
    try:
        # Check for loading spinners or progress indicators
        spinner_selectors = [
            'svg[style*="animation"]',  # Animated SVG spinners
            '[role="progressbar"]',
            '.loading',
            '[class*="spinner"]',
            '[class*="loading"]',
            # X/Twitter specific selectors
            'svg circle[cx]',  # Common spinner pattern
            '[data-testid="loading"]',
        ]
        
        for selector in spinner_selectors:
            try:
                spinners = driver.find_elements(By.CSS_SELECTOR, selector)
                for spinner in spinners:
                    if spinner.is_displayed():
                        # Check if it's actually visible in viewport
                        try:
                            rect = driver.execute_script(
                                "return arguments[0].getBoundingClientRect();", spinner
                            )
                            if rect["width"] > 20 and rect["height"] > 20:
                                return True
                        except:
                            pass
            except:
                pass
        
        # Check for "Loading..." text or similar
        loading_texts = ["Loading", "loading", "Please wait", "please wait"]
        for text in loading_texts:
            try:
                elements = driver.find_elements(
                    By.XPATH, f"//*[contains(text(), '{text}')]"
                )
                if any(e.is_displayed() for e in elements):
                    return True
            except:
                pass
        
        return False
    except Exception as e:
        logger.debug(f"Error checking for stuck loading: {str(e)}")
        return False


def wait_for_login_page_ready(driver, max_wait=30, refresh_on_stuck=True):
    """Wait until login page controls are interactive.
    
    Args:
        driver: The Chrome driver instance
        max_wait: Maximum time to wait in seconds
        refresh_on_stuck: If True, refresh the page if it appears stuck loading
    """
    wait_start = time.time()
    stuck_check_interval = 5  # Check for stuck state every 5 seconds
    last_stuck_check = wait_start
    is_stuck = False
    js_ready_checks = 0  # Track how many times we've seen JS ready
    
    while time.time() - wait_start < max_wait:
        try:
            ready_state = driver.execute_script("return document.readyState")
            
            # Check for stuck loading state periodically
            if refresh_on_stuck and time.time() - last_stuck_check >= stuck_check_interval:
                if is_page_stuck_loading(driver):
                    logger.warning("Page appears stuck on loading spinner, refreshing...")
                    is_stuck = True
                    break  # Exit loop to trigger refresh
                last_stuck_check = time.time()
            
            # Check for login form elements
            login_elements = driver.find_elements(
                By.CSS_SELECTOR,
                'input[name="text"], div[role="button"], form[data-testid="LoginForm"]',
            )
            
            # Also check if React/Vue apps have mounted by looking for dynamic content
            js_complete = driver.execute_script("""
                // Check if any JS frameworks have rendered content
                var reactRoot = document.querySelector('[data-reactroot]');
                var vueApp = document.querySelector('[data-v-app]') || document.querySelector('#app');
                var mainContent = document.querySelector('main') || document.querySelector('[role="main"]');
                var bodyContent = document.body && document.body.innerHTML.length > 500;
                
                // Return true if we have meaningful content
                return !!(reactRoot || vueApp || mainContent || bodyContent);
            """)
            
            if ready_state == "complete" and any(
                elem.is_displayed() for elem in login_elements if login_elements
            ):
                # Require JS to be ready multiple times to avoid false positives
                if js_complete:
                    js_ready_checks += 1
                    if js_ready_checks >= 2:  # Must be ready 2 consecutive checks
                        if is_stuck:
                            logger.info("Page recovered from stuck state and is now ready")
                        logger.info(f"Login page ready after {(time.time() - wait_start):.1f}s")
                        return True
                else:
                    js_ready_checks = 0  # Reset if JS not ready
                
        except WebDriverException as e:
            if "no such window" in str(e).lower() or "no such session" in str(e).lower():
                raise
            logger.warning(f"Error checking page load: {str(e)}")
        time.sleep(0.5)
    
    # If we detected stuck state, return False so caller can refresh
    if is_stuck:
        return False
    
    return False


def wait_for_post_click_transition(driver, previous_url, timeout=CLICK_WAIT):
    """Wait for login UI to advance and return as soon as it does."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            current_url = driver.current_url
            if current_url != previous_url:
                return True

            markers = driver.find_elements(
                By.CSS_SELECTOR,
                'input[name="password"], input[type="password"], input[name="text"], input[type="tel"], input[type="email"], input[autocomplete="one-time-code"], form[data-testid="LoginForm"]',
            )
            if markers and any(elem.is_displayed() for elem in markers):
                return True
        except WebDriverException as e:
            if "no such window" in str(e).lower() or "no such session" in str(e).lower():
                raise
        time.sleep(0.25)
    return False


def human_like_post_action_pause():
    """Small natural pause after a user action."""
    time.sleep(random.uniform(0.35, 0.9))


def safe_navigate(driver, url, max_retries=3, retry_delay=2):
    """Navigate to URL with retry logic for transient network errors."""
    for attempt in range(1, max_retries + 1):
        try:
            driver.get(url)
            return True
        except WebDriverException as e:
            error_str = str(e).lower()
            
            # Check for network-related errors that warrant retry
            network_errors = [
                "net::err_name_not_resolved",
                "net::err_internet_disconnected", 
                "net::err_connection_reset",
                "net::err_connection_refused",
                "net::err_connection_timed_out",
                "net::err_timed_out",
                "dns",
                "timeout",
                "unable to connect",
            ]
            
            is_network_error = any(err in error_str for err in network_errors)
            
            if is_network_error and attempt < max_retries:
                logger.warning(
                    f"Network error on attempt {attempt}/{max_retries}: {str(e)[:100]}. "
                    f"Retrying in {retry_delay}s..."
                )
                time.sleep(retry_delay * attempt)  # Increasing backoff
            else:
                # Non-network error or last attempt - re-raise
                raise
    
    return False


def process_account_state_machine(driver, username, password):
    """Process an account using a state machine approach with continuous polling."""
    logger.info(f"==========================================")
    logger.info(f"Starting to process account: {username}")
    output_file = f"{username}_twitter_cookies.json"

    # Create output directory if it doesn't exist
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Extract email from password if needed for verification
    email = extract_email_from_password(password)
    logger.info(f"Using email {email} for account {username}")

    # Navigate to login page with short in-session retries before full restart.
    try:
        nav_ready = False
        nav_start = time.time()
        for nav_attempt in range(1, 4):
            attempt_start = time.time()
            
            # Strategy: Try different entry points to bypass bot detection
            if nav_attempt == 1:
                # Attempt 1: Direct login URL
                logger.info("Attempt 1: Navigating directly to login URL")
                safe_navigate(driver, TWITTER_LOGIN_URL, max_retries=3)
            elif nav_attempt == 2:
                # Attempt 2: Visit home page first, then click login
                logger.info("Attempt 2: Visiting home page first, then navigating to login")
                try:
                    safe_navigate(driver, "https://x.com", max_retries=3)
                    time.sleep(3)  # Let home page JS execute
                    
                    # Look for and click login button on home page
                    login_buttons = driver.find_elements(
                        By.CSS_SELECTOR, 
                        'a[href="/login"], a[href="/i/flow/login"], [data-testid="loginButton"]'
                    )
                    if login_buttons:
                        for btn in login_buttons:
                            if btn.is_displayed():
                                logger.info("Found login button on home page, clicking...")
                                btn.click()
                                time.sleep(2)
                                break
                    else:
                        # No login button found, navigate directly
                        logger.info("No login button found, navigating to login URL")
                        safe_navigate(driver, TWITTER_LOGIN_URL, max_retries=3)
                except Exception as e:
                    logger.warning(f"Home page approach failed: {str(e)}, trying direct URL")
                    safe_navigate(driver, TWITTER_LOGIN_URL, max_retries=3)
            else:
                # Attempt 3: Try with a different user agent string
                logger.info("Attempt 3: Trying alternative navigation approach")
                safe_navigate(driver, TWITTER_LOGIN_URL, max_retries=3)
            
            # Wait for page with stuck-detection
            if wait_for_login_page_ready(driver, max_wait=30, refresh_on_stuck=True):
                logger.info(
                    f"Login page loaded successfully (attempt {nav_attempt}, "
                    f"{time.time() - attempt_start:.1f}s)"
                )
                nav_ready = True
                break
            
            # If we got here, page may be stuck or not ready - try refresh
            logger.warning(
                f"Login page not ready on attempt {nav_attempt} "
                f"({time.time() - attempt_start:.1f}s), refreshing..."
            )
            
            # Try refreshing the page before giving up on this attempt
            if nav_attempt < 3:
                # CRITICAL: If we detected stuck spinner, the profile may be poisoned
                # Clear it completely and restart browser with fresh profile
                if is_page_stuck_loading(driver):
                    logger.warning(
                        "Detected stuck loading spinner - profile may be poisoned by bot detection. "
                        "Clearing profile and restarting browser..."
                    )
                    try:
                        # Close current browser
                        driver.quit()
                    except:
                        pass
                    
                    # Clear the entire profile directory
                    try:
                        profile_dir = os.path.join(OUTPUT_DIR, "profiles", username)
                        if os.path.exists(profile_dir):
                            logger.info(f"Removing poisoned profile: {profile_dir}")
                            shutil.rmtree(profile_dir)
                            logger.info("Profile cleared successfully")
                    except Exception as clear_err:
                        logger.warning(f"Could not clear profile: {str(clear_err)}")
                    
                    # Restart browser with fresh profile
                    try:
                        logger.info("Restarting browser with fresh profile...")
                        driver = setup_driver(username, aggressive_cleanup=True)
                        logger.info("Browser restarted successfully")
                        # Don't increment nav_attempt - try again with fresh profile
                        continue
                    except Exception as restart_err:
                        logger.error(f"Failed to restart browser: {str(restart_err)}")
                        # Fall through to normal refresh attempt
                
                # Normal refresh attempt
                try:
                    driver.refresh()
                    # Wait a bit after refresh
                    time.sleep(random.uniform(2.0, 3.5))
                    
                    # Check if refresh helped
                    if wait_for_login_page_ready(driver, max_wait=20, refresh_on_stuck=False):
                        logger.info(
                            f"Login page ready after refresh (attempt {nav_attempt})"
                        )
                        nav_ready = True
                        break
                except WebDriverException as e:
                    if "no such window" in str(e).lower() or "no such session" in str(e).lower():
                        raise
                    logger.warning(f"Refresh failed: {str(e)}")
                
                # Brief pause before next attempt
                time.sleep(random.uniform(1.0, 2.5))
        
        logger.info(f"Login page stage duration: {time.time() - nav_start:.1f}s")
        if not nav_ready:
            logger.warning("Proceeding despite login page readiness timeouts")
    except WebDriverException as e:
        # Check if window was closed - if so, propagate this up immediately
        if "no such window" in str(e).lower() or "no such session" in str(e).lower():
            logger.info(
                "Browser window was closed during navigation. Might be for VPN switching."
            )
            raise
        logger.error(f"Failed to navigate to login page: {str(e)}")
        return False

    # Setup state machine variables
    start_time = time.time()
    last_action_time = start_time
    last_url = driver.current_url
    login_successful = False
    manual_intervention_active = False
    last_filled_at = {"username": 0.0, "password": 0.0, "email": 0.0}
    last_filled_url = {"username": "", "password": "", "email": ""}

    # State machine loop
    loop_count = 0
    last_progress_time = start_time
    last_progress_url = ""
    
    while time.time() - start_time < WAITING_TIME:
        loop_count += 1
        try:
            current_url = driver.current_url
            
            # Log every 10 iterations to show we're still alive
            if loop_count % 10 == 0:
                logger.info(f"State machine loop iteration {loop_count}, URL: {current_url}")
            
            # Detect if we're stuck on a loading spinner during the flow
            if loop_count % 20 == 0 and is_page_stuck_loading(driver):
                logger.warning("Detected stuck loading spinner during login flow")
                
                # Check if we've made progress recently
                time_since_progress = time.time() - last_progress_time
                url_changed = current_url != last_progress_url
                
                if time_since_progress > 45 and not url_changed:
                    logger.warning(
                        f"No progress for {time_since_progress:.0f}s, attempting recovery refresh"
                    )
                    try:
                        driver.refresh()
                        time.sleep(3)
                        last_progress_time = time.time()  # Reset timer after refresh
                    except WebDriverException as e:
                        if "no such window" in str(e).lower() or "no such session" in str(e).lower():
                            raise
                        logger.warning(f"Recovery refresh failed: {str(e)}")
                else:
                    logger.info(f"Progress check: {time_since_progress:.0f}s since last action, URL changed: {url_changed}")

            # Check if already logged in
            if is_logged_in(driver):
                logger.info("Login successful!")
                login_successful = True
                break

            # Check for rate limiting - if detected, we'll need to back off
            if loop_count % 15 == 0 and is_rate_limited(driver):
                record_rate_limit_hit(username)
                backoff = get_account_backoff(username)
                logger.warning(f"Rate limited. Backing off for {backoff}s...")
                
                # Capture screenshot for debugging
                capture_screenshot(driver, username, "rate_limited")
                
                # Wait out the backoff period
                time.sleep(backoff)
                
                # Try refreshing after backoff
                try:
                    driver.refresh()
                    time.sleep(3)
                    last_progress_time = time.time()
                except WebDriverException as e:
                    if "no such window" in str(e).lower() or "no such session" in str(e).lower():
                        raise
                    logger.warning(f"Post-backoff refresh failed: {str(e)}")
                continue

            # Check for account lockout/suspension (permanent failure for this account)
            if loop_count % 25 == 0 and is_account_locked_or_suspended(driver):
                logger.error(f"Account {username} appears to be locked or suspended. Aborting.")
                capture_screenshot(driver, username, "account_locked")
                # Don't retry this account - it's a permanent failure
                return False

            # Check if URL changed since last check
            if current_url != last_url:
                logger.info(f"URL changed to: {current_url}")
                last_url = current_url
                last_action_time = time.time()  # Reset the idle timer when URL changes
                last_progress_time = time.time()  # Track progress for stuck detection
                last_progress_url = current_url

            # Check if we need verification
            if needs_verification(driver):
                if not manual_intervention_active:
                    logger.info("Manual verification required")
                    manual_intervention_active = True

                # Try to help with the verification by filling known fields
                # Check for phone/email verification screen
                verification_inputs = driver.find_elements(
                    By.CSS_SELECTOR,
                    'input[placeholder*="Phone or email"], input[placeholder*="phone number or email"], input[aria-label*="phone"], input[aria-label*="email"], input[name="text"], input.r-30o5oe, input[placeholder*="Email address"]',
                )
                if verification_inputs and any(
                    inp.is_displayed() for inp in verification_inputs
                ):
                    logger.info(
                        "Phone/email verification screen detected - filling with email"
                    )
                    for input_field in verification_inputs:
                        if input_field.is_displayed():
                            try:
                                # Clear the field completely
                                input_field.clear()
                                input_field.send_keys(Keys.CONTROL + "a")
                                input_field.send_keys(Keys.DELETE)
                                time.sleep(0.5)
                            except:
                                pass
                            # Only type the email, nothing else
                            human_like_typing(input_field, email)
                            logger.info(
                                f"Filled verification input with email: {email}"
                            )
                            time.sleep(1)
                            before_click_url = driver.current_url
                            click_next_button(driver)
                            human_like_post_action_pause()
                            wait_for_post_click_transition(
                                driver, before_click_url, timeout=CLICK_WAIT
                            )
                            last_action_time = time.time()
                            last_progress_time = time.time()
                            continue

                # Check specifically for the "Help us keep your account safe" screen
                help_safe_elements = driver.find_elements(
                    By.XPATH, "//*[contains(text(), 'Help us keep your account safe')]"
                )
                if help_safe_elements and any(
                    elem.is_displayed() for elem in help_safe_elements
                ):
                    logger.info("Account safety verification screen detected")
                    # Try to find email input field
                    email_inputs = driver.find_elements(
                        By.CSS_SELECTOR, 'input[placeholder="Email address"]'
                    )
                    if email_inputs and any(inp.is_displayed() for inp in email_inputs):
                        for input_field in email_inputs:
                            if input_field.is_displayed():
                                try:
                                    # Clear the field completely
                                    input_field.clear()
                                    input_field.send_keys(Keys.CONTROL + "a")
                                    input_field.send_keys(Keys.DELETE)
                                    time.sleep(0.5)
                                except:
                                    pass
                                # Type the email address
                                human_like_typing(input_field, email)
                                logger.info(
                                    f"Filled account safety email with: {email}"
                                )
                                time.sleep(1)
                                # Look for the Next button
                                next_buttons = driver.find_elements(
                                    By.XPATH,
                                    '//div[@role="button" and contains(text(), "Next")]',
                                )
                                if next_buttons and any(
                                    btn.is_displayed() for btn in next_buttons
                                ):
                                    for btn in next_buttons:
                                        if btn.is_displayed():
                                            before_click_url = driver.current_url
                                            btn.click()
                                            logger.info(
                                                "Clicked Next button on account safety screen"
                                            )
                                            human_like_post_action_pause()
                                            wait_for_post_click_transition(
                                                driver,
                                                before_click_url,
                                                timeout=CLICK_WAIT,
                                            )
                                            last_action_time = time.time()
                                            last_progress_time = time.time()
                                            break
                                else:
                                    # If can't find specific Next button, try generic button click
                                    before_click_url = driver.current_url
                                    click_next_button(driver)
                                    human_like_post_action_pause()
                                    wait_for_post_click_transition(
                                        driver, before_click_url, timeout=CLICK_WAIT
                                    )
                                    last_action_time = time.time()
                                    last_progress_time = time.time()
                                continue

                # Check for email input (older style)
                can_refill_email = (
                    current_url != last_filled_url["email"]
                    or time.time() - last_filled_at["email"] > 12
                )
                if can_refill_email and find_and_fill_input(driver, "email", email):
                    last_filled_at["email"] = time.time()
                    last_filled_url["email"] = current_url
                    before_click_url = driver.current_url
                    click_next_button(driver)
                    human_like_post_action_pause()
                    wait_for_post_click_transition(
                        driver, before_click_url, timeout=CLICK_WAIT
                    )
                    last_action_time = time.time()
                    last_progress_time = time.time()
                    continue

            # Check for phone input (we'll let the user handle this)
                phone_inputs = driver.find_elements(
                    By.CSS_SELECTOR, 'input[type="tel"], input[placeholder*="phone" i]'
                )
                if phone_inputs and any(inp.is_displayed() for inp in phone_inputs):
                    logger.info(
                        "Phone verification required - waiting for manual completion"
                    )
                    # Just continue polling, user needs to complete this manually
                    time.sleep(POLLING_INTERVAL)
                    continue
            else:
                # If we no longer need verification, update the flag
                if manual_intervention_active:
                    logger.info("Manual verification appears to be completed")
                    manual_intervention_active = False

            # Normal login flow - try to identify and fill inputs
            # Username field
            can_refill_username = (
                current_url != last_filled_url["username"]
                or time.time() - last_filled_at["username"] > 12
            )
            if can_refill_username and find_and_fill_input(driver, "username", username):
                last_filled_at["username"] = time.time()
                last_filled_url["username"] = current_url
                before_click_url = driver.current_url
                click_next_button(driver)
                human_like_post_action_pause()
                wait_for_post_click_transition(
                    driver, before_click_url, timeout=CLICK_WAIT
                )
                last_action_time = time.time()
                last_progress_time = time.time()
                continue

            # Password field
            can_refill_password = (
                current_url != last_filled_url["password"]
                or time.time() - last_filled_at["password"] > 12
            )
            if can_refill_password and find_and_fill_input(driver, "password", password):
                last_filled_at["password"] = time.time()
                last_filled_url["password"] = current_url
                before_click_url = driver.current_url
                click_next_button(driver)
                human_like_post_action_pause()
                wait_for_post_click_transition(
                    driver, before_click_url, timeout=CLICK_WAIT
                )
                last_action_time = time.time()
                last_progress_time = time.time()
                continue

            # If we haven't taken any action for a while, try clicking a button
            if time.time() - last_action_time > 30:  # 30 seconds of no action
                if click_next_button(driver):
                    logger.info("Clicked a button after 30 seconds of inactivity")
                    human_like_post_action_pause()
                    last_action_time = time.time()
                    last_progress_time = time.time()
                    continue

            # If we're not logged in and can't find any inputs, wait
            time.sleep(POLLING_INTERVAL)

        except WebDriverException as e:
            # Immediately propagate window closing exceptions
            if (
                "no such window" in str(e).lower()
                or "no such session" in str(e).lower()
            ):
                logger.info("Browser window was closed. Might be for VPN switching.")
                raise

            # Handle other WebDriver exceptions
            logger.error(f"WebDriver error: {str(e)}")
            # Continue the loop to try again

        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
            # Continue the loop to try again

    # After the loop, check if login was successful
    if login_successful:
        try:
            # Keep a small natural pause, then rely on adaptive cookie checks.
            logger.info("Login detected, verifying cookies...")
            human_like_post_action_pause()
            
            # Ensure we are on a stable post-login page before cookie extraction.
            current = driver.current_url.lower()
            if (
                "x.com/i/flow/login" in current
                or ("x.com" not in current and "twitter.com" not in current)
            ):
                logger.info("Navigating to https://x.com/home to stabilize authenticated session")
                try:
                    driver.get("https://x.com/home")
                    nav_wait_start = time.time()
                    while time.time() - nav_wait_start < 4:
                        try:
                            if driver.execute_script("return document.readyState") == "complete":
                                break
                        except Exception:
                            pass
                        time.sleep(0.25)
                except WebDriverException as e:
                    if (
                        "no such window" in str(e).lower()
                        or "no such session" in str(e).lower()
                    ):
                        logger.info(
                            "Browser window was closed after login. Might be for VPN switching."
                        )
                        raise
                    logger.warning(f"Failed to navigate: {str(e)}")
            else:
                logger.info(f"On X/Twitter domain, extracting cookies from: {current}")

            # Poll for required post-login session cookies before final extraction.
            cookie_wait_start = time.time()
            cookie_values = wait_for_required_session_cookies(driver)
            logger.info(
                f"Cookie readiness stage duration: {time.time() - cookie_wait_start:.1f}s"
            )
            
            # Clear rate limit hits on successful login
            clear_rate_limit_hits(username)
            
            domain = "x.com"
            cookies_json = generate_cookies_json(cookie_values, domain)

            # Save cookies to file
            output_path = os.path.join(OUTPUT_DIR, output_file)
            with open(output_path, "w") as f:
                f.write(json.dumps(cookies_json, indent=2))
            logger.info(f"Saved cookies for {username} to {output_path}")

            return True
        except WebDriverException as e:
            # Check if window was closed
            if (
                "no such window" in str(e).lower()
                or "no such session" in str(e).lower()
            ):
                logger.info(
                    "Browser window was closed after login. Might be for VPN switching."
                )
                raise
            logger.error(f"Error after successful login: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Error after successful login: {str(e)}")
            return False
    else:
        logger.error(f"Failed to login for {username} within the time limit")
        
        # Capture screenshot for debugging the failure
        try:
            capture_screenshot(driver, username, "login_failed")
        except Exception as e:
            logger.debug(f"Could not capture failure screenshot: {str(e)}")
        
        return False


def main():
    """Main function to process Twitter accounts from environment variable."""
    logger.info("Starting cookie grabber")

    # Check for required environment variables
    if not os.environ.get("TWITTER_EMAIL"):
        logger.error("TWITTER_EMAIL environment variable is not set.")
        logger.error("This is required for email verification during login.")
        return

    # Get Twitter accounts from environment variable
    twitter_accounts_str = os.environ.get("TWITTER_ACCOUNTS", "")

    if not twitter_accounts_str:
        logger.error("TWITTER_ACCOUNTS environment variable is not set.")
        logger.error("Format should be: username1:password1,username2:password2")
        return

    account_pairs = twitter_accounts_str.split(",")
    logger.info(f"Found {len(account_pairs)} accounts to process")
    logger.info(
        "Browser reset between accounts is disabled to reduce verification challenges"
    )

    # Create the output directory if it doesn't exist
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Process accounts one by one
    current_account_index = 0
    while current_account_index < len(account_pairs):
        # Maximum number of retries for account processing
        max_retries = 5  # Increased retries to allow for VPN switches
        retry_count = 0
        consecutive_window_closes = 0
        driver = None

        account_pair = account_pairs[current_account_index]
        if ":" not in account_pair:
            logger.error(
                f"Invalid account format: {account_pair}. Expected format: username:password"
            )
            current_account_index += 1
            continue

        username, password = account_pair.split(":", 1)
        username = username.strip()
        password = password.strip()

        logger.info(
            f"Processing account {current_account_index+1}/{len(account_pairs)}: {username}"
        )

        # Check if we should use fresh profiles (clears any poisoned profile data)
        use_fresh_profiles = os.environ.get("COOKIE_GRABBER_FRESH_PROFILES", "false").lower() == "true"
        if use_fresh_profiles:
            profile_dir = os.path.join(OUTPUT_DIR, "profiles", username)
            if os.path.exists(profile_dir):
                logger.info(f"FRESH_PROFILES enabled: Clearing existing profile for {username}")
                try:
                    shutil.rmtree(profile_dir)
                    logger.info("Profile cleared successfully")
                except Exception as e:
                    logger.warning(f"Could not clear profile: {str(e)}")

        # Process account with potential window closing for VPN switching
        success = False
        while retry_count < max_retries and not success:
            try:
                # Apply backoff delay if this is a retry (not the first attempt)
                if retry_count > 0:
                    backoff = get_account_backoff(username)
                    # Add some jitter to avoid thundering herd
                    jitter = random.uniform(0, 5)
                    total_delay = backoff + jitter
                    if total_delay > 0:
                        logger.info(
                            f"Waiting {total_delay:.1f}s before retry {retry_count} "
                            f"({backoff}s backoff + {jitter:.1f}s jitter)"
                        )
                        time.sleep(total_delay)
                
                # Initialize a new driver for each retry
                if driver is not None:
                    try:
                        driver.quit()
                    except:
                        pass

                driver_setup_start = time.time()
                driver = setup_driver(username, aggressive_cleanup=(retry_count > 0))
                logger.info(
                    f"Driver setup duration: {time.time() - driver_setup_start:.1f}s"
                )
                logger.info(
                    f"Browser initialized for account: {username} (attempt {retry_count+1}/{max_retries})"
                )

                # Process the current account
                success = process_account_state_machine(driver, username, password)

                if success:
                    logger.info(f"Successfully processed account: {username}")
                else:
                    retry_count += 1
                    logger.info(
                        f"Account processing unsuccessful. Retries left: {max_retries - retry_count}"
                    )
                    time.sleep(10)  # Brief pause before retry

            except WebDriverException as e:
                # Special handling for closed window (VPN switching)
                if (
                    "no such window" in str(e).lower()
                    or "no such session" in str(e).lower()
                ):
                    consecutive_window_closes += 1
                    
                    if consecutive_window_closes > 3:
                        logger.error(f"Window closed unexpectedly {consecutive_window_closes} times in a row. Treating as failure.")
                        
                        # Handle potential profile corruption by moving the profile directory
                        try:
                            profile_dir = os.path.join(OUTPUT_DIR, "profiles", username)
                            if os.path.exists(profile_dir):
                                timestamp = int(time.time())
                                backup_path = f"{profile_dir}_corrupted_{timestamp}"
                                logger.warning(f"Profile likely corrupted. Moving {profile_dir} to {backup_path}")
                                # Close any lingering file handles before moving (best effort)
                                if driver:
                                    try:
                                        driver.quit()
                                    except:
                                        pass
                                    driver = None
                                os.rename(profile_dir, backup_path)
                                logger.info("Profile directory reset. Next attempt will start fresh.")
                        except Exception as e:
                            logger.error(f"Failed to move corrupted profile: {str(e)}")

                        retry_count += 1
                        # We don't continue here, so it will fall through to cleanup and loop check
                    else:
                        logger.info(
                            f"Browser window was closed (occurrence {consecutive_window_closes}). This might be for VPN switching."
                        )
                        logger.info(
                            "Waiting 30 seconds for VPN to stabilize before retrying..."
                        )

                        # Clean up the driver
                        try:
                            if driver:
                                driver.quit()
                        except:
                            pass

                        # Wait for VPN switch to complete
                        time.sleep(30)

                        # Don't increment retry count for intentional window closing
                        # This allows unlimited VPN switches
                        logger.info(f"Resuming after window close for account: {username}")
                        continue
                else:
                    consecutive_window_closes = 0  # Reset on different error
                    # Handle other WebDriver exceptions
                    retry_count += 1
                    logger.error(
                        f"WebDriver error (attempt {retry_count}/{max_retries}): {str(e)}"
                    )
                    time.sleep(15)

            except Exception as e:
                consecutive_window_closes = 0  # Reset on different error
                retry_count += 1
                logger.error(
                    f"Unexpected error (attempt {retry_count}/{max_retries}): {str(e)}"
                )
                time.sleep(15)

                try:
                    if driver:
                        driver.quit()
                except:
                    pass

        # Clean up the driver
        try:
            if driver:
                driver.quit()
        except:
            pass

        # Move to next account only if successful or max retries reached
        if success or retry_count >= max_retries:
            if success:
                logger.info(f"Successfully completed account: {username}")
            else:
                logger.warning(
                    f"Failed to process account after {max_retries} attempts: {username}"
                )

            current_account_index += 1

            # Cooldown between accounts
            if current_account_index < len(account_pairs):
                cool_down = random.uniform(5, 10)  # 5-10 seconds cooldown
                logger.info(
                    f"Cooling down for {cool_down:.1f} seconds before next account"
                )
                time.sleep(cool_down)

    logger.info("All accounts processed")


if __name__ == "__main__":
    load_dotenv()  # Load environment variables
    logger.info("Starting cookie grabber script")
    main()
