import asyncio
from selenium import webdriver
from selenium.webdriver.chrome.options import Options as ChromeOptions
from pyppeteer import launch
from pyppeteer.errors import TimeoutError
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)

# Error handling function (unified for Puppeteer and WebDriver)
def handle_browser_errors(error):
    # Specific CSS errors to ignore
    css_errors = [
        "Could not parse CSS stylesheet",
        "Failed to parse CSS",
        "CSS syntax error",
        "Unexpected token in CSS",
        "Invalid CSS property"
    ]
    
    if any(msg in str(error) for msg in css_errors):
        return  # Silently ignore CSS errors
    
    # Existing timeout handling
    if "timeout" in str(error) or "Timed out" in str(error):
        logging.error("Timeout occurred: %s", error)
        return
    
    logging.error("Error: %s", error)


# Selenium Browser Setup
def set_up_browser():
    """Setup Selenium WebDriver with error handling and exponential backoff"""
    try:
        chrome_options = ChromeOptions()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument('--disable-web-security')
        chrome_options.add_argument('--disable-features=IsolateOrigins,site-per-process')
        
        driver = webdriver.Chrome(options=chrome_options)
        
        # Set timeouts with exponential backoff
        max_retries = 3
        current_timeout = 15  # Start with 15 seconds
        for i in range(max_retries):
            try:
                driver.set_page_load_timeout(current_timeout)
                driver.set_script_timeout(current_timeout * 0.8)
                break
            except Exception as error:
                if i == max_retries - 1:
                    handle_browser_errors(error)
                    raise error
                current_timeout *= 2
                logging.info(f"Retry {i + 1} with timeout {current_timeout}s")
        
        return driver
    except Exception as error:
        handle_browser_errors(error)
        raise error


# Pyppeteer Browser Setup (Standard Configuration)
async def set_up_puppeteer():
    """Setup Pyppeteer with error handling and retry logic"""
    try:
        browser = await launch(
            headless=True,
            args=[
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-dev-shm-usage',
                '--disable-accelerated-2d-canvas',
                '--disable-gpu',
                '--window-size=1920x1080',
                '--disable-web-security',
                '--disable-features=IsolateOrigins,site-per-process',
            ],
            defaultViewport=None
        )
        
        page = await browser.newPage()

        # Intercept and handle CSS errors
        await page.setRequestInterception(True)
        page.on('request', lambda request: request.continue_() if request.resourceType != 'stylesheet' else request.abort())
        
        # Increased timeouts with exponential backoff
        max_retries = 3
        current_timeout = 15000  # Start with 15s
        for i in range(max_retries):
            try:
                await page.setDefaultNavigationTimeout(current_timeout)
                await page.setDefaultTimeout(current_timeout)
                break
            except TimeoutError as error:
                if i == max_retries - 1:
                    handle_browser_errors(error)
                    raise error
                current_timeout *= 2  # Double timeout for next retry
                logging.info(f"Retry {i + 1} with timeout {current_timeout}ms")

        # Additional error handling
        page.on('error', handle_browser_errors)
        page.on('pageerror', handle_browser_errors)

        return browser, page

    except Exception as error:
        handle_browser_errors(error)
        raise error


# Pyppeteer Browser Setup (Simplified Configuration)
async def set_up_puppeteer2():
    """Setup Pyppeteer with a simpler configuration"""
    try:
        browser = await launch(
            headless=True, 
            args=['--no-sandbox', '--disable-setuid-sandbox', '--disable-infobars', '--window-size=1200x800']
        )
        page = await browser.newPage()
        await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')
        
        return browser, page

    except Exception as error:
        logging.error('Error during Puppeteer setup:', error)
        raise error

