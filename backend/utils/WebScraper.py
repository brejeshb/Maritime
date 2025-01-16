import asyncio
from pyppeteer import launch
from selenium import webdriver
from selenium.webdriver.chrome.options import Options as ChromeOptions
from bs4 import BeautifulSoup
from openai import OpenAI
import json
from typing import Dict, Any
import time
import logging
import os
from dotenv import load_dotenv
load_dotenv()
logging.basicConfig(level=logging.INFO)


class WebScraper:
    def __init__(self):
        self.browser = None
        self.api_key = os.getenv('OPENAI_API_KEY')
        self.client = OpenAI(api_key=self.api_key)  # Initialize OpenAI client
        
    async def initialize_pyppeteer(self):
        """Initialize Pyppeteer browser instance"""
        try:
            self.browser = await launch(
                headless=True,
                args=['--no-sandbox', '--disable-setuid-sandbox'],
                ignoreHTTPSErrors=True
            )
            logging.info("Initialized Pyppeteer browser")
        except Exception as e:
            logging.error(f"Failed to initialize Pyppeteer: {e}")
            raise e

    def initialize_selenium(self):
        """Initialize Selenium browser instance"""
        try:
            chrome_options = ChromeOptions()
            chrome_options.add_argument('--headless')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--window-size=1920,1080')
            chrome_options.add_argument('--disable-web-security')
            chrome_options.add_argument('--disable-features=IsolateOrigins,site-per-process')

            self.browser = webdriver.Chrome(options=chrome_options)
            logging.info("Initialized Selenium browser")
        except Exception as e:
            logging.error(f"Failed to initialize Selenium: {e}")
            raise e

    async def cleanup(self):
        """Clean up resources"""
        if self.browser:
            if isinstance(self.browser, webdriver.Chrome):
                self.browser.quit()
            else:
                await self.browser.close()
        logging.info("Cleanup complete")

    async def extract_main_content(self, page) -> str:
        """Extract the entire body content, remove CSS and tags, and clean the text."""
        try:
            content = await page.content()  # Get the full HTML content of the page
            soup = BeautifulSoup(content, 'html.parser')  # Parse the HTML with BeautifulSoup
            
            # Remove unwanted tags like style, script, and other non-content elements
            for element in soup.find_all(['style', 'script', 'header', 'footer', 'nav', 'aside']):
                element.decompose()  # Decompose removes the tag and its content
            
            # Now we extract all the remaining text from the page, cleaning up unnecessary whitespace
            text = soup.get_text(separator=' ', strip=True)
            
            # Remove extra whitespace and newlines
            text = ' '.join(text.split())  # This will remove excess spaces and newlines
            
            # If the resulting text is too short or empty, return an error message
            if len(text) < 200:  # You can adjust the threshold based on your needs
                return ""
            
            return text  # Return the cleaned text from the page
        
        except Exception as e:
            logging.error(f"Content extraction error: {e}")
            return ""
    async def process_with_llm(self, text: str, url: str) -> Dict[str, Any]:
        """Process maritime article text with GPT-3.5 to extract and format content."""
        prompt = f"""
        Analyze the following text from a maritime article. Extract and format the following details:
        1. Title of the article
        2. Main topics or key themes discussed in the article (e.g., maritime safety, shipping regulations, naval history, etc.)
        3. Key events or occurrences mentioned (e.g., maritime accidents, significant dates, notable vessels)
        4. Author(s) or source of the article
        5. Relevant location(s) or geographic areas mentioned (e.g., specific ports, regions, or seas)
        6. Publication date or time frame (if mentioned)
        7. Any maritime-specific terms or jargon used in the article

        Text: {text}

        Format the response as a JSON object with these exact fields:
        {{
            "text": "{text}",  # Return the full article text without any modification
            "metadata": {{
                "source_url": "{url}",
                "title": "<title of the article>",
                "main_topics": "<key themes or topics from the article>",
                "key_events": "<major events or occurrences mentioned>",
                "author": "<author(s) or source of the article>",
                "locations": "<relevant geographic areas or locations mentioned>",
                "publication_date": "<publication date or time frame>",
                "maritime_terms": "<list of maritime-specific terms or jargon used>",
                "content_type": "maritime_article",
                "last_verified": "<current date>"
            }}
        }}

        Make sure to preserve all relevant details about the article while removing any irrelevant content like advertisements, sidebars, or unrelated sections.
        """

        try:
            # Call the GPT-3.5 model to process the article text
            response = self.client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=1500,
                temperature=0,
                top_p=1.0
            )

            # Extract the LLM response and process it
            result = response.choices[0].message.content.strip()

            try:
                # Parse the result into a structured JSON response
                result_json = json.loads(result)
                if "text" in result_json and "metadata" in result_json:
                    return result_json
            except json.JSONDecodeError:
                logging.warning("Failed to parse LLM response as JSON")

            # Fallback if LLM response parsing fails
            return {
                "text": text,
                "metadata": {
                    "source_url": url,
                    "title": "Unknown",
                    "main_topics": "",
                    "key_events": "",
                    "author": "Unknown",
                    "locations": "",
                    "publication_date": "",
                    "maritime_terms": "",
                    "content_type": "maritime_article",
                    "last_verified": time.strftime("%Y-%m-%d")
                }
            }

        except Exception as e:
            logging.error(f"LLM processing error: {e}")
            return {"error": str(e)}


    async def scrape_url(self, url: str) -> Dict[str, Any]:
        """Main scraping function"""
        try:
            retry_count = 2
            attempt = 0
            while attempt < retry_count:
                try:
                    if not self.browser:
                        if attempt == 0:
                            await self.initialize_pyppeteer()
                        else:
                            self.initialize_selenium()
                    
                    if isinstance(self.browser, webdriver.Chrome):  # Using Selenium
                        self.browser.get(url)
                        content = self.browser.page_source
                        soup = BeautifulSoup(content, 'html.parser')
                        # Extract text here as per your needs (not implemented for simplicity)
                        text = soup.get_text(separator=' ', strip=True)
                        if len(text) < 200:
                            return {"error": "No content extracted"}
                    else:  # Using Pyppeteer
                        page = await self.browser.newPage()
                        await page.goto(url, {'waitUntil': 'networkidle0', 'timeout': 30000})
                        content = await self.extract_main_content(page)
                        if not content:
                            return {"error": "No content extracted"}
                        await page.close()

                    # Process the content using LLM
                    result = await self.process_with_llm(content, url)
                    return result

                except Exception as e:
                    logging.error(f"Attempt {attempt + 1} failed with error: {e}")
                    attempt += 1
                    if isinstance(self.browser, webdriver.Chrome):
                        self.browser.quit()  # Clean up Selenium browser

                    if attempt == retry_count:
                        return {"error": str(e)}

        except Exception as e:
            logging.error(f"Scraping error: {e}")
            return {"error": str(e)}

async def main():
    scraper = WebScraper()
    try:
        url = "https://www.marineinsight.com/maritime-law/what-is-imsbc-code/"
        result = await scraper.scrape_url(url)
        print(json.dumps(result, indent=2))
    finally:
        await scraper.cleanup()

if __name__ == "__main__":
    asyncio.run(main())
