# collectors/unstructured/news.py
import logging
import requests
from bs4 import BeautifulSoup
import datetime
from typing import List, Dict
from urllib.parse import urlparse, urljoin  # Import urljoin
from core.utils import get_random_user_agent, solve_captcha
from collectors.base import BaseDataCollector

# Requires: requests, beautifulsoup4

logger = logging.getLogger(__name__)

class NewsCollector(BaseDataCollector):
    """Collects news headlines and article summaries from various sources."""

    def __init__(self, config: Dict, data_queue: queue.Queue, get_user_agent_fn, solve_captcha_fn):
      super().__init__(config, data_queue)
      self.get_user_agent = get_user_agent_fn
      self.solve_captcha = solve_captcha_fn


    def collect_data(self) -> List[Dict]:
        """Collects news data from configured sources."""
        collected_data = []

        for source_data in self.config.get("sources", []):  # Expect a list of dicts
            source_url = source_data["url"]
            headlines_selector = source_data["headlines_selector"]
            article_selector = source_data.get("article_selector")  # Optional
            source_name = source_data.get("name", urlparse(source_url).netloc) # Default

            try:
                headers = {"User-Agent": self.get_user_agent()}
                response = requests.get(source_url, headers=headers, timeout=10)
                response.raise_for_status()  # Raise HTTPError for bad responses

                soup = BeautifulSoup(response.text, "html.parser")

                # --- CAPTCHA Handling ---
                if "captcha" in response.text.lower():
                    logger.info(f"CAPTCHA detected on {source_url}, attempting to solve.")
                    site_key = self._extract_site_key(soup)
                    if site_key:
                        captcha_solution = self.solve_captcha(site_key, source_url)
                        if captcha_solution:
                            headers["X-Captcha-Solution"] = captcha_solution
                            response = requests.get(source_url, headers=headers, timeout=10)
                            response.raise_for_status()
                            soup = BeautifulSoup(response.text, "html.parser")
                        else:
                            logger.warning(f"CAPTCHA solution failed for {source_url}.")
                            continue  # Skip this source if CAPTCHA fails


                headlines = soup.select(headlines_selector)

                for headline in headlines:
                    headline_text = headline.get_text(strip=True)
                    link = headline.get("href", "")
                    # Handle relative URLs
                    if link and not link.startswith(("http://", "https://")):
                        link = urljoin(source_url, link)


                    article_summary = ""
                    if article_selector and link: #Only if article selector and link is given
                        try:
                            article_response = requests.get(link, headers=headers, timeout=10)
                            article_response.raise_for_status()
                            article_soup = BeautifulSoup(article_response.text, "html.parser")
                            article_content = article_soup.select(article_selector)
                            article_summary = " ".join([p.get_text(strip=True) for p in article_content])[:500] #Limit summary
                        except requests.exceptions.RequestException as e:
                            logger.warning(f"Failed to fetch article from {link}: {e}")



                    news_data = {
                        "source": source_name,
                        "data_type": "news_headline",
                        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                        "headline": headline_text,
                        "link": link,
                        "summary": article_summary,  # Add the summary
                    }
                    collected_data.append(news_data)

                logger.info(f"Collected {len(headlines)} headlines from {source_name}")

            except requests.exceptions.RequestException as e:
                logger.error(f"Error collecting news from {source_url}: {e}", exc_info=True)
            except Exception as e:
                logger.error(f"An unexpected error occurred while collecting news from {source_url}: {e}", exc_info=True)

        return collected_data

    def _extract_site_key(self, soup) -> str:
        """Extract reCAPTCHA site key from HTML."""
        try:
            captcha_div = soup.find("div", {"class": "g-recaptcha"})
            if captcha_div:
                return captcha_div.get("data-sitekey", "")
            return ""
        except Exception as e:
            logger.error(f"Error extracting site key: {e}")
            return ""