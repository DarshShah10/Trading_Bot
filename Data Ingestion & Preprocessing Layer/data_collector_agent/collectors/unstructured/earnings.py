# collectors/unstructured/earnings.py
import logging
import requests
from bs4 import BeautifulSoup
import datetime
from typing import List, Dict, Optional
from urllib.parse import urljoin

from collectors.base import BaseDataCollector
from core.utils import get_random_user_agent

logger = logging.getLogger(__name__)
class EarningsCallsCollector(BaseDataCollector):
    """
    Collects earnings call transcript information from a website
    (e.g., Seeking Alpha, Fool.com).  This is a *scraper*, so it's inherently
    fragile and depends on the website's structure.  A dedicated financial
    data API would be much more reliable.
    """
    def __init__(self, config: Dict, data_queue: queue.Queue, get_user_agent_fn):
        super().__init__(config, data_queue)
        self.get_user_agent = get_user_agent_fn

    def collect_data(self) -> List[Dict]:
        """Collects earnings call transcript information."""
        collected_data = []

        for company_data in self.config.get("companies", []):  # List of dicts
            company_name = company_data["name"]
            url = company_data["url"]
            transcript_selector = company_data["transcript_selector"]
            date_selector = company_data.get("date_selector")

            try:
                headers = {"User-Agent": self.get_user_agent()}
                response = requests.get(url, headers=headers, timeout=10)
                response.raise_for_status()

                soup = BeautifulSoup(response.text, "html.parser")
                transcripts = soup.select(transcript_selector)

                for transcript in transcripts:
                    title = transcript.get_text(strip=True)
                    transcript_url = transcript.get("href", "")
                    if transcript_url and not transcript_url.startswith(("http://", "https://")):
                        transcript_url = urljoin(url, transcript_url)  # Handle relative URLs

                    call_date = ""
                    if date_selector and transcript_url:
                      try:
                        transcript_response = requests.get(transcript_url, headers=headers, timeout = 10)
                        transcript_response.raise_for_status()
                        transcript_soup = BeautifulSoup(transcript_response.text, 'html.parser')
                        date_element = transcript_soup.select_one(date_selector)
                        if date_element:
                          call_date = date_element.get_text(strip=True)
                      except requests.exceptions.RequestException as e:
                        logger.warning(f"Failed to get transcript date from {transcript_url}: {e}")


                    transcript_data = {
                        "source": "earnings_calls",  # Generic source
                        "data_type": "earnings_call_transcript",
                        "company": company_name,
                        "title": title,
                        "url": transcript_url,
                        "call_date": call_date,  # Add the call date
                        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                    }
                    collected_data.append(transcript_data)

                logger.info(f"Collected earnings call transcript info for {company_name}")

            except requests.exceptions.RequestException as e:
                logger.error(f"Error collecting earnings call info for {company_name}: {e}", exc_info=True)
            except Exception as e:
              logger.error(f"An unexpected error occurred while collecting earnings call info for {company_name}: {e}", exc_info = True)

        return collected_data