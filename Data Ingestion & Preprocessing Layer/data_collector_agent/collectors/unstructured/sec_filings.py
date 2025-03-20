# collectors/unstructured/sec_filings.py
import logging
import requests
from bs4 import BeautifulSoup
import datetime
from typing import List, Dict, Optional
from urllib.parse import urljoin

from collectors.base import BaseDataCollector
from core.utils import get_random_user_agent, solve_captcha

logger = logging.getLogger(__name__)

class SECFilingsCollector(BaseDataCollector):
    """
    Collects information about recent SEC filings (10-K, 10-Q) for specified
    companies using the SEC EDGAR system.  Does *not* download the full filings
    (which can be very large).  Instead, it extracts metadata and links.
    """
    def __init__(self, config: Dict, data_queue: queue.Queue, get_user_agent_fn, solve_captcha_fn):
      super().__init__(config, data_queue)
      self.get_user_agent = get_user_agent_fn
      self.solve_captcha = solve_captcha_fn

    def collect_data(self) -> List[Dict]:
        """Collects SEC filing information for the configured companies."""
        collected_data = []

        for company_cik in self.config.get("company_ciks", []):  # Use CIKs
            try:
                # Construct the EDGAR search URL.
                base_url = "https://www.sec.gov/cgi-bin/browse-edgar"
                params = {
                    "action": "getcompany",
                    "CIK": company_cik,
                    "type": "10-K,10-Q",  # Search for 10-K and 10-Q filings
                    "dateb": "",  # You can specify a date range here if needed
                    "owner": "exclude",
                    "count": "40",  # Number of filings to retrieve
                }

                headers = {"User-Agent": self.get_user_agent(),
                           "Host": "www.sec.gov"} #Need host too
                response = requests.get(base_url, params=params, headers=headers, timeout=15)
                response.raise_for_status()

                soup = BeautifulSoup(response.text, "html.parser")

                 # --- CAPTCHA Handling ---
                if "captcha" in response.text.lower():
                    logger.info(f"CAPTCHA detected on SEC EDGAR, attempting to solve.")
                    site_key = self._extract_site_key(soup)
                    if site_key:
                        captcha_solution = self.solve_captcha(site_key, base_url)
                        if captcha_solution:
                            headers["X-Captcha-Solution"] = captcha_solution
                            response = requests.get(base_url, params=params, headers=headers, timeout=15)
                            response.raise_for_status()
                            soup = BeautifulSoup(response.text, "html.parser")
                        else:
                            logger.warning(f"CAPTCHA solution failed for SEC EDGAR.")
                            continue


                # Find the table containing the filing information.
                filings_table = soup.find("table", class_="tableFile2")
                if not filings_table:
                    logger.warning(f"No filings table found for CIK {company_cik}")
                    continue

                # Extract data from each row of the table.
                for row in filings_table.find_all("tr"):
                    cells = row.find_all("td")
                    if len(cells) >= 5:  # Ensure we have enough columns
                        filing_type = cells[0].get_text(strip=True)
                        filing_link = cells[1].find("a", id="documentsbutton")
                        if filing_link:
                            filing_url = urljoin("https://www.sec.gov", filing_link["href"])
                        else:
                            filing_url = ""

                        filing_date = cells[3].get_text(strip=True)
                        filing_number = cells[4].get_text(strip=True)

                        filing_data = {
                            "source": "sec_edgar",
                            "data_type": "sec_filing",
                            "company_cik": company_cik,
                            "filing_type": filing_type,
                            "filing_date": filing_date,
                            "filing_number": filing_number,
                            "filing_url": filing_url,
                            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                        }
                        collected_data.append(filing_data)

                logger.info(f"Collected SEC filing information for CIK {company_cik}")

            except requests.exceptions.RequestException as e:
                logger.error(f"Error collecting SEC filings for CIK {company_cik}: {e}", exc_info=True)
            except Exception as e:
                logger.error(f"An unexpected error occurred while collecting SEC filings for CIK {company_cik}: {e}", exc_info=True)
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