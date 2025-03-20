# collectors/macro/fed_rates.py
import logging
import requests
import datetime
from typing import Dict
from collectors.base import BaseDataCollector
from xml.etree import ElementTree as ET

# Requires: requests

logger = logging.getLogger(__name__)

class FedRatesCollector(BaseDataCollector):
    """
    Collects Federal Reserve interest rate data from the Federal Reserve website
    using their RSS feed.  This is more reliable than scraping.
    """

    def collect_data(self) -> Dict:
        """Collects the latest federal funds rate data."""
        try:
            # Federal Reserve RSS feed for H.15 data (Selected Interest Rates)
            rss_url = "https://www.federalreserve.gov/feeds/h15.xml"
            response = requests.get(rss_url, timeout=10)
            response.raise_for_status()

            # Parse the XML data.
            root = ET.fromstring(response.content)

            # Namespaces are used in the XML, so we need to define them.
            namespaces = {
                'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
                'dc': 'http://purl.org/dc/elements/1.1/',
                'cb': 'http://www.cbwiki.net/wiki/index.php/Specification_1.2',
                'fed': 'http://www.federalreserve.gov/structure/data'
            }
            # Find the most recent entry (item).  The data is ordered, so the
            # first item is the latest.
            latest_entry = root.find('.//item')  # Find the first 'item' element
            if latest_entry is None:
                logger.warning("No entries found in Federal Reserve RSS feed.")
                return {}

            # Extract the relevant data.
            date_str = latest_entry.find('dc:date', namespaces).text
            date = datetime.datetime.fromisoformat(date_str).date()  # Date only


            # Use find with namespaces to get the target rate.
            # The path is relative to the 'item', and we look for the
            # federal funds target range (upper bound).
            target_rate_upper_elem = latest_entry.find(
                './fed:rates/fed:fedfunds/fed:target_range_upper_limit', namespaces
            )

            # Handle case if fed funds is not there
            if target_rate_upper_elem is not None:
               target_rate_upper = float(target_rate_upper_elem.text)
            else:
               target_rate_upper = None #Or set a default
               logger.warning("Could not find fed funds target rate upper in Federal Reserve data.")



            processed_data = {
                "source": "federal_reserve",
                "data_type": "interest_rates",
                "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "date": date.isoformat(),  # Store the effective date
                "target_rate_upper": target_rate_upper, # Include target too
                "raw_data": ET.tostring(latest_entry, encoding='unicode')  # Store raw XML
            }
            logger.info(f"Collected Federal Reserve interest rates data for {date}")
            return processed_data

        except requests.exceptions.RequestException as e:
            logger.error(f"Error collecting Fed rates data: {e}", exc_info=True)
            return {}
        except ET.ParseError as e:
            logger.error(f"Error parsing XML from Federal Reserve: {e}", exc_info=True)
            return {}
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}", exc_info=True)
            return {}