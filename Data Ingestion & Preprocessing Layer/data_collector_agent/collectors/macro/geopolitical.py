# collectors/macro/geopolitical.py
import logging
import requests
import datetime
from typing import Dict, List
from bs4 import BeautifulSoup
from collectors.base import BaseDataCollector
from core.utils import get_random_user_agent

logger = logging.getLogger(__name__)

class GeopoliticalEventsCollector(BaseDataCollector):
    """
    Collects geopolitical event information.  This is a *scraper*, so it's
    inherently fragile. There isn't a readily available, free, and reliable API
    for this type of data. This example scrapes a hypothetical calendar.
    You'll likely need to adapt the selectors to a real website.
    """

    def __init__(self, config: Dict, data_queue: queue.Queue, get_user_agent_fn):
        super().__init__(config, data_queue)
        self.get_user_agent = get_user_agent_fn


    def collect_data(self) -> List[Dict]:
        """Collects geopolitical event data from the configured sources."""
        collected_data = []
        for source in self.config.get('sources', []):
            try:
                url = source['url']
                event_selector = source['event_selector']
                title_selector = source['title_selector']
                date_selector = source['date_selector']
                description_selector = source.get('description_selector') #Optional
                source_name = source.get('name', 'Geopolitical Events') #Optional

                headers = {"User-Agent": self.get_user_agent()}
                response = requests.get(url, headers=headers, timeout=10)
                response.raise_for_status()

                soup = BeautifulSoup(response.text, "html.parser")
                events = soup.select(event_selector)

                for event in events:
                    title_element = event.select_one(title_selector)
                    date_element = event.select_one(date_selector)
                    description_element = event.select_one(description_selector) if description_selector else None

                    if title_element and date_element:
                        title = title_element.get_text(strip=True)
                        date_str = date_element.get_text(strip=True)
                        description = description_element.get_text(strip=True) if description_element else ""

                        # Date parsing is tricky and depends on the website's format.
                        # This is a placeholder; you'll need to adapt it.
                        try:
                            # Example: Assuming date is in "YYYY-MM-DD" format
                            event_date = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
                        except ValueError:
                            try:
                                #Another example if day first
                                event_date = datetime.datetime.strptime(date_str, "%d-%m-%Y").date()
                            except ValueError:
                                logger.warning(f"Could not parse date: {date_str}")
                                event_date = None

                        if event_date:
                            event_data = {
                                "source": source_name,
                                "data_type": "geopolitical_event",
                                "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                                "title": title,
                                "date": event_date.isoformat(),  # Store as ISO 8601
                                "description": description,
                            }
                            collected_data.append(event_data)

                logger.info(f"Collected geopolitical events from {source_name}")

            except requests.exceptions.RequestException as e:
                logger.error(f"Error collecting geopolitical events from {url}: {e}", exc_info=True)
            except Exception as e:
                 logger.error(f"An unexpected error occurred while collecting geopolitical events data from {url}: {e}", exc_info=True)

        return collected_data