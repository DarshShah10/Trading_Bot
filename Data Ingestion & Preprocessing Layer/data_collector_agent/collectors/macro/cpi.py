# collectors/macro/cpi.py
import logging
import requests
import datetime
from typing import Dict, Optional
from collectors.base import BaseDataCollector
import os
from dotenv import load_dotenv

# Requires: requests, python-dotenv

logger = logging.getLogger(__name__)

class CPICollector(BaseDataCollector):
    """
    Collects Consumer Price Index (CPI) data from the Bureau of Labor Statistics (BLS) API.
    Requires a BLS API key (v2).  You can register for a key at:
    https://www.bls.gov/developers/api_signature_v2.htm
    """
    def __init__(self, config: Dict, data_queue: queue.Queue):
        super().__init__(config, data_queue)
        self.api_key = self._get_api_key()

    def _get_api_key(self) -> Optional[str]:
        """Loads the BLS API key from an environment variable."""
        load_dotenv()  # Load environment variables from .env file
        try:
            return os.environ["BLS_API_KEY"]
        except KeyError:
            logger.error("BLS_API_KEY environment variable not set. CPI collector will not work.")
            return None


    def collect_data(self) -> Dict:
        """Collects the latest CPI data from the BLS API."""

        if not self.api_key:
            return {}

        # BLS API endpoint and series ID for CPI (All items in U.S. city average,
        # seasonally adjusted).
        api_url = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
        series_id = "CUUR0000SA0"  # All items in U.S. city average, seasonally adjusted

        headers = {'Content-type': 'application/json'}
        current_year = datetime.datetime.now().year
        # Get data for the last 2 years to have some history
        data = json.dumps({
            "seriesid": [series_id],
            "startyear": str(current_year - 1),  # Last year
            "endyear": str(current_year),
            "registrationkey": self.api_key
        })

        try:
            response = requests.post(api_url, data=data, headers=headers, timeout=10)
            response.raise_for_status()
            json_data = response.json()

            if json_data['status'] != 'REQUEST_SUCCEEDED':
                logger.error(f"BLS API request failed: {json_data.get('message')}")
                return {}

            # Extract the latest CPI data point.
            if not json_data['Results']['series'][0]['data']: #If no data
                logger.warning('No CPI data returned from BLS')
                return {}

            latest_data = json_data['Results']['series'][0]['data'][0] #Data is ordered
            year = int(latest_data['year'])
            period = latest_data['period']  # e.g., "M11" for November
            period_name = latest_data['periodName'] #Full month, like March
            value = float(latest_data['value'])
            # Create a datetime object for the CPI data.
            # We'll assume the date is the first day of the month.
            cpi_date = datetime.datetime(year, int(period[1:]), 1, tzinfo=datetime.timezone.utc)


            processed_data = {
                "source": "bls",
                "data_type": "cpi",
                "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "date": cpi_date.isoformat(),  # Date of the CPI value
                "value": value,
                "period": period,
                "period_name": period_name, # User-friendly
                "year": year,
                "raw_data": latest_data  # Store raw API response
            }
            logger.info(f"Collected CPI data for {period_name} {year}: {value}")
            return processed_data

        except requests.exceptions.RequestException as e:
            logger.error(f"Error collecting CPI data: {e}", exc_info=True)
            return {}
        except (KeyError, IndexError) as e:
            logger.error(f"Error parsing BLS API response: {e}", exc_info=True)
            return {}
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}", exc_info=True)
            return {}