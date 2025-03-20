import logging
import time
import queue
from typing import Union, Dict, List

logger = logging.getLogger(__name__)

class BaseDataCollector:
    """
    Base class for all data collectors. Defines a common interface and
    provides shared functionality for data collection processes.
    """

    def __init__(self, config: Dict, data_queue: queue.Queue):
        """
        Initializes the base data collector.

        Args:
            config (Dict): Configuration dictionary containing collector-specific settings
                (e.g., API keys, asset lists, intervals).
            data_queue (queue.Queue): The shared queue where collected data will be placed.
        """
        self.config = config
        self.data_queue = data_queue
        self.running = False
        self.interval = self.config.get("interval", 60)  # Default to 60 seconds
        self._last_collection_time = time.monotonic()

    def run(self):
        """
        The main collection loop. Runs continuously while `self.running` is True,
        collecting data at the specified interval with precise timing.
        """
        self.running = True
        logger.info(f"{self.__class__.__name__} starting.")
        self._last_collection_time = time.monotonic()

        while self.running:
            try:
                data = self.collect_data()
                if data:
                    if isinstance(data, list):
                        for item in data:
                            self.data_queue.put(item)
                    else:
                        self.data_queue.put(data)
            except Exception as e:
                logger.error(f"{self.__class__.__name__} encountered an error: {e}", exc_info=True)
                # Optional: Implement retry logic with exponential backoff
            
            self._sleep_until_next_interval()
            self._last_collection_time = time.monotonic()

    def collect_data(self) -> Union[Dict, List[Dict]]:
        """
        Abstract method to be implemented by subclasses.

        Returns:
            Union[Dict, List[Dict]]: Collected data as a dictionary or a list of dictionaries.
            Should return an empty dictionary or list if no data was collected.
        """
        raise NotImplementedError("Subclasses must implement the collect_data method.")

    def stop(self):
        """
        Stops the data collector by setting `self.running` to False.
        """
        self.running = False
        logger.info(f"{self.__class__.__name__} stopping.")

    def _sleep_until_next_interval(self):
        """
        Sleeps for the remaining time until the next scheduled data collection.
        Adjusts for the time taken by `collect_data` to maintain precise intervals.
        """
        elapsed_time = time.monotonic() - self._last_collection_time
        sleep_time = max(0, self.interval - elapsed_time)
        logger.debug(f"{self.__class__.__name__} sleeping for {sleep_time:.2f} seconds.")
        time.sleep(sleep_time)