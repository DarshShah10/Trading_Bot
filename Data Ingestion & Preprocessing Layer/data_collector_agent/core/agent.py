# core/agent.py
import logging
import queue
import time
import threading
import datetime
from typing import Dict, List
import yaml

from core.storage import DataStorage, PostgresDataStorage  # Import storage classes
from core.utils import get_random_user_agent, solve_captcha # Import utility functions
from core.exceptions import ConfigurationError, DataCollectionError
from collectors.base import BaseDataCollector # Centralized import

# Import all collector classes using dynamic imports:
from collectors import market, unstructured, macro
import importlib



logger = logging.getLogger(__name__)


class DataCollectorAgent:
    """
    The main class for the data collector agent.  Orchestrates data
    collection, processing, and storage.
    """

    def __init__(self, config_path: str = "config.yaml"):
        """
        Initializes the DataCollectorAgent.

        Args:
            config_path: Path to the configuration YAML file.
        """
        self.config_path = config_path
        self.config = self._load_config()
        self.data_queue = queue.Queue()
        self.running = False
        self.collectors: Dict[str, BaseDataCollector] = {}
        self.lock = threading.Lock()  # Lock for thread-safe operations

        # Initialize storage based on configuration
        db_config = self.config.get("storage", {}).get("database", {})
        if db_config.get("enabled", False):
            self.data_storage = PostgresDataStorage(db_config)
        else:
            self.data_storage = DataStorage(self.config.get("storage", {}))

        self.captcha_api_key = self.config.get("captcha", {}).get("api_key")  # For utils

    def _load_config(self) -> Dict:
        """Loads the configuration from the YAML file."""
        try:
            with open(self.config_path, 'r') as file:
                return yaml.safe_load(file)
        except FileNotFoundError:
            raise ConfigurationError(f"Configuration file not found: {self.config_path}")
        except yaml.YAMLError as e:
            raise ConfigurationError(f"Error parsing YAML configuration: {e}")
        except Exception as e:
            raise ConfigurationError(f"Error loading configuration: {e}")

    def _get_collector_class(self, collector_type: str, source: str):
        """Dynamically imports and returns the collector class."""

        try:
            if collector_type == "market":
                module = importlib.import_module(f"collectors.market.{source}")
            elif collector_type == "unstructured":
                module = importlib.import_module(f"collectors.unstructured.{source}")
            elif collector_type == "macro":
                module = importlib.import_module(f"collectors.macro.{source}")
            else:
                raise ValueError(f"Invalid collector type: {collector_type}")

            # Get the class name.  Assumes class name follows convention (e.g., Crypto -> CryptoCollector).
            class_name = source.capitalize()
            if collector_type == "market" and source == "crypto":
                class_name = "CryptoMarketData"  # Special case due to naming
            elif collector_type == "market" and source == "equities":
                class_name = "EquitiesMarketData"
            elif collector_type == "unstructured" and source == "sec_filings":
                class_name = "SECFilings"
            elif collector_type == "unstructured" and source == "earnings":
                class_name = "EarningsCalls"
            elif collector_type == "macro" and source == "fed_rates":
                class_name = "FedRates"
            elif collector_type == 'macro' and source == 'geopolitical':
                class_name = 'GeopoliticalEvents'

            class_name += "Collector"

            return getattr(module, class_name)  # Get the class object

        except (ImportError, AttributeError, ValueError) as e:
            logger.error(f"Failed to load collector class for {collector_type}.{source}: {e}")
            return None # Return None so it can be handled gracefully

    def _start_collectors(self, collector_type: str, config: Dict):
      """Starts collectors of a specific type."""
      for source, src_config in config.items():
          if not src_config.get("enabled", True):
              continue

          logger.info(f"Starting {collector_type} data collector for {source}")
          collector_class = self._get_collector_class(collector_type, source)
          if collector_class is None: # Handle loading failures
              continue

          # Pass necessary arguments, including utility functions.
          if collector_type == "unstructured":
              if source in ("news", "sec_filings"):
                  collector = collector_class(src_config, self.data_queue, get_random_user_agent, self.solve_captcha)
              elif source in ("twitter","reddit", "earnings"):
                  collector = collector_class(src_config, self.data_queue, get_random_user_agent) #No captcha needed
              else: #Just incase to prevent unexpected error
                  collector = collector_class(src_config, self.data_queue)


          else: #For market and macro
              collector = collector_class(src_config, self.data_queue)

          self.collectors[f"{collector_type}_{source}"] = collector
          collector_thread = threading.Thread(target=collector.run, daemon=True)
          collector_thread.start()

    def start(self):
        """Starts the data collection process."""
        if self.running:
            logger.warning("Data collector is already running.")
            return

        self.running = True

        # Start the processor thread
        processor_thread = threading.Thread(target=self._process_data_queue, daemon=True)
        processor_thread.start()

        # Start all collectors
        for collector_type, config in self.config.get("collectors", {}).items():
            if not config.get("enabled", True):
                continue
            self._start_collectors(collector_type, config)

        logger.info("Data collector agent started.")

    def stop(self):
        """Stops the data collection process."""
        self.running = False

        # Stop all collectors
        for collector in self.collectors.values():
            collector.stop()

        logger.info("Data collector agent stopped.")

    def _process_data_queue(self):
        """Processes data from the queue and stores it."""
        while self.running or not self.data_queue.empty():
            try:
                data = self.data_queue.get(timeout=1)  # Block for max. 1 sec
                self._process_data(data)
                self.data_queue.task_done()  # Signal that processing is complete

            except queue.Empty:
                continue  # If the queue is empty, just loop back

            except Exception as e:
                logger.error(f"Error processing data: {e}")


    def _process_data(self, data: Dict):
      """Processes and stores the collected data."""
      try:
          # Add metadata if not present, using UTC for consistency
          if "timestamp" not in data:
              data["timestamp"] = datetime.datetime.now(datetime.timezone.utc).isoformat()

          # Store data using the configured storage mechanism
          with self.lock:
              self.data_storage.store_data(data)

          logger.debug(f"Processed data: {data.get('source')}")

      except Exception as e:
          logger.error(f"Error in data processing: {e}")

    def solve_captcha(self, site_key: str, url: str) -> str:
        """
        Solve CAPTCHA using AntiCaptcha API.  This now uses the agent's API key.

        Args:
            site_key: The site key for reCAPTCHA
            url: The URL of the page with CAPTCHA

        Returns:
            The solution token, or an empty string on failure.
        """
        return solve_captcha(site_key, url, self.captcha_api_key)