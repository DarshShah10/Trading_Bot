# collectors/market/crypto.py
import logging
import datetime
from typing import Dict, List
import yfinance as yf
from collectors.base import BaseDataCollector

logger = logging.getLogger(__name__)

class CryptoMarketDataCollector(BaseDataCollector):
    """Collects cryptocurrency market data (OHLCV) using yfinance."""

    def collect_data(self) -> List[Dict]:
        """Collects OHLCV and limited order book proxy data for cryptocurrencies."""
        collected_data = []

        try:
            for asset in self.config.get("assets", ["BTC-USD"]):  # Default: Bitcoin
                ticker = yf.Ticker(asset)

                # --- OHLCV Data ---
                data = ticker.history(period="1d")  # Get the last day's data

                if not data.empty:
                    for index, row in data.iterrows():
                        processed_data = {
                            "source": "yfinance",
                            "data_type": "crypto_market_data",
                            "asset": asset,
                            "timestamp": index.to_pydatetime().isoformat(),
                            "open": row["Open"],
                            "high": row["High"],
                            "low": row["Low"],
                            "close": row["Close"],
                            "volume": row["Volume"],
                            "raw_data": row.to_dict()  # Store the entire row as raw data
                        }
                        collected_data.append(processed_data)
                else:
                    logger.warning(f"No OHLCV data returned for {asset}")

                # --- Order Book Proxy (Limited by yfinance) ---
                # yfinance provides *very* limited "order book" information
                # (only current bid/ask).  For real order book data, you NEED
                # a dedicated market data provider (e.g., a crypto exchange API).
                info = ticker.info
                if info:
                    order_book_proxy = {
                        "source": "yfinance",
                        "data_type": "crypto_order_book_proxy",
                        "asset": asset,
                        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                        "bid": info.get("bid"),  # Current best bid price
                        "ask": info.get("ask"),  # Current best ask price
                        "bidSize": info.get("bidSize"),  # Size of the best bid
                        "askSize": info.get("askSize"),  # Size of the best ask
                        "raw_data": {k: info[k] for k in ('bid', 'ask', 'bidSize', 'askSize') if k in info} #Only include keys taht exists
                    }
                    collected_data.append(order_book_proxy)
                else:
                    logger.warning(f"No order book proxy data available for {asset}")


        except Exception as e:
            logger.error(f"Error collecting crypto market data for {asset}: {e}", exc_info=True)

        return collected_data