# collectors/market/futures.py
import logging
import datetime
from typing import Dict, List
import yfinance as yf
from collectors.base import BaseDataCollector

logger = logging.getLogger(__name__)

class FuturesDataCollector(BaseDataCollector):
    """
    Collects futures data using yfinance (as a proxy, since yfinance has
    limited futures support).  For real, detailed futures data, you would
    need a specialized data provider.
    """

    def collect_data(self) -> List[Dict]:
        """
        Collects futures data (proxy) for the specified symbols.  Uses OHLCV
        data as a stand-in for actual futures contract details.
        """
        collected_data = []

        try:
            for symbol in self.config.get("symbols", ["ES=F"]):  # Example: E-mini S&P 500
                ticker = yf.Ticker(symbol)
                data = ticker.history(period="1d")

                if not data.empty:
                    for index, row in data.iterrows():
                        processed_data = {
                            "source": "yfinance",
                            "data_type": "futures_data",  # More accurate data type
                            "asset": symbol,
                            "timestamp": index.to_pydatetime().isoformat(),
                            "open": row["Open"],
                            "high": row["High"],
                            "low": row["Low"],
                            "close": row["Close"],
                            "volume": row["Volume"],
                            "raw_data": row.to_dict()
                        }
                        collected_data.append(processed_data)
                    logger.info(f"Collected futures data (proxy) for {symbol}")
                else:
                    logger.warning(f"Failed to collect futures data (proxy) for {symbol}")

                # --- Order Book Proxy (VERY Limited) ---
                info = ticker.info
                if info:
                  order_book_proxy = {
                      "source": "yfinance",
                      "data_type": "futures_order_book_proxy",  # Distinct type
                      "asset": symbol,
                      "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                      "bid": info.get("bid"),  # Current best bid price (limited)
                      "ask": info.get("ask"),  # Current best ask price (limited)
                      "bidSize": info.get("bidSize"),  # Size of best bid (limited)
                      "askSize": info.get("askSize"),  # Size of best ask (limited)
                      "raw_data": {k: info[k] for k in ('bid', 'ask', 'bidSize', 'askSize') if k in info}
                  }
                  collected_data.append(order_book_proxy)
                else:
                  logger.warning(f"No order book proxy available for futures symbol {symbol}")



        except Exception as e:
            logger.error(f"Error collecting futures data (proxy): {e}", exc_info=True)
        return collected_data