# collectors/market/equities.py
import logging
import datetime
from typing import Dict, List
import yfinance as yf
from collectors.base import BaseDataCollector

logger = logging.getLogger(__name__)

class EquitiesMarketDataCollector(BaseDataCollector):
    """Collects equities market data (OHLCV, order book proxy) using yfinance."""

    def collect_data(self) -> List[Dict]:
        """Collects OHLCV and limited order book proxy data for equities."""
        collected_data = []

        try:
            for ticker_symbol in self.config.get("tickers", ["AAPL"]):  # Default: AAPL
                ticker = yf.Ticker(ticker_symbol)

                # --- OHLCV Data ---
                data = ticker.history(period="1d")

                if not data.empty:
                    for index, row in data.iterrows():
                        processed_data = {
                            "source": "yfinance",
                            "data_type": "equities_market_data",
                            "asset": ticker_symbol,
                            "timestamp": index.to_pydatetime().isoformat(),  # Use pandas Timestamp
                            "open": row["Open"],
                            "high": row["High"],
                            "low": row["Low"],
                            "close": row["Close"],
                            "volume": row["Volume"],
                            "dividends": row["Dividends"],
                            "stock_splits": row["Stock Splits"],
                            "raw_data": row.to_dict()
                        }
                        collected_data.append(processed_data)
                else:
                    logger.warning(f"No OHLCV data returned for {ticker_symbol}")

                # --- Order Book Proxy (Limited by yfinance) ---
                info = ticker.info
                if info:
                    order_book_proxy = {
                        "source": "yfinance",
                        "data_type": "equities_order_book_proxy",
                        "asset": ticker_symbol,
                        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                        "bid": info.get("bid"),
                        "ask": info.get("ask"),
                        "bidSize": info.get("bidSize"),
                        "askSize": info.get("askSize"),
                         "raw_data": {k: info[k] for k in ('bid', 'ask', 'bidSize', 'askSize') if k in info}
                    }
                    collected_data.append(order_book_proxy)
                else:
                    logger.warning(f"No order book proxy data for {ticker_symbol}")

        except Exception as e:
            logger.error(f"Error collecting equities market data for {ticker_symbol}: {e}", exc_info=True)

        return collected_data