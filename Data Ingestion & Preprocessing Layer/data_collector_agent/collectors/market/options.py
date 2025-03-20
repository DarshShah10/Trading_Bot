# collectors/market/options.py
import logging
import datetime
from typing import Dict, List
import yfinance as yf
from collectors.base import BaseDataCollector

logger = logging.getLogger(__name__)

class OptionsDataCollector(BaseDataCollector):
    """Collects options chain data using yfinance."""

    def collect_data(self) -> List[Dict]:
        """Collects options data for the specified underlyings."""
        collected_data = []

        try:
            for underlying in self.config.get("underlyings", ["AAPL"]):  # Default: AAPL
                ticker = yf.Ticker(underlying)
                expirations = ticker.options  # Get available expiration dates

                if not expirations:
                    logger.warning(f"No options expirations found for {underlying}")
                    continue

                for expiration in expirations:
                    try:
                        options_chain = ticker.option_chain(expiration)

                        # Process calls
                        for index, row in options_chain.calls.iterrows():
                            call_data = {
                                "source": "yfinance",
                                "data_type": "options_data",
                                "asset": underlying,
                                "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                                "option_type": "call",
                                "expiration": expiration,
                                "strike": row["strike"],
                                "lastPrice": row["lastPrice"],
                                "bid": row["bid"],
                                "ask": row["ask"],
                                "volume": row["volume"],
                                "openInterest": row["openInterest"],
                                "impliedVolatility": row["impliedVolatility"],
                                "inTheMoney": row["inTheMoney"],
                                "contractSize": row["contractSize"],
                                "currency": row["currency"],
                                "lastTradeDate": row["lastTradeDate"].to_pydatetime().isoformat() if row["lastTradeDate"] is not None else None, #Handle potential NaT
                                "contractSymbol": row['contractSymbol'],
                                "raw_data": row.to_dict()
                            }
                            collected_data.append(call_data)

                        # Process Puts
                        for index, row in options_chain.puts.iterrows():
                            put_data = {
                                'source': 'yfinance',
                                'data_type': 'options_data',
                                'asset': underlying,
                                'timestamp': datetime.datetime.now(datetime.timezone.utc).isoformat(),
                                'option_type': 'put',
                                'expiration': expiration,
                                'strike': row['strike'],
                                'lastPrice': row['lastPrice'],
                                'bid': row['bid'],
                                'ask': row['ask'],
                                'volume': row['volume'],
                                'openInterest': row['openInterest'],
                                'impliedVolatility': row['impliedVolatility'],
                                'inTheMoney': row['inTheMoney'],
                                "contractSize": row["contractSize"],
                                "currency": row["currency"],
                                 "lastTradeDate": row["lastTradeDate"].to_pydatetime().isoformat() if row["lastTradeDate"] is not None else None, #Handle potential NaT
                                "contractSymbol": row['contractSymbol'],
                                "raw_data": row.to_dict()
                            }
                            collected_data.append(put_data)

                        logger.info(f"Collected options data for {underlying} expiration {expiration}")

                    except Exception as e:
                        logger.error(f"Error processing options chain for {underlying} expiration {expiration}: {e}", exc_info=True)
                        # Consider *not* raising here, to continue with other expirations.

        except Exception as e:
            logger.error(f"Error collecting options data for {underlying}: {e}", exc_info=True)
        return collected_data