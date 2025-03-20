# collectors/unstructured/twitter.py
import logging
import tweepy  # Requires: tweepy
import datetime
from typing import List, Dict, Optional
import os
from dotenv import load_dotenv  # Requires: python-dotenv

from collectors.base import BaseDataCollector
from core.utils import get_random_user_agent  # Corrected import

logger = logging.getLogger(__name__)

class TwitterCollector(BaseDataCollector):
    """
    Collects tweets from specified Twitter accounts.  Uses the Tweepy library
    to interact with the Twitter API.  Requires API keys (see below).
    """

    def __init__(self, config: Dict, data_queue: queue.Queue, get_user_agent_fn):
        super().__init__(config, data_queue)
        self.get_user_agent = get_user_agent_fn
        self.api = self._initialize_tweepy()


    def _initialize_tweepy(self) -> Optional[tweepy.API]:
        """
        Initializes the Tweepy API client using credentials from environment
        variables.  Looks for:
            TWITTER_API_KEY
            TWITTER_API_SECRET_KEY
            TWITTER_ACCESS_TOKEN
            TWITTER_ACCESS_TOKEN_SECRET

        Returns:
            A Tweepy API object, or None if authentication fails.
        """
        load_dotenv()  # Load environment variables from .env file
        try:
            api_key = os.environ["TWITTER_API_KEY"]
            api_secret_key = os.environ["TWITTER_API_SECRET_KEY"]
            access_token = os.environ["TWITTER_ACCESS_TOKEN"]
            access_token_secret = os.environ["TWITTER_ACCESS_TOKEN_SECRET"]

            # Authenticate with the Twitter API.
            auth = tweepy.OAuthHandler(api_key, api_secret_key)
            auth.set_access_token(access_token, access_token_secret)
            return tweepy.API(auth, wait_on_rate_limit=True)  # Enable rate limiting handling

        except KeyError as e:
            logger.error(f"Missing Twitter API credential: {e}.  Twitter collector will not work.")
            return None
        except tweepy.TweepyException as e:
            logger.error(f"Error authenticating with Twitter API: {e}")
            return None
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}")
            return None
    def collect_data(self) -> List[Dict]:
        """Collects recent tweets from the configured accounts."""
        collected_data = []

        if not self.api:  # Check if Tweepy was initialized successfully.
            return collected_data  # Return empty list if no API access

        for account in self.config.get("accounts", []):  # Iterate through accounts
            try:
                # Fetch recent tweets (adjust count as needed).
                tweets = self.api.user_timeline(screen_name=account, count=25, tweet_mode="extended")

                for tweet in tweets:
                    tweet_data = {
                        "source": "twitter",
                        "data_type": "tweet",
                        "account": account,
                        "timestamp": tweet.created_at.isoformat(),  # Use Tweepy's datetime
                        "content": tweet.full_text,  # Use full_text for extended tweets
                        "likes": tweet.favorite_count,
                        "retweets": tweet.retweet_count,
                        "tweet_id": tweet.id_str,
                        "user_id": tweet.user.id_str,
                        "user_name": tweet.user.screen_name,
                        "user_followers": tweet.user.followers_count,
                        "raw_data": tweet._json  # Store the raw tweet data
                    }
                    collected_data.append(tweet_data)

                logger.info(f"Collected {len(tweets)} tweets from {account}")

            except tweepy.TweepyException as e:
                logger.error(f"Error collecting tweets from {account}: {e}", exc_info=True)
            except Exception as e:
                logger.error(f"An unexpected error occurred while collecting tweets from {account}: {e}", exc_info=True)

        return collected_data