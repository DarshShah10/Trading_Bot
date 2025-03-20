# collectors/unstructured/reddit.py
import logging
import praw  # Requires: praw
import datetime
from typing import List, Dict, Optional
import os
from dotenv import load_dotenv
from collectors.base import BaseDataCollector
from core.utils import get_random_user_agent  # Corrected import

logger = logging.getLogger(__name__)

class RedditCollector(BaseDataCollector):
    """
    Collects posts and comments from specified subreddits using the PRAW
    (Python Reddit API Wrapper) library.  Requires Reddit API credentials.
    """
    def __init__(self, config: Dict, data_queue: queue.Queue, get_user_agent_fn):
      super().__init__(config, data_queue)
      self.get_user_agent = get_user_agent_fn
      self.reddit = self._initialize_praw()

    def _initialize_praw(self) -> Optional[praw.Reddit]:
        """
        Initializes the PRAW Reddit instance using credentials from environment
        variables.  Looks for:
            REDDIT_CLIENT_ID
            REDDIT_CLIENT_SECRET
            REDDIT_USER_AGENT  (Use a descriptive user agent)

        Returns:
            A PRAW Reddit object, or None if authentication fails.
        """
        load_dotenv()
        try:
            client_id = os.environ["REDDIT_CLIENT_ID"]
            client_secret = os.environ["REDDIT_CLIENT_SECRET"]
            user_agent = os.environ["REDDIT_USER_AGENT"]

            return praw.Reddit(
                client_id=client_id,
                client_secret=client_secret,
                user_agent=user_agent,
            )
        except KeyError as e:
            logger.error(f"Missing Reddit API credential: {e}. Reddit collector will not work.")
            return None
        except praw.exceptions.PRAWException as e:
            logger.error(f"Error authenticating with Reddit API: {e}")
            return None
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}")
            return None


    def collect_data(self) -> List[Dict]:
        """Collects recent posts and top comments from configured subreddits."""
        collected_data = []
        if not self.reddit:
            return collected_data

        for subreddit_name in self.config.get("subreddits", []):
            try:
                subreddit = self.reddit.subreddit(subreddit_name)

                # Fetch top posts (adjust limit as needed)
                for submission in subreddit.top(time_filter="day", limit=25):
                    post_data = {
                        "source": "reddit",
                        "data_type": "reddit_post",
                        "subreddit": subreddit_name,
                        "timestamp": datetime.datetime.utcfromtimestamp(submission.created_utc).isoformat(),
                        "title": submission.title,
                        "content": submission.selftext,
                        "author": str(submission.author),  # Convert Redditor object to string
                        "score": submission.score,
                        "comments": submission.num_comments,
                        "url": submission.url,
                        "post_id": submission.id,
                        "upvote_ratio": submission.upvote_ratio,
                        "raw_data": {  #  Store useful attributes from the submission object
                            "gilded": submission.gilded,
                            "over_18": submission.over_18,
                            "spoiler": submission.spoiler,
                            "stickied": submission.stickied
                        }
                    }
                    collected_data.append(post_data)

                    # Fetch top-level comments (adjust limit as needed)
                    submission.comments.replace_more(limit=0)  # Avoid MoreComments objects
                    for comment in submission.comments.list()[:10]:  # Limit to top 10
                        if isinstance(comment, praw.models.Comment): #Ensure not morecomments
                            comment_data = {
                                "source": "reddit",
                                "data_type": "reddit_comment",
                                "subreddit": subreddit_name,
                                "timestamp": datetime.datetime.utcfromtimestamp(comment.created_utc).isoformat(),
                                "post_id": submission.id,  # Link comment to post
                                "comment_id": comment.id,
                                "author": str(comment.author),
                                "content": comment.body,
                                "score": comment.score,
                                 "raw_data": {
                                    "gilded": comment.gilded,
                                    "is_submitter": comment.is_submitter,
                                    "stickied": comment.stickied,
                                    # Add other relevant comment attributes as needed
                                },
                            }
                            collected_data.append(comment_data)
                logger.info(f"Collected posts and comments from r/{subreddit_name}")

            except praw.exceptions.PRAWException as e:
                logger.error(f"Error collecting data from r/{subreddit_name}: {e}", exc_info=True)
            except Exception as e:
                logger.error(f"An unexpected error occurred while collecting data from r/{subreddit_name}: {e}", exc_info=True)

        return collected_data