# core/utils.py
import logging
import random
from typing import List, Optional
from anticaptchaofficial.recaptchav2proxyless import *
from core.exceptions import CaptchaError

logger = logging.getLogger(__name__)


def get_random_user_agent(user_agents_file: str = "user_agents.txt") -> str:
    """
    Retrieves a random user agent string from the specified file.

    Args:
        user_agents_file: Path to the file containing user agents (one per line).

    Returns:
        A randomly selected user agent string.  Returns a default if the file
        cannot be read.
    """
    try:
        with open(user_agents_file, 'r') as file:
            user_agents = [line.strip() for line in file if line.strip()]
        return random.choice(user_agents)
    except FileNotFoundError:
        logger.warning(f"User agents file not found: {user_agents_file}. Using default.")
        return "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:98.0) Gecko/20100101 Firefox/98.0"
    except Exception as e:
        logger.warning(f"Error reading user agents file: {e}. Using default.")
        return "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:98.0) Gecko/20100101 Firefox/98.0"

def solve_captcha(site_key: str, url: str, api_key: str) -> str:
    """
    Solves a reCAPTCHA v2 using the AntiCaptcha service.

    Args:
        site_key: The reCAPTCHA site key.
        url: The URL of the page containing the reCAPTCHA.
        api_key: your AntiCaptcha API KEY.

    Returns:
        The CAPTCHA solution token, or an empty string on failure.
    """
    if not api_key:
        logger.error("No CAPTCHA API key provided.")
        return ""

    solver = recaptchaV2Proxyless()
    solver.set_verbose(1)
    solver.set_key(api_key)
    solver.set_website_url(url)
    solver.set_website_key(site_key)

    try:
        token = solver.solve_and_return_solution()
        if token:
            logger.info(f"CAPTCHA solved for {url}")
            return token
        else:
            logger.error(f"CAPTCHA solution failed: {solver.error_code}")
            return ""  # Return empty string on failure
    except Exception as e:
        logger.error(f"Error solving CAPTCHA: {e}")
        raise CaptchaError(f"Error solving CAPTCHA: {e}") from e