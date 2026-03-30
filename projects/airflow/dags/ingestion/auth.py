"""
vnstock authentication helper.

Reads VNSTOCK_API_KEY from the environment and registers it with the
vnstock library (register_user). This upgrades the session from Guest
(20 req/min) to at least Community (60 req/min) or Sponsor tier.

Call setup_vnstock_auth() once at the start of every Airflow task that
makes vnstock API calls, since each task runs in its own process.
"""

import logging
import os

log = logging.getLogger(__name__)

_AUTH_DONE = False   # module-level flag so we only register once per process


def setup_vnstock_auth() -> None:
    """Register the vnstock API key if VNSTOCK_API_KEY is set in the environment."""
    global _AUTH_DONE
    if _AUTH_DONE:
        return

    api_key = os.environ.get("VNSTOCK_API_KEY", "").strip()
    if not api_key:
        log.warning(
            "VNSTOCK_API_KEY is not set — running as Guest tier (20 req/min). "
            "Set the env var to unlock Community/Sponsor rate limits."
        )
        _AUTH_DONE = True
        return

    try:
        from vnstock import register_user
        register_user(api_key=api_key)
        log.info("vnstock authenticated (key: %s...%s)", api_key[:4], api_key[-4:])
    except Exception as e:
        log.warning("vnstock auth failed, continuing as Guest: %s", e)

    _AUTH_DONE = True
