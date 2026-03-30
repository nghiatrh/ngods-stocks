"""
Cross-process sliding-window rate limiter for the vnstock API.

Multiple Airflow tasks run in separate OS processes (LocalExecutor).
An in-memory counter inside market.py / reference.py would be invisible to the
other processes, so calls from parallel tasks add up and exceed the per-minute cap.

This module uses a JSON file + an exclusive OS file-lock so that every process
on the same machine shares a single, consistent call log.

Usage:
    from ingestion.rate_limiter import throttle

    throttle()               # blocks until a slot is free
    result = api_call(...)   # safe to call immediately after
"""

import fcntl
import json
import logging
import os
import time

log = logging.getLogger(__name__)

# Location of the shared state files (must be writable by all Airflow workers)
_STATE_FILE = "/tmp/vnstock_rl_state.json"
_LOCK_FILE  = "/tmp/vnstock_rl.lock"

# Stay a few calls below the hard limit so transient clock skew / retries
# don't accidentally push us over.
_MAX_CALLS_PER_MINUTE = 55
_WINDOW_SECONDS       = 60.0


def throttle() -> None:
    """
    Block the calling process until making one more API call is safe.

    Algorithm (sliding window):
      1. Acquire an exclusive file lock (blocks other processes).
      2. Load the shared call-timestamp list from disk.
      3. Evict timestamps older than 60 seconds.
      4. If the window is full, sleep until the oldest timestamp ages out,
         then re-evict.
      5. Append now, persist the list, release the lock.
    """
    with open(_LOCK_FILE, "a") as lf:
        fcntl.flock(lf, fcntl.LOCK_EX)
        try:
            # --- load state ---
            try:
                with open(_STATE_FILE) as sf:
                    timestamps: list[float] = json.load(sf)
            except (FileNotFoundError, json.JSONDecodeError):
                timestamps = []

            now = time.time()
            timestamps = [t for t in timestamps if now - t < _WINDOW_SECONDS]

            # --- wait if window is full ---
            if len(timestamps) >= _MAX_CALLS_PER_MINUTE:
                oldest   = min(timestamps)
                wait_sec = _WINDOW_SECONDS - (now - oldest) + 0.05
                if wait_sec > 0:
                    log.debug(
                        "Rate limit: %d/%d calls in window — sleeping %.2fs",
                        len(timestamps), _MAX_CALLS_PER_MINUTE, wait_sec,
                    )
                    # Release lock while sleeping so other processes can proceed
                    fcntl.flock(lf, fcntl.LOCK_UN)
                    time.sleep(wait_sec)
                    fcntl.flock(lf, fcntl.LOCK_EX)

                    # Re-read & re-evict after sleeping
                    try:
                        with open(_STATE_FILE) as sf:
                            timestamps = json.load(sf)
                    except (FileNotFoundError, json.JSONDecodeError):
                        timestamps = []
                    now = time.time()
                    timestamps = [t for t in timestamps if now - t < _WINDOW_SECONDS]

            # --- record this call ---
            timestamps.append(time.time())
            with open(_STATE_FILE, "w") as sf:
                json.dump(timestamps, sf)

        finally:
            fcntl.flock(lf, fcntl.LOCK_UN)
