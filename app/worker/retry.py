import random


def backoff_seconds(attempt: int, base: float = 30, factor: float = 4, jitter: float = 0.1) -> float:
    """Delay before the next attempt, after `attempt` failed.
    attempt=1 -> ~30s, attempt=2 -> ~120s.
    Jitter (+-10%) prevents many failed jobs from retrying at the exact same moment."""
    delay = base * factor ** (attempt - 1)
    return delay * random.uniform(1 - jitter, 1 + jitter)