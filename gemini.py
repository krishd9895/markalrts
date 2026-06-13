import asyncio
import logging
from datetime import datetime, timedelta, timezone

from google import genai
from google.genai import types
from google.genai.errors import APIError

from config import db

logger = logging.getLogger(__name__)

# Indian Standard Time: UTC+5:30
IST = timezone(timedelta(hours=5, minutes=30))


class GeminiClusterManager:
    """
    Manages multiple Gemini API keys with:
    - In-memory cooldown tracking per key (429 / 503 circuit breaking)
    - Async queue that serialises all requests
    - Minimum 60-second gap between consecutive API calls
    """

    def __init__(self):
        self.keys = []                              # [{"key": str, "cooldown_until": datetime}]
        self.current_idx = 0
        self._queue: asyncio.Queue = None           # initialised on first call
        self._worker_task: asyncio.Task = None      # background drain worker
        self._last_call_time: float = 0.0           # monotonic time of last dispatch
        self.MIN_GAP_SECONDS = 60                   # minimum delay between requests

    # ── Queue worker ────────────────────────────────────────────────

    async def _worker(self):
        """Drains the request queue one at a time, honouring the rate gap."""
        while True:
            prompt, system_instruction, response_mime_type, future = await self._queue.get()
            elapsed = asyncio.get_event_loop().time() - self._last_call_time
            if elapsed < self.MIN_GAP_SECONDS:
                wait = self.MIN_GAP_SECONDS - elapsed
                logger.info(f"[Gemini] Rate-gap: waiting {wait:.1f}s before next call.")
                await asyncio.sleep(wait)
            try:
                result = await self._call_api(prompt, system_instruction, response_mime_type)
                future.set_result(result)
            except Exception as e:
                future.set_exception(e)
            finally:
                self._last_call_time = asyncio.get_event_loop().time()
                self._queue.task_done()

    def _ensure_worker(self):
        if self._queue is None:
            self._queue = asyncio.Queue()
        if self._worker_task is None or self._worker_task.done():
            self._worker_task = asyncio.ensure_future(self._worker())

    # ── Key management ───────────────────────────────────────────────

    async def sync_keys(self):
        """Reload keys from MongoDB, preserving in-memory cooldown state."""
        doc = await db["config"].find_one({"_id": "gemini_settings"})
        if doc and "keys" in doc:
            current_cooldowns = {
                k["key"]: k["cooldown_until"]
                for k in self.keys
                if "cooldown_until" in k
            }
            self.keys = []
            for k in doc["keys"]:
                stored = k.get("cooldown_until", None)
                if stored is None:
                    cd = datetime.min.replace(tzinfo=IST)
                elif isinstance(stored, datetime):
                    cd = stored.astimezone(IST) if stored.tzinfo else stored.replace(tzinfo=IST)
                else:
                    cd = datetime.min.replace(tzinfo=IST)
                self.keys.append({
                    "key": k["key"],
                    "cooldown_until": current_cooldowns.get(k["key"], cd)
                })
        else:
            self.keys = []
        logger.info(f"Gemini Key Cluster synchronised. Active Keys: {len(self.keys)}")

    def _get_next_available_key(self):
        now = datetime.now(IST)
        for _ in range(len(self.keys)):
            candidate = self.keys[self.current_idx]
            self.current_idx = (self.current_idx + 1) % len(self.keys)
            cd = candidate["cooldown_until"]
            if cd.tzinfo is None:
                cd = cd.replace(tzinfo=IST)
            if cd <= now:
                return candidate
        return None

    # ── API call with backoff ────────────────────────────────────────

    async def _call_api(
        self,
        prompt: str,
        system_instruction: str = None,
        response_mime_type: str = None,
    ) -> str:
        """Direct API call with exponential backoff. Called only from the queue worker."""
        backoff_delay = 2
        max_retries = 5

        for _ in range(max_retries):
            await self.sync_keys()
            candidate = self._get_next_available_key()

            if not candidate:
                logger.error("All Gemini keys cooling down or pool empty. Sleeping 30s...")
                await asyncio.sleep(30)
                continue

            try:
                client = genai.Client(api_key=candidate["key"])
                cfg = {}
                if system_instruction:
                    cfg["system_instruction"] = system_instruction
                if response_mime_type:
                    cfg["response_mime_type"] = response_mime_type

                response = client.models.generate_content(
                    model="gemini-2.0-flash",
                    contents=prompt,
                    config=types.GenerateContentConfig(**cfg) if cfg else None,
                )
                return response.text

            except APIError as e:
                if e.code in (429, 503):
                    cooldown_time = datetime.now(IST) + timedelta(minutes=5)
                    candidate["cooldown_until"] = cooldown_time
                    logger.warning(
                        f"[{e.code}] Key cooling down until "
                        f"{cooldown_time.strftime('%H:%M:%S IST')}. Retrying..."
                    )
                    await db["config"].update_one(
                        {"_id": "gemini_settings", "keys.key": candidate["key"]},
                        {"$set": {"keys.$.cooldown_until": cooldown_time}},
                    )
                else:
                    logger.error(f"Gemini API Error: {e}")
                await asyncio.sleep(backoff_delay)
                backoff_delay *= 2

            except Exception as ex:
                logger.error(f"Unexpected Gemini exception: {ex}")
                await asyncio.sleep(backoff_delay)
                backoff_delay *= 2

        raise RuntimeError("Gemini API failed after maximum retries.")

    # ── Public interface ─────────────────────────────────────────────

    async def generate_content(
        self,
        prompt: str,
        system_instruction: str = None,
        response_mime_type: str = None,
    ) -> str:
        """
        Enqueue a generation request and await the result.
        All requests are dispatched one at a time with a 60-second gap.
        """
        self._ensure_worker()
        future = asyncio.get_event_loop().create_future()
        await self._queue.put((prompt, system_instruction, response_mime_type, future))
        return await future


# Singleton instance — import this everywhere
ai_manager = GeminiClusterManager()
