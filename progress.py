
import logging

logger = logging.getLogger(__name__)


class ProgressManager:
    """
    Manages progress messages for long-running operations like OCR and channel scanning
    """
    def __init__(self, bot, owners):
        self.bot = bot
        self.owners = owners
        self.progress_messages = {}  # owner: message_id

    async def send_initial_progress(self, title: str, initial_text: str):
        """
        Send initial progress message to all owners
        """
        self.progress_messages = {}
        full_text = f"{title}\n{initial_text}" if initial_text else title
        for owner in self.owners:
            try:
                msg = await self.bot.send_message(
                    owner,
                    full_text,
                    link_preview=False
                )
                self.progress_messages[owner] = msg.id
                logger.info(f"Sent initial progress message to {owner}")
            except Exception as e:
                logger.error(f"Failed to send progress message to {owner}: {e}")

    async def update_progress(self, title: str, progress_text: str):
        """
        Update all progress messages with new text
        """
        full_text = f"{title}\n{progress_text}"
        for owner, msg_id in self.progress_messages.items():
            try:
                await self.bot.edit_message(
                    owner,
                    msg_id,
                    full_text,
                    link_preview=False
                )
            except Exception as e:
                logger.error(f"Failed to update progress for {owner}: {e}")

    async def finalize_progress(self, final_text: str):
        """
        Finalize progress messages with completion text
        """
        for owner, msg_id in self.progress_messages.items():
            try:
                await self.bot.edit_message(
                    owner,
                    msg_id,
                    final_text,
                    link_preview=False
                )
                logger.info(f"Finalized progress message for {owner}")
            except Exception as e:
                logger.error(f"Failed to finalize progress for {owner}: {e}")
