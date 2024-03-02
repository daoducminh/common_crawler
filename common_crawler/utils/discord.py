import httpx
import logging

logger = logging.getLogger(__name__)


class DiscordWebhook:
    def __init__(self, url: str):
        self.url = url

    async def send(self, content: str):
        if not self.url:
            logger.warning("No Discord Webhook URL provided, skipping notification")
            return

        payload = {"content": content}
        async with httpx.AsyncClient() as client:
            try:
                response = await client.post(self.url, json=payload)
                response.raise_for_status()
                logger.info("Discord notification sent successfully")
            except Exception as e:
                logger.error(f"Failed to send Discord notification: {e}")
