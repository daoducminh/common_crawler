import httpx
import logging

logger = logging.getLogger(__name__)


class DiscordNotifier:
    def __init__(self, url: str):
        self.url = url

    async def send(self, content: str) -> str:
        if not self.url:
            logger.warning("No Discord Webhook URL provided, skipping notification")
            return None

        payload = {"content": content}
        async with httpx.AsyncClient() as client:
            try:
                # Add wait=true to get the message ID in response
                url = f"{self.url}?wait=true"
                response = await client.post(url, json=payload)
                response.raise_for_status()
                data = response.json()
                msg_id = data.get("id")
                logger.info(f"Discord notification sent successfully, msg_id: {msg_id}")
                return msg_id
            except Exception as e:
                logger.error(f"Failed to send Discord notification: {e}")
                return None

    async def delete(self, message_id: str):
        if not self.url or not message_id:
            return

        async with httpx.AsyncClient() as client:
            try:
                # Webhook delete message URL: [webhook_url]/messages/[message_id]
                url = f"{self.url}/messages/{message_id}"
                response = await client.delete(url)
                response.raise_for_status()
                logger.info(f"Discord message {message_id} deleted successfully")
            except Exception as e:
                logger.error(f"Failed to delete Discord message {message_id}: {e}")
