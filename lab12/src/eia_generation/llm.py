import logging
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

logger = logging.getLogger(__name__)


def ask(
    client: WorkspaceClient,
    model: str,
    system: str,
    user: str,
    temperature: float,
    max_tokens: int,
    max_retries: int,
    retry_wait_s: int,
) -> tuple[str | None, str | None, int]:
    messages = [
        ChatMessage(role=ChatMessageRole.SYSTEM, content=system),
        ChatMessage(role=ChatMessageRole.USER, content=user),
    ]
    started = time.perf_counter()
    answer = None
    error = None
    for attempt in range(1, max_retries + 1):
        try:
            response = client.serving_endpoints.query(
                name=model, messages=messages, temperature=temperature, max_tokens=max_tokens
            )
            answer = response.choices[0].message.content
            error = None
            break
        except Exception as exc:
            error = str(exc)
            logger.warning("Attempt %d of %d failed: %s", attempt, max_retries, error)
            if attempt < max_retries:
                time.sleep(retry_wait_s)
    latency_ms = int((time.perf_counter() - started) * 1000)
    return answer, error, latency_ms