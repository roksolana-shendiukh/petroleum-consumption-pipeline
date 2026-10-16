from typing import Any


def format_context(chunks: list[dict[str, Any]], template: str) -> str:
    parts = []
    for position, chunk in enumerate(chunks, start=1):
        parts.append(
            template.format(
                index=position,
                title=chunk.get("title") or "",
                part_heading=chunk.get("part_heading") or "",
                source_url=chunk.get("source_url") or "",
                chunk_text=chunk.get("chunk_text") or "",
            )
        )
    return "\n\n".join(parts)


def build_messages(config: dict[str, Any], mode: str, question: str, chunks: list[dict[str, Any]]) -> tuple[str, str]:
    if mode == "retrieval":
        context = format_context(chunks, config["context_template"])
        user = config["user_template_retrieval"].format(context=context, question=question)
        return config["system_prompt_retrieval"], user
    if mode == "no_retrieval":
        user = config["user_template_no_retrieval"].format(question=question)
        return config["system_prompt_no_retrieval"], user
    raise ValueError(f"unknown mode: {mode}")


def render_prompt(system: str, user: str) -> str:
    return f"SYSTEM:\n{system}\n\nUSER:\n{user}"