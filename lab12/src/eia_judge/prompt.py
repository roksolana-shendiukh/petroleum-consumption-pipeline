from typing import Any

from eia_generation.prompt import format_context


def build_messages(
    config: dict[str, Any],
    mode: str,
    question: dict[str, Any],
    answer: str,
    chunks: list[dict[str, Any]],
) -> tuple[str, str]:
    values = {
        "question": question["question"],
        "answerable": "yes" if question["answerable"] else "no",
        "expected_answer": question["expected_answer"],
        "answer": answer,
    }
    if mode == "retrieval":
        values["context"] = format_context(chunks, config["context_template"])
        return config["system_prompt"], config["user_template_retrieval"].format(**values)
    if mode == "no_retrieval":
        return config["system_prompt"], config["user_template_no_retrieval"].format(**values)
    raise ValueError(f"unknown mode: {mode}")