def fits(first: dict[str, str], second: dict[str, str], max_chars: int) -> bool:
    return len(first["text"]) + len(second["heading"]) + len(second["text"]) + 2 <= max_chars


def combine(first: dict[str, str], second: dict[str, str]) -> dict[str, str]:
    return {"heading": first["heading"], "text": f"{first['text']}\n{second['heading']}\n{second['text']}"}


def merge_small_parts(parts: list[dict[str, str]], min_chars: int, max_chars: int) -> list[dict[str, str]]:
    merged: list[dict[str, str]] = []
    for part in parts:
        if merged and len(merged[-1]["text"]) < min_chars and fits(merged[-1], part, max_chars):
            merged[-1] = combine(merged[-1], part)
        else:
            merged.append(dict(part))
    if len(merged) > 1 and len(merged[-1]["text"]) < min_chars and fits(merged[-2], merged[-1], max_chars):
        last = merged.pop()
        merged[-1] = combine(merged[-1], last)
    return merged