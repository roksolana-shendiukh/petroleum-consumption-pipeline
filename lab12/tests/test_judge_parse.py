import json

import pytest

from eia_judge.parse import content_text, parse_verdict


def _payload(**overrides):
    data = {
        "correctness": 4,
        "relevance": 5,
        "groundedness": 3,
        "refused": False,
        "reasoning": "Matches the source.",
    }
    data.update(overrides)
    return data


def _drop(key):
    data = _payload()
    del data[key]
    return json.dumps(data)


EXPECTED = {
    "correctness": 4,
    "relevance": 5,
    "groundedness": 3,
    "refused": False,
    "reasoning": "Matches the source.",
}


def test_valid_verdict():
    assert parse_verdict(json.dumps(_payload()), 1, 5, True) == EXPECTED


def test_missing_reasoning_defaults_to_empty():
    result = parse_verdict(_drop("reasoning"), 1, 5, True)
    assert result["reasoning"] == ""


def test_null_groundedness():
    result = parse_verdict(json.dumps(_payload(groundedness=None)), 1, 5, True)
    assert result["groundedness"] is None
    assert result["correctness"] == 4


def test_absent_groundedness():
    result = parse_verdict(_drop("groundedness"), 1, 5, True)
    assert result["groundedness"] is None


def test_groundedness_ignored_when_disabled():
    result = parse_verdict(json.dumps(_payload(groundedness=9)), 1, 5, False)
    assert result["groundedness"] is None


@pytest.mark.parametrize("field", ["correctness", "relevance", "groundedness"])
@pytest.mark.parametrize("value", [0, 6])
def test_score_out_of_range(field, value):
    with pytest.raises(ValueError, match=f"{field} must be an integer from 1 to 5"):
        parse_verdict(json.dumps(_payload(**{field: value})), 1, 5, True)


@pytest.mark.parametrize("value", [1, 5])
def test_score_bounds_accepted(value):
    text = json.dumps(_payload(correctness=value, relevance=value, groundedness=value))
    result = parse_verdict(text, 1, 5, True)
    assert result["correctness"] == result["relevance"] == result["groundedness"] == value


@pytest.mark.parametrize("value", [True, 3.0, "3"])
def test_score_wrong_type(value):
    with pytest.raises(ValueError, match="correctness must be an integer"):
        parse_verdict(json.dumps(_payload(correctness=value)), 1, 5, True)


@pytest.mark.parametrize(
    ("key", "message"),
    [
        ("correctness", "correctness must be an integer"),
        ("relevance", "relevance must be an integer"),
        ("refused", "refused must be true or false"),
    ],
)
def test_missing_keys(key, message):
    with pytest.raises(ValueError, match=message):
        parse_verdict(_drop(key), 1, 5, True)


def test_refused_not_bool():
    with pytest.raises(ValueError, match="refused must be true or false"):
        parse_verdict(json.dumps(_payload(refused="no")), 1, 5, True)


@pytest.mark.parametrize(
    "template",
    [
        "```json\n{body}\n```",
        "```\n{body}\n```",
        "Here is my verdict:\n```json\n{body}\n```\nThanks.",
    ],
)
def test_code_fenced_json(template):
    text = template.format(body=json.dumps(_payload(), indent=2))
    assert parse_verdict(text, 1, 5, True) == EXPECTED


def test_no_json_object():
    with pytest.raises(ValueError, match="no JSON object in judge response"):
        parse_verdict("I cannot judge this answer.", 1, 5, True)


def test_content_text_none():
    assert content_text(None) == ""


@pytest.mark.parametrize("value", ["hello", ""])
def test_content_text_plain_string(value):
    assert content_text(value) == value


def test_content_text_text_blocks():
    blocks = [{"type": "text", "text": "Hello, "}, {"type": "text", "text": "world"}]
    assert content_text(blocks) == "Hello, world"


def test_content_text_skips_non_text_blocks_and_non_dicts():
    blocks = [
        {"type": "text", "text": "Hello"},
        {"type": "image", "url": "x"},
        {"text": "no type"},
        "raw string",
        42,
        None,
        {"type": "text", "text": " world"},
    ]
    assert content_text(blocks) == "Hello world"


def test_content_text_text_block_edge_cases():
    assert content_text([{"type": "text"}]) == ""
    assert content_text([{"type": "text", "text": 7}]) == "7"


def test_content_text_empty_list():
    assert content_text([]) == ""


@pytest.mark.parametrize("value", [42, 3.5, {"type": "text", "text": "x"}])
def test_content_text_other_types(value):
    assert content_text(value) == str(value)
