"""LLM call: question + cube schemas -> Cube query JSON."""
from __future__ import annotations

import json
import re
from typing import Any

from anthropic import Anthropic

from .config import settings
from .prompts import SYSTEM_PROMPT, build_user_message


_client = Anthropic(api_key=settings.anthropic_api_key)


_FENCE_RE = re.compile(r"```(?:json)?\s*(.*?)\s*```", re.DOTALL)


def _extract_json(text: str) -> dict[str, Any]:
    text = text.strip()
    if (m := _FENCE_RE.search(text)):
        text = m.group(1).strip()
    return json.loads(text)


def generate(question: str, cubes: list[dict[str, Any]]) -> tuple[dict[str, Any], str]:
    """Return (parsed_response, model_used)."""
    if not cubes:
        return ({"error": "No relevant cubes were retrieved for this question."},
                settings.llm_generator_model)

    user = build_user_message(question, cubes, settings.cube_max_rows)
    msg = _client.messages.create(
        model=settings.llm_generator_model,
        max_tokens=1024,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user}],
    )
    text = "".join(b.text for b in msg.content if b.type == "text")
    try:
        return _extract_json(text), settings.llm_generator_model
    except json.JSONDecodeError as e:
        return ({"error": f"LLM returned non-JSON: {e}", "raw": text[:500]},
                settings.llm_generator_model)
