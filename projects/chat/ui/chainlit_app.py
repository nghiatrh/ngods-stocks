"""Chainlit co-pilot UI: question -> retrieved context -> Cube query -> table.

Co-pilot mode: we never auto-write prose. The user sees the exact Cube query
that produced the numbers, plus thumbs-up/down feeding back into log.feedback.
"""
from __future__ import annotations

import json
import os

import chainlit as cl
import httpx

API = os.environ.get("CHAT_API_URL", "http://chat-api:8001")


def _format_table(rows: list[dict]) -> str:
    if not rows:
        return "_(no rows)_"
    cols = list(rows[0].keys())
    head = "| " + " | ".join(cols) + " |"
    sep = "| " + " | ".join("---" for _ in cols) + " |"
    body = ["| " + " | ".join(str(r.get(c, "")) for c in cols) + " |" for r in rows[:50]]
    more = f"\n\n_…showing 50 of {len(rows)} rows_" if len(rows) > 50 else ""
    return "\n".join([head, sep, *body]) + more


async def _send_feedback(query_id: int, rating: int) -> None:
    async with httpx.AsyncClient(timeout=10) as c:
        await c.post(f"{API}/feedback", json={"query_id": query_id, "rating": rating})


@cl.action_callback("good")
async def on_good(action: cl.Action) -> None:
    await _send_feedback(int(action.payload["query_id"]), 1)
    await cl.Message(content="thanks — logged as a good answer.").send()


@cl.action_callback("bad")
async def on_bad(action: cl.Action) -> None:
    await _send_feedback(int(action.payload["query_id"]), -1)
    await cl.Message(content="thanks — logged as wrong. it'll surface in eval.").send()


@cl.on_chat_start
async def start() -> None:
    await cl.Message(
        content=(
            "Ask anything about the Vietnamese equities data covered by your cubes "
            "(equity performance, foreign flow, trade flow, sector performance, "
            "market summary).\n\n"
            "I show you the Cube query I generated so you can verify the numbers."
        )
    ).send()


@cl.on_message
async def on_message(msg: cl.Message) -> None:
    async with httpx.AsyncClient(timeout=60) as c:
        r = await c.post(f"{API}/ask", json={"question": msg.content})
    if r.status_code != 200:
        await cl.Message(content=f"error: {r.status_code} — {r.text}").send()
        return
    data = r.json()

    if data.get("error"):
        await cl.Message(content=f"**Cannot answer:** {data['error']}").send()
        return

    blocks: list[str] = []
    if data.get("rationale"):
        blocks.append(f"**Approach:** {data['rationale']}")
    blocks.append(
        "**Cube query:**\n```json\n"
        + json.dumps(data["cube_query"], indent=2)
        + "\n```"
    )
    rows = (data.get("result") or {}).get("rows", [])
    if rows:
        blocks.append("**Result:**\n" + _format_table(rows))
    else:
        # Build a human-readable date hint from timeDimensions
        td = (data["cube_query"].get("timeDimensions") or [{}])[0]
        date_range = td.get("dateRange")
        if isinstance(date_range, list):
            date_str = date_range[0] if date_range[0] == date_range[1] else f"{date_range[0]} → {date_range[1]}"
        elif isinstance(date_range, str):
            date_str = date_range
        else:
            date_str = ""

        hint = f" for **{date_str}**" if date_str else ""
        blocks.append(
            f"⚠️ **No data found{hint}.** "
            "The query executed successfully but returned 0 rows.\n\n"
            "Possible reasons:\n"
            "- The date is a **public holiday or non-trading day** (e.g. weekends, Tết, Labour Day)\n"
            "- **Data hasn't been loaded** for that period yet — check Airflow for pipeline status\n\n"
            "_Try asking for the latest available data instead._"
        )
    blocks.append(f"_latency: {data['latency_ms']} ms_")

    await cl.Message(
        content="\n\n".join(blocks),
        actions=[
            cl.Action(name="good", label="✓ correct", payload={"query_id": data["query_id"]}),
            cl.Action(name="bad", label="✗ wrong", payload={"query_id": data["query_id"]}),
        ],
    ).send()
