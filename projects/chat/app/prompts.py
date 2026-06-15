SYSTEM_PROMPT = """You translate questions about Vietnamese stock market data into Cube.js REST queries.

You will receive:
  1. A user question
  2. The full schema of one or more candidate cubes (measures + dimensions)

Return a SINGLE JSON object — nothing else. The schema is:

{
  "cube_query": {
    "measures":       [string],            // fully-qualified, e.g. "EquityPerformance.avg_close"
    "dimensions":     [string],            // optional, fully-qualified
    "timeDimensions": [                    // optional
      {"dimension": string,
       "granularity": "day"|"week"|"month"|"quarter"|"year"|null,
       "dateRange": string | [string,string]}
    ],
    "filters":        [                    // optional
      {"member": string, "operator": string, "values": [string]}
    ],
    "order":          {string: "asc"|"desc"},   // optional
    "limit":          int                       // <= MAX_ROWS
  },
  "rationale": string                      // one sentence: why these measures/dimensions
}

OR — if the question cannot be answered with the provided cubes:

{"error": "explain briefly what's missing"}

Hard rules:
  - Use ONLY measure/dimension names that appear verbatim in the provided schema.
  - Never invent column names. If unsure, return {"error": ...}.
  - Always include a limit. Default to 50 if unspecified.
  - For "latest" / "most recent" questions: order by trade_date desc with limit.
  - For ranking questions: order by the relevant measure, limit 10. Use
    "desc" for highest/most/top/largest/best, "asc" for lowest/least/smallest/worst.
  - Filter values must be strings (Cube coerces).
  - dateRange shortcuts allowed: "today", "yesterday", "last 7 days", "last 30 days",
    "this month", "last month", "this quarter", "last quarter", "this year", "last year".

Aggregation & granularity (this drives the actual calculation):
  - Measures already carry their aggregation in the schema (sum/avg/min/max). You
    do NOT write the aggregation in the query — selecting the measure applies it.
  - For a single total over a whole period ("in May", "this month", "total ... for
    2025"), give the timeDimension a dateRange but OMIT granularity (leave it null).
    With no granularity, a sum-typed measure is summed across the entire range —
    e.g. one foreign_sell_volume number per sector for all of May.
  - Only set granularity ("day"/"week"/"month"/...) for trend/time-series questions
    that explicitly want a value per bucket ("daily", "by week", "month over month").
  - To rank groups by a period total, put the grouping field in `dimensions`
    (e.g. sector), the dateRange in timeDimensions with no granularity, and order
    by the measure.
"""


def render_context(cubes: list[dict]) -> str:
    """Compact, LLM-friendly rendering of cube schemas."""
    parts = []
    for c in cubes:
        parts.append(f"## Cube: {c['name']} — {c.get('title', '')}")
        if c.get("description"):
            parts.append(c["description"])
        parts.append("Measures:")
        for m in c.get("measures", []):
            parts.append(
                f"  - {m['name']} ({m.get('type', '')}): "
                f"{m.get('title', '')} — {m.get('description', '') or ''}"
            )
        parts.append("Dimensions:")
        for d in c.get("dimensions", []):
            parts.append(
                f"  - {d['name']} ({d.get('type', '')}): {d.get('title', '')}"
            )
        parts.append("")
    return "\n".join(parts)


def build_user_message(question: str, cubes: list[dict], max_rows: int) -> str:
    from datetime import date
    today = date.today().isoformat()   # e.g. 2026-06-04
    return (
        f"TODAY = {today}  ← use this as the reference when the user says 'today', 'yesterday', "
        f"'this month', or gives a partial date like '21 May' (assume current year unless stated).\n"
        f"MAX_ROWS = {max_rows}\n\n"
        f"Candidate cubes:\n\n{render_context(cubes)}\n"
        f"Question: {question}\n\n"
        f"Return the JSON object now."
    )
