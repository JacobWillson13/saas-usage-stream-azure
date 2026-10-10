#!/usr/bin/env python3
"""Draw docs/img/erd.png from the live schema: app tables, their columns, and FKs.

Reads the catalog through the stream_rw role, so `make db-init` must have run.
"""

from __future__ import annotations

import math
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

from saas_stream.db import stream_pool

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "docs" / "img" / "erd.png"
FONTS = Path("/usr/share/fonts/truetype/dejavu")
INGEST = ["event_ts", "source_event_ts", "ingested_at", "ingest_id", "is_backfill", "is_demo",
          "is_injected"]  # fmt: skip
# Top-left corners, placed so no FK line crosses a box: user-side tables on the left
# and top, usage and plan tables on the right and bottom, billing on the far left.
POSITIONS = {
    "accounts": (980, 733), "users": (980, 130), "feature_events": (420, 160),
    "license_events": (420, 470), "invoices": (60, 700), "payments": (60, 1230),
    "devices": (1560, 150), "sessions": (1560, 470), "usage_daily": (1920, 640),
    "feature_usage_daily": (1560, 1250), "plan_changes": (980, 1350),
}  # fmt: skip
WIDTH, HEIGHT = 2300, 1720
BOX_W, ROW_H, HEAD_H, PAD = 340, 22, 32, 10
COLORS = {"bg": "#ffffff", "box": "#f8fafc", "border": "#334155", "head": "#1e3a8a",
          "head_text": "#ffffff", "text": "#0f172a", "muted": "#64748b", "key": "#b45309",
          "edge": "#64748b"}  # fmt: skip


def font(name: str, size: int) -> ImageFont.FreeTypeFont:
    return ImageFont.truetype(str(FONTS / name), size)


def load_schema() -> tuple[dict, dict, list[tuple[str, str, str, str]]]:
    pool = stream_pool(max_size=1)
    try:
        with pool.connection() as conn:
            columns: dict[str, list[tuple[str, str]]] = {}
            for table, column, data_type in conn.execute(
                "SELECT table_name, column_name, data_type FROM information_schema.columns "
                "WHERE table_schema = 'app' ORDER BY table_name, ordinal_position"
            ).fetchall():
                columns.setdefault(table, []).append((column, data_type))
            primary = {}
            for table, column in conn.execute(
                "SELECT c.conrelid::regclass::text, a.attname FROM pg_constraint c "
                "JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ANY (c.conkey) "
                "WHERE c.contype = 'p' AND c.connamespace = 'app'::regnamespace"
            ).fetchall():
                primary.setdefault(table.removeprefix("app."), set()).add(column)
            foreign = [
                (child.removeprefix("app."), column, parent.removeprefix("app."), ref)
                for child, column, parent, ref in conn.execute(
                    "SELECT c.conrelid::regclass::text, a.attname, c.confrelid::regclass::text, "
                    "af.attname FROM pg_constraint c "
                    "JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = c.conkey[1] "
                    "JOIN pg_attribute af "
                    "ON af.attrelid = c.confrelid AND af.attnum = c.confkey[1] "
                    "WHERE c.contype = 'f' AND c.connamespace = 'app'::regnamespace"
                ).fetchall()
            ]
    finally:
        pool.close()
    return columns, primary, foreign


def short_type(data_type: str) -> str:
    return {"timestamp with time zone": "timestamptz", "character varying": "varchar"}.get(
        data_type, data_type
    )


def main() -> None:
    columns, primary, foreign = load_schema()
    fk_columns = {(child, column) for child, column, _, _ in foreign}
    body = {
        table: [c for c in cols if c[0] not in INGEST] for table, cols in columns.items()
    }
    heights = {t: HEAD_H + PAD + ROW_H * (len(body[t]) + 1) + PAD for t in columns}

    positions = dict(POSITIONS)
    missing = set(columns) - set(positions)
    if missing:
        raise SystemExit(f"no position for tables: {sorted(missing)}")

    def row_y(table: str, column: str) -> float:
        names = [c for c, _ in body[table]]
        index = names.index(column) if column in names else 0
        return positions[table][1] + HEAD_H + PAD + ROW_H * index + ROW_H / 2

    def stacked(a: str, b: str) -> bool:
        return abs(positions[a][0] - positions[b][0]) < BOX_W

    def edge(child: str, column: str, parent: str, ref: str) -> tuple[tuple, tuple]:
        (cx, cy), (px, py) = positions[child], positions[parent]
        if stacked(child, parent):  # one above the other: bottom edge to top edge
            mid = cx + BOX_W / 2
            if cy > py:
                return (mid, cy), (mid, py + heights[parent])
            return (mid, cy + heights[child]), (mid, py)
        if cx > px:  # child to the right of the parent
            return (cx, row_y(child, column)), (px + BOX_W, row_y(parent, ref))
        return (cx + BOX_W, row_y(child, column)), (px, row_y(parent, ref))

    image = Image.new("RGB", (WIDTH, HEIGHT), COLORS["bg"])
    draw = ImageDraw.Draw(image)
    regular, bold = font("DejaVuSans.ttf", 15), font("DejaVuSans-Bold.ttf", 17)
    small, title = font("DejaVuSans.ttf", 13), font("DejaVuSans-Bold.ttf", 26)

    for child, column, parent, ref in foreign:
        start, end = edge(child, column, parent, ref)
        draw.line([start, end], fill=COLORS["edge"], width=2)
        angle = math.atan2(end[1] - start[1], end[0] - start[0])
        tip = [end] + [
            (end[0] - 12 * math.cos(angle + turn), end[1] - 12 * math.sin(angle + turn))
            for turn in (-0.4, 0.4)
        ]
        draw.polygon(tip, fill=COLORS["edge"])

    for table, (x, y) in positions.items():
        h = heights[table]
        draw.rounded_rectangle([x, y, x + BOX_W, y + h], radius=8, fill=COLORS["box"],
                               outline=COLORS["border"], width=2)  # fmt: skip
        draw.rounded_rectangle([x, y, x + BOX_W, y + HEAD_H], radius=8, fill=COLORS["head"])
        draw.rectangle([x, y + HEAD_H - 8, x + BOX_W, y + HEAD_H], fill=COLORS["head"])
        draw.text((x + PAD, y + 6), f"app.{table}", font=bold, fill=COLORS["head_text"])
        for i, (column, data_type) in enumerate(body[table]):
            ry = y + HEAD_H + PAD + ROW_H * i
            tags = []
            if column in primary.get(table, set()):
                tags.append("PK")
            if (table, column) in fk_columns:
                tags.append("FK")
            if tags:
                draw.text((x + PAD, ry), " ".join(tags), font=small, fill=COLORS["key"])
            draw.text((x + PAD + 46, ry), column, font=regular, fill=COLORS["text"])
            type_text = short_type(data_type)
            tw = draw.textlength(type_text, font=small)
            draw.text((x + BOX_W - PAD - tw, ry + 1), type_text, font=small, fill=COLORS["muted"])
        ry = y + HEAD_H + PAD + ROW_H * len(body[table])
        draw.text((x + PAD + 46, ry), "+ ingest columns", font=small, fill=COLORS["muted"])

    draw.text((40, 30), "saas_stream: app schema", font=title, fill=COLORS["text"])
    draw.text(
        (40, 66),
        "Arrows point from a foreign key to the referenced key. Every table also has the "
        "ingest columns: " + ", ".join(INGEST) + ".",
        font=regular, fill=COLORS["muted"],
    )  # fmt: skip
    OUT.parent.mkdir(parents=True, exist_ok=True)
    image.save(OUT, optimize=True)
    print(f"wrote {OUT} ({len(columns)} tables, {len(foreign)} foreign keys)")


if __name__ == "__main__":
    main()
