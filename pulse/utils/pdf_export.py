from __future__ import annotations

from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle
from reportlab.pdfgen import canvas

def write_text_pdf(lines: list[str], output_path: str, title: str = "Document") -> None:
    page_width, page_height = letter
    margin_left = 36
    margin_top = 42
    margin_bottom = 36
    font_name = "Courier"
    font_size = 9
    line_height = 12
    max_chars_per_line = int((page_width - (margin_left * 2)) / pdfmetrics.stringWidth("W", font_name, font_size))

    wrapped_lines: list[str] = []
    for line in lines:
        value = str(line) if line is not None else ""
        if value == "":
            wrapped_lines.append("")
            continue
        while len(value) > max_chars_per_line:
            wrapped_lines.append(value[:max_chars_per_line])
            value = value[max_chars_per_line:]
        wrapped_lines.append(value)

    c = canvas.Canvas(output_path, pagesize=letter, pageCompression=0)
    c.setTitle(title)
    c.setAuthor("Pulse")
    c.setSubject("MS Cut List")

    y = page_height - margin_top
    c.setFont(font_name, font_size)

    for line in wrapped_lines:
        if y < margin_bottom:
            c.showPage()
            c.setFont(font_name, font_size)
            y = page_height - margin_top
        c.drawString(margin_left, y, line)
        y -= line_height

    c.save()


def write_table_pdf(
    headers: list[str],
    rows: list[list[str]],
    output_path: str,
    title: str = "Document",
    column_widths_mm: list[float] | None = None,
    row_color_group_col: int | None = None,
    row_color_palette: list[str] | None = None,
) -> None:
    doc = SimpleDocTemplate(
        output_path,
        pagesize=letter,
        leftMargin=10 * mm,
        rightMargin=10 * mm,
        topMargin=10 * mm,
        bottomMargin=10 * mm,
        title=title,
        author="Pulse",
        subject="MS Cut List",
    )

    styles = getSampleStyleSheet()
    title_style = styles["Heading4"]
    title_style.spaceAfter = 6
    body_style = styles["BodyText"]
    body_style.fontName = "Helvetica"
    body_style.fontSize = 8
    body_style.leading = 10
    header_style = styles["BodyText"]
    header_style.fontName = "Helvetica-Bold"
    header_style.fontSize = 8
    header_style.leading = 10

    table_data: list[list[Paragraph]] = [
        [Paragraph(str(cell), header_style) for cell in headers]
    ]
    for row in rows:
        table_data.append([Paragraph(str(cell or ""), body_style) for cell in row])

    col_widths = None
    if column_widths_mm and len(column_widths_mm) == len(headers):
        col_widths = [max(8.0, float(width_mm)) * mm for width_mm in column_widths_mm]

    table = Table(table_data, repeatRows=1, colWidths=col_widths)

    style_commands = [
        ("GRID", (0, 0), (-1, -1), 0.5, colors.black),
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#d9d9d9")),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("ALIGN", (0, 0), (0, -1), "CENTER"),
        ("ALIGN", (3, 1), (4, -1), "RIGHT"),
        ("LEFTPADDING", (0, 0), (-1, -1), 4),
        ("RIGHTPADDING", (0, 0), (-1, -1), 4),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
    ]

    if (
        row_color_group_col is not None
        and 0 <= row_color_group_col < len(headers)
        and rows
    ):
        default_palette = [
            "#f2f8ff",
            "#eefaf2",
            "#fff8ee",
            "#f7f1ff",
            "#edf7f7",
            "#fff0f3",
            "#f4f4ec",
            "#f0f6ff",
            "#f2fff7",
        ]
        palette = row_color_palette or default_palette
        safe_colors: list[colors.Color] = []
        for hex_color in palette:
            try:
                safe_colors.append(colors.HexColor(hex_color))
            except Exception:
                continue
        if safe_colors:
            group_color_map: dict[str, colors.Color] = {}
            next_color_idx = 0
            for table_row_index, row in enumerate(rows, start=1):
                key = str(row[row_color_group_col] if row_color_group_col < len(row) else "")
                if key not in group_color_map:
                    group_color_map[key] = safe_colors[next_color_idx % len(safe_colors)]
                    next_color_idx += 1
                style_commands.append(
                    ("BACKGROUND", (0, table_row_index), (-1, table_row_index), group_color_map[key])
                )

    table.setStyle(TableStyle(style_commands))

    story = [Paragraph(title, title_style), Spacer(1, 4), table]
    doc.build(story)
