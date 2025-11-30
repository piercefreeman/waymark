#!/usr/bin/env -S uv run --script
# /// script
# dependencies = ["click"]
# ///
"""Parse coverage reports from Python and Rust test runs."""

import json
import xml.etree.ElementTree as ET
from pathlib import Path

import click


def parse_python_coverage(xml_path: Path) -> dict:
    """Parse Python coverage from Cobertura XML format."""
    tree = ET.parse(xml_path)
    root = tree.getroot()

    line_rate = float(root.get("line-rate", 0)) * 100
    branch_rate = float(root.get("branch-rate", 0)) * 100

    return {
        "line": round(line_rate, 1),
        "branch": round(branch_rate, 1),
    }


def parse_rust_coverage(lcov_path: Path) -> dict:
    """Parse Rust coverage from LCOV format."""
    lines_hit = 0
    lines_total = 0
    branches_hit = 0
    branches_total = 0

    with open(lcov_path) as f:
        for line in f:
            if line.startswith("LH:"):
                lines_hit += int(line.split(":")[1])
            elif line.startswith("LF:"):
                lines_total += int(line.split(":")[1])
            elif line.startswith("BRH:"):
                branches_hit += int(line.split(":")[1])
            elif line.startswith("BRF:"):
                branches_total += int(line.split(":")[1])

    line_pct = (lines_hit / lines_total * 100) if lines_total > 0 else 0
    branch_pct = (branches_hit / branches_total * 100) if branches_total > 0 else None

    return {
        "line": round(line_pct, 1),
        "branch": round(branch_pct, 1) if branch_pct is not None else None,
    }


def format_diff(current: float, baseline: float | None) -> str:
    """Format the difference between current and baseline coverage."""
    if baseline is None:
        return ""
    diff = current - baseline
    if abs(diff) < 0.1:
        return ""
    sign = "+" if diff > 0 else ""
    emoji = "green_circle" if diff > 0 else "red_circle"
    return f" :{emoji}: ({sign}{diff:.1f}%)"


def format_value(value: float | None) -> str:
    """Format a coverage value."""
    if value is None:
        return "N/A"
    return f"{value}%"


def generate_comment(
    pr_coverage: dict,
    main_coverage: dict | None,
    python_html_url: str | None,
    rust_html_url: str | None,
) -> str:
    """Generate a markdown comment with coverage comparison."""
    lines = ["## Coverage Report", ""]

    pr_python = pr_coverage.get("python", {})
    pr_rust = pr_coverage.get("rust", {})
    main_python = main_coverage.get("python", {}) if main_coverage else {}
    main_rust = main_coverage.get("rust", {}) if main_coverage else {}

    # Build the table
    lines.append("<table>")
    lines.append("<tr>")

    # Python column
    lines.append("<td>")
    lines.append("")
    lines.append("### Python Coverage")
    lines.append("")
    lines.append("| Metric | Coverage |")
    lines.append("|--------|----------|")

    py_line = pr_python.get("line", 0)
    py_branch = pr_python.get("branch", 0)
    main_py_line = main_python.get("line")
    main_py_branch = main_python.get("branch")

    py_line_diff = format_diff(py_line, main_py_line)
    py_branch_diff = format_diff(py_branch, main_py_branch)

    lines.append(f"| Lines | **{format_value(py_line)}**{py_line_diff} |")
    lines.append(f"| Branches | **{format_value(py_branch)}**{py_branch_diff} |")

    if python_html_url:
        lines.append("")
        lines.append(f"[Download HTML Report]({python_html_url})")

    lines.append("")
    lines.append("</td>")

    # Rust column
    lines.append("<td>")
    lines.append("")
    lines.append("### Rust Coverage")
    lines.append("")
    lines.append("| Metric | Coverage |")
    lines.append("|--------|----------|")

    rust_line = pr_rust.get("line", 0)
    rust_branch = pr_rust.get("branch")
    main_rust_line = main_rust.get("line")
    main_rust_branch = main_rust.get("branch")

    rust_line_diff = format_diff(rust_line, main_rust_line)
    rust_branch_diff = format_diff(rust_branch, main_rust_branch) if rust_branch is not None else ""

    lines.append(f"| Lines | **{format_value(rust_line)}**{rust_line_diff} |")
    lines.append(f"| Branches | **{format_value(rust_branch)}**{rust_branch_diff} |")

    if rust_html_url:
        lines.append("")
        lines.append(f"[Download HTML Report]({rust_html_url})")

    lines.append("")
    lines.append("</td>")

    lines.append("</tr>")
    lines.append("</table>")

    # Add baseline note if we have main coverage
    if main_coverage:
        lines.append("")
        lines.append("<sub>Compared to `main` branch</sub>")

    return "\n".join(lines)


@click.command()
@click.option("--python-xml", type=click.Path(path_type=Path), help="Path to Python coverage.xml")
@click.option("--rust-lcov", type=click.Path(path_type=Path), help="Path to Rust lcov.info")
@click.option(
    "--main-python-xml",
    type=click.Path(path_type=Path),
    help="Path to main branch Python coverage.xml",
)
@click.option(
    "--main-rust-lcov",
    type=click.Path(path_type=Path),
    help="Path to main branch Rust lcov.info",
)
@click.option("--python-html-url", type=str, help="URL to Python HTML coverage report")
@click.option("--rust-html-url", type=str, help="URL to Rust HTML coverage report")
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["json", "comment"]),
    default="json",
    help="Output format (json or comment)",
)
@click.option(
    "--output",
    type=click.Path(path_type=Path),
    help="Output file (optional, prints to stdout if not specified)",
)
def main(
    python_xml: Path | None,
    rust_lcov: Path | None,
    main_python_xml: Path | None,
    main_rust_lcov: Path | None,
    python_html_url: str | None,
    rust_html_url: str | None,
    output_format: str,
    output: Path | None,
) -> None:
    """Parse coverage reports from Python and Rust test runs."""
    pr_coverage: dict = {}
    main_coverage: dict | None = None

    if python_xml:
        pr_coverage["python"] = parse_python_coverage(python_xml)

    if rust_lcov:
        pr_coverage["rust"] = parse_rust_coverage(rust_lcov)

    if main_python_xml or main_rust_lcov:
        main_coverage = {}
        if main_python_xml:
            main_coverage["python"] = parse_python_coverage(main_python_xml)
        if main_rust_lcov:
            main_coverage["rust"] = parse_rust_coverage(main_rust_lcov)

    if output_format == "comment":
        output_str = generate_comment(pr_coverage, main_coverage, python_html_url, rust_html_url)
    else:
        result = {"pr": pr_coverage}
        if main_coverage:
            result["main"] = main_coverage
        output_str = json.dumps(result)

    if output:
        output.write_text(output_str)
    else:
        print(output_str)


if __name__ == "__main__":
    main()
