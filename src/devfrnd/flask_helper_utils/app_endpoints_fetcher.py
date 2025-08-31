import signal
from pathlib import Path
import pandas as pd
from devfrnd.base_utils.logger_utils import RichLogger
from rich.progress import (
    Progress,
    SpinnerColumn,
    TextColumn,
    BarColumn,
    TimeElapsedColumn,
)
from rich.console import Console
import json
from typing import Optional, Any

# Logger
logger = RichLogger()
console = Console()

_stop = False  # graceful shutdown flag

def handle_sigint(_sig, _frame):
    """Handle Ctrl+C interrupt"""
    global _stop
    _stop = True
    logger.error("⚠ Migration interrupted by user. Closing connections...")

signal.signal(signal.SIGINT, handle_sigint)

def flask_endpoint_fetcher(
    app: Any,
    get: Optional[bool] = None,
    post: Optional[bool] = None,
    put: Optional[bool] = None,
    delete: Optional[bool] = None,
    patch: Optional[bool] = None,
    blueprint_name: str = "",
    output: str = "excel"
) -> None:
    """
    Fetches Flask endpoints based on allowed methods and blueprint name,
    and writes them to an Excel file in the user's root folder.
    Args:
        app: Flask application instance.
        get, post, put, delete, patch: Flags to include HTTP methods.
        blueprint_name: Filter endpoints by blueprint name (default: "").
        output: Output format ("excel", "console", "json").
    """
    logger.info("Starting flask_endpoint_fetcher...")
    try:
        allowed_methods = [
            method for method, flag in zip(
                ["GET", "POST", "PUT", "DELETE", "PATCH"], [get, post, put, delete, patch]
            ) if flag
        ]
        logger.info(f"Allowed methods: {allowed_methods}")
        endpoints = []

        rules = list(app.url_map.iter_rules())
        total_rules = len(rules)
        logger.info(f"Total rules found: {total_rules}")
        logger.info(f"Fetching endpoints for blueprint '{blueprint_name}' with allowed methods: {allowed_methods}")

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TimeElapsedColumn(),
            console=console,
            transient=True,
        ) as progress:
            task = progress.add_task("Processing endpoints...", total=total_rules)
            for idx, rule in enumerate(rules):
                logger.info(f"Processing rule {idx+1}/{total_rules}: {rule}")
                if _stop:
                    logger.warning("Process interrupted. Returning partial results.")
                    break
                # Filter by blueprint name if provided
                if blueprint_name:
                    if not (rule.endpoint.startswith(f"{blueprint_name}.") or (hasattr(rule, "blueprint") and rule.blueprint == blueprint_name)):
                        logger.info(f"Skipping rule '{rule.endpoint}' due to blueprint filter.")
                        progress.advance(task)
                        continue
                # Exclude static endpoint
                if rule.endpoint == "static":
                    logger.info("Skipping static endpoint.")
                    progress.advance(task)
                    continue
                # Filter allowed methods
                rule_methods = [m for m in rule.methods if m in allowed_methods]
                logger.info(f"Allowed methods for rule '{rule.endpoint}': {rule_methods}")
                for method in rule_methods:
                    endpoints.append({
                        "blueprint": rule.endpoint.split(".")[0] if "." in rule.endpoint else "root",
                        "endpoint": rule.endpoint,
                        "method": method,
                        "url": str(rule)
                    })
                progress.advance(task)

        logger.info(f"Total endpoints collected: {len(endpoints)}")
        if output == "excel":
            # Write to Excel in user's root folder, with sheets per blueprint
            user_dir = Path.cwd()
            excel_path = user_dir / "endpoints.xlsx"
            df = pd.DataFrame(endpoints)
            if not df.empty:
                logger.info(f"Writing endpoints to Excel at {excel_path}")
                with pd.ExcelWriter(excel_path, engine="xlsxwriter", mode="w") as writer:
                    for blueprint in df["blueprint"].unique():
                        sheet_df = df[df["blueprint"] == blueprint].copy()
                        sheet_df = sheet_df[["endpoint", "method", "url"]]
                        sheet_df = sheet_df.sort_values(by=["endpoint", "method"])
                        sheet_name = blueprint[:31] if blueprint else "root"
                        logger.info(f"Writing sheet: {sheet_name} with {len(sheet_df)} endpoints")
                        sheet_df.to_excel(writer, sheet_name=sheet_name, index=False)
                logger.success(f"Endpoints dumped to {excel_path}")
            else:
                logger.warning("No endpoints found to write to Excel.")
        elif output == "console":
            logger.info("Printing endpoints to console.")
            for blueprint in set(e["blueprint"] for e in endpoints):
                console.print(f"[bold cyan]Blueprint:[/bold cyan] {blueprint}")
                for item in [e for e in endpoints if e["blueprint"] == blueprint]:
                    console.print(f"  [green]Endpoint:[/green] {item['endpoint']}, [yellow]Method:[/yellow] {item['method']}, [magenta]URL:[/magenta] {item['url']}")
        elif output == "json":
            json_path = Path.cwd() / "endpoints.json"
            logger.info(f"Dumping endpoints to JSON at {json_path}")
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(endpoints, f, indent=2)
            logger.success(f"Endpoints dumped to {json_path}")
        else:
            logger.warning(f"Unknown output format: {output}")

    except Exception as e:
        logger.error(f"Error fetching endpoints: {e}")
