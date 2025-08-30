from pathlib import Path
from loguru import logger
from rich.console import Console

class RichLogger:
    def __init__(self, log_dir=None):
        self.console = Console()

        # Default log path: ~/.devfrnd/logs/app.log
        if log_dir is None:
            log_dir = Path.home() / ".devfrnd" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)

        self.log_file = log_dir / "app.log"

        # Configure loguru
        logger.remove()  # remove default stderr logger
        logger.add(
            self.log_file,
            rotation="10 MB",
            retention="7 days",
            compression="zip",
            enqueue=True,
            level="INFO",
            format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}"
        )

        self.logger = logger

    def _log(self, level, color, message):
        getattr(self.logger, level)(message)
        self.console.print(f"\n[bold {color}]{message}[/bold {color}]\n")

    def info(self, message): self._log("info", "blue", message)
    def success(self, message): self._log("info", "green", message)
    def warning(self, message): self._log("warning", "yellow", message)
    def error(self, message): self._log("error", "red", message)

    def log_to_file(self, level, message):
        getattr(self.logger, level)(message)
