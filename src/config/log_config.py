import logging
import logging.handlers
import sys
from pathlib import Path
from src.config.config_loader import config


def setup_logger() -> logging.Logger:
    """Configure and return the production application logger."""
    project_root = Path(__file__).resolve().parents[2]
    log_directory = project_root / config.logging.dir
    log_directory.mkdir(exist_ok=True)
    log_file_path = log_directory / config.logging.file_name

    log_format = config.logging.format
    formatter = logging.Formatter(log_format, datefmt="%Y-%m-%d %H:%M:%S")

    file_handler = logging.handlers.RotatingFileHandler(
        filename=log_file_path,
        mode="a",
        maxBytes=config.logging.max_bytes,
        backupCount=config.logging.backup_count,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(getattr(logging, config.logging.level.upper(), logging.INFO))

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.ERROR)

    app_logger = logging.getLogger("FlightViewer")
    app_logger.setLevel(getattr(logging, config.logging.level.upper(), logging.INFO))
    app_logger.handlers.clear()
    app_logger.addHandler(file_handler)
    app_logger.addHandler(console_handler)
    app_logger.propagate = False

    def handle_unhandled_exception(exc_type, exc_value, exc_traceback):
        if issubclass(exc_type, KeyboardInterrupt):
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
            return
        error_message = f"{exc_type.__name__}: {exc_value}"
        app_logger.error(error_message)
        print(f"\033[91m[Unhandled Exception] {error_message}\033[0m", file=sys.stderr)

    sys.excepthook = handle_unhandled_exception
    app_logger.info("Production logger configured successfully.")
    return app_logger


logger = setup_logger()