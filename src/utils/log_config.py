# src/utils/log_config.py
import logging
import logging.handlers
import sys
from pathlib import Path
from src.utils.config_loader import config


def setup_logger() -> logging.Logger:
    """Production logger setup."""
    project_root = Path(__file__).resolve().parents[2]
    log_dir = project_root / config.logging.dir
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / config.logging.file_name

    log_format = config.logging.format
    formatter = logging.Formatter(log_format, datefmt="%Y-%m-%d %H:%M:%S")

    file_handler = logging.handlers.RotatingFileHandler(
        filename=log_file,
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

    logger = logging.getLogger("FlightViewer")
    logger.setLevel(getattr(logging, config.logging.level.upper(), logging.INFO))
    logger.handlers.clear()
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    logger.propagate = False

    # handle uncaught exceptions
    def handle_exception(exc_type, exc_value, exc_traceback):
        if issubclass(exc_type, KeyboardInterrupt):
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
            return
        msg = f"{exc_type.__name__}: {exc_value}"
        logger.error(msg)
        print(f"\033[91m[Unhandled Exception] {msg}\033[0m", file=sys.stderr)

    sys.excepthook = handle_exception
    logger.info("Production logger configured successfully.")
    return logger


def setup_test_logger() -> logging.Logger:
    """
    Configure a separate logger for pytest and integration tests.
    Writes logs to logs/tests.log, separate from the main application logs.
    """
    project_root = Path(__file__).resolve().parents[2]
    log_dir = project_root / config.logging.dir
    log_dir.mkdir(exist_ok=True)

    test_log_file = log_dir / "tests.log"
    formatter = logging.Formatter(config.logging.format, datefmt="%Y-%m-%d %H:%M:%S")

    file_handler = logging.handlers.RotatingFileHandler(
        filename=test_log_file,
        mode="a",
        maxBytes=config.logging.max_bytes,
        backupCount=config.logging.backup_count,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.INFO)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)

    test_logger = logging.getLogger("FlightViewerTests")
    test_logger.setLevel(logging.INFO)
    test_logger.handlers.clear()
    test_logger.addHandler(file_handler)
    test_logger.addHandler(console_handler)
    test_logger.propagate = False

    test_logger.info("Test logger initialized successfully.")
    return test_logger


logger = setup_logger()
