# src/utils/log_config.py
import logging
import logging.handlers
import sys
from pathlib import Path
from src.utils.config_loader import config


def setup_logger() -> logging.Logger:
    """
    Production logger setup.

    Features:
    - Writes logs to the /logs folder at the project root.
    - Rotating file handler with configurable size and backups.
    - Console handler for ERROR and above.
    - Logs concise error messages (no full traceback).
    - Prints unhandled exceptions to the console in red for visibility.
    """

    #  Always resolve log directory relative to project root
    project_root = Path(__file__).resolve().parents[2]
    log_dir = project_root / config.logging.dir
    log_dir.mkdir(exist_ok=True)

    log_file = log_dir / config.logging.file_name

    # Log format
    log_format = config.logging.format
    formatter = logging.Formatter(log_format, datefmt="%Y-%m-%d %H:%M:%S")

    #  File handler with rotation
    file_handler = logging.handlers.RotatingFileHandler(
        filename=log_file,
        mode="a",
        maxBytes=config.logging.max_bytes,
        backupCount=config.logging.backup_count,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(getattr(logging, config.logging.level.upper(), logging.INFO))

    # === 💬 Console handler (ERROR and above only) ===
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.ERROR)

    #  Main logger configuration
    logger = logging.getLogger("FlightViewer")
    logger.setLevel(getattr(logging, config.logging.level.upper(), logging.INFO))
    logger.handlers.clear()
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    logger.propagate = False

    #  Handle uncaught exceptions
    def handle_exception(exc_type, exc_value, exc_traceback):
        # Ignore keyboard interrupts (Ctrl+C)
        if issubclass(exc_type, KeyboardInterrupt):
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
            return

        # Create a short message
        msg = f"{exc_type.__name__}: {exc_value}"

        # Log concise error (no traceback)
        logger.error(msg)

        # Print visibly in red to the console (for development)
        print(f"\033[91m[Unhandled Exception] {msg}\033[0m", file=sys.stderr)

    # Replace default exception hook
    sys.excepthook = handle_exception

    logger.info("Production logger configured successfully.")
    return logger


logger = setup_logger()
