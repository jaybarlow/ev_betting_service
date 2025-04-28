import sys
import logging
from typing import Any

from loguru import logger

from src.config.settings import settings


def sensitive_data_filter(record: dict[str, Any]) -> bool:
    """Filter function to mask sensitive data in log records."""
    sensitive_keys = ["key", "token", "password", "secret", "cookie"]

    def mask_value(value: Any) -> Any:
        if isinstance(value, str):
            for key in sensitive_keys:
                if (
                    key in record.get("name", "").lower()
                    or key in record.get("message", "").lower()
                ):
                    # Rudimentary masking, could be improved
                    if len(value) > 8:
                        return value[:4] + "****" + value[-4:]
                    else:
                        return "********"
            # Also check explicit keys in extra dict
            if record.get("extra"):
                for extra_key in record["extra"]:
                    if any(sk in extra_key.lower() for sk in sensitive_keys):
                        if (
                            isinstance(record["extra"][extra_key], str)
                            and len(record["extra"][extra_key]) > 8
                        ):
                            record["extra"][extra_key] = (
                                record["extra"][extra_key][:4]
                                + "****"
                                + record["extra"][extra_key][-4:]
                            )
                        else:
                            record["extra"][extra_key] = "********"
        elif isinstance(value, dict):
            return {k: mask_value(v) for k, v in value.items()}
        elif isinstance(value, list):
            return [mask_value(item) for item in value]
        return value

    # Apply masking to the message string itself
    # record["message"] = mask_value(record["message"])

    # Apply masking to the 'extra' dictionary
    if "extra" in record and isinstance(record["extra"], dict):
        record["extra"] = mask_value(record["extra"])

    # Apply masking specifically to known sensitive settings if they appear in the message
    # This is more targeted but requires knowing the setting names
    sensitive_settings = {
        settings.supabase_key: "********" if settings.supabase_key else None,
        settings.supabase_service_key: (
            "********" if settings.supabase_service_key else None
        ),
        settings.crabsports_cookie: "********" if settings.crabsports_cookie else None,
        settings.pinnacle_api_key: "********" if settings.pinnacle_api_key else None,
        # settings.tbd_book_api_key: "********" if settings.tbd_book_api_key else None,
    }
    for original, masked in sensitive_settings.items():
        if original and masked and original in record["message"]:
            record["message"] = record["message"].replace(original, masked)

    return True  # Keep the record after filtering/masking


def setup_logging() -> None:
    """Configures Loguru logger based on application settings."""
    logger.remove()  # Remove default handler

    # Basic console logging
    logger.add(
        sys.stderr,  # Output to standard error
        level=settings.log_level.upper(),
        format=(
            "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
            "<level>{level: <8}</level> | "
            "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
            "<level>{message}</level>"
        ),
        colorize=True,
        backtrace=True,  # Better tracebacks
        diagnose=True,  # More detailed error info
        filter=sensitive_data_filter,  # Apply the filter to mask sensitive data
    )

    # TODO: Add file logging sink if needed
    # logger.add(
    #     "logs/app_{time}.log",
    #     level="DEBUG", # Log everything to file
    #     rotation="10 MB", # Rotate log file when it reaches 10 MB
    #     retention="7 days", # Keep logs for 7 days
    #     compression="zip", # Compress rotated logs
    #     serialize=True, # Output logs in JSON format for structured logging
    #     filter=sensitive_data_filter
    # )

    logger.info(f"Logging initialized with level: {settings.log_level}")

    # Intercept standard logging messages
    class InterceptHandler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            # Get corresponding Loguru level if it exists
            try:
                level = logger.level(record.levelname).name
            except ValueError:
                level = record.levelno

            # Find caller from where originated the logged message
            frame, depth = logging.currentframe(), 2
            while frame.f_code.co_filename == logging.__file__:
                frame = frame.f_back
                depth += 1

            logger.opt(depth=depth, exception=record.exc_info).log(
                level, record.getMessage()
            )

    logging.basicConfig(handlers=[InterceptHandler()], level=0, force=True)
    logger.info("Standard logging intercepted.")
