"""Entry point for the WhatsApp MCP server.

Redirects all logging to stderr (MCP protocol requires stdout to be clean for
JSON-RPC framing) and overrides the built-in ``print`` to also default to
stderr so that any accidental ``print()`` call does not corrupt the stdio pipe.

Usage::

    # Via installed console script (preferred):
    whatsapp-mcp

    # Via uv without installing:
    uv run --directory /path/to/whatsapp-mcp-python whatsapp-mcp

    # Directly (development):
    python -m whatsapp_mcp.main
"""

from __future__ import annotations

import builtins
import functools
import logging
import logging.config
import os
import platform
import sys

LOG_LEVELS = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


def get_logger_config(log_level: str = "INFO") -> dict:
    """Return logging config dictionary for dictConfig.

    Args:
        log_level: One of DEBUG, INFO, WARNING, ERROR, CRITICAL.
            Falls back to INFO if an unrecognised value is supplied.

    Returns:
        A dict suitable for passing directly to ``logging.config.dictConfig``.
    """
    if log_level not in LOG_LEVELS:
        log_level = "INFO"

    hostname = platform.node().split(".")[0]

    return {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "standard": {
                "format": (
                    "%(asctime)s %(levelname)s %(process)d "
                    "[%(name)s] %(filename)s:%(lineno)d - %(message)s"
                ),
            },
            "verbose": {
                "format": (
                    f"[hostname={hostname}] "
                    "%(asctime)s %(levelname)s %(process)d "
                    "[%(name)s] %(filename)s:%(lineno)d - %(message)s"
                ),
            },
        },
        "handlers": {
            "console": {
                "level": log_level,
                "class": "logging.StreamHandler",
                "formatter": "standard",
                "stream": "ext://sys.stderr",
            },
        },
        "root": {
            "level": log_level,
            "handlers": ["console"],
        },
        "loggers": {
            "whatsapp_mcp": {
                "level": log_level,
                "handlers": ["console"],
                "propagate": False,
            },
            "httpx": {
                "level": "WARNING",
                "handlers": ["console"],
                "propagate": False,
            },
            "httpcore": {
                "level": "WARNING",
                "handlers": ["console"],
                "propagate": False,
            },
        },
    }


def _configure_logging() -> None:
    """Route all log output to stderr, keeping stdout clean for MCP framing.

    The log level is read from the ``LOG_LEVEL`` environment variable
    (default: ``INFO``).  Unrecognised values silently revert to ``INFO``.
    """
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.config.dictConfig(get_logger_config(log_level))


def _patch_print() -> None:
    """Override built-in ``print`` so its default file is stderr.

    FastMCP writes JSON-RPC messages to stdout.  Any stray ``print()`` call
    that reaches stdout corrupts the MCP framing and causes the client to
    disconnect.  This patch makes ``print()`` safe by changing the default
    target to stderr while still allowing explicit ``print(..., file=sys.stdout)``
    if ever needed.
    """
    original_print = builtins.print

    @functools.wraps(original_print)
    def safe_print(*args, **kwargs):  # type: ignore[override]
        kwargs.setdefault("file", sys.stderr)
        original_print(*args, **kwargs)

    builtins.print = safe_print  # type: ignore[assignment]


def main() -> None:
    """Configure the process and start the FastMCP server.

    This function is the entry point registered in ``pyproject.toml`` under
    ``[project.scripts]``.  It:

    1. Configures logging to stderr.
    2. Patches ``print`` to default to stderr.
    3. Imports the :data:`~whatsapp_mcp.server.mcp` instance (which registers
       all tools and the telemetry middleware).
    4. Calls ``mcp.run()`` which blocks on the MCP transport loop until the
       parent process closes the pipe (stdio) or the server is stopped.

    Transport is selected via the ``MCP_TRANSPORT`` environment variable
    (default: ``stdio``).  Set it to ``sse`` or ``http`` (streamable-HTTP) to
    bind a network transport instead.  When using a network transport,
    ``MCP_HOST`` (default: ``0.0.0.0``) and ``MCP_PORT`` (default: ``3000``)
    control the bind address and port.
    """
    _configure_logging()
    _patch_print()

    logger = logging.getLogger(__name__)
    logger.info("WhatsApp MCP server initialising.")

    # Import here (after logging is configured) so any module-level log calls
    # during tool registration already have the correct handler installed.
    from whatsapp_mcp.server import mcp  # noqa: PLC0415

    transport = os.getenv("MCP_TRANSPORT", "stdio")
    if transport == "stdio":
        mcp.run()
    else:
        host = os.getenv("MCP_HOST", "0.0.0.0")
        port = int(os.getenv("MCP_PORT", "3000"))
        logger.info("Starting %s transport on %s:%d", transport, host, port)
        mcp.run(transport=transport, host=host, port=port)


if __name__ == "__main__":
    main()
