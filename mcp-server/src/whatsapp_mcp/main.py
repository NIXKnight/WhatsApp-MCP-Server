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
import sys


def _configure_logging() -> None:
    """Route all log output to stderr, keeping stdout clean for MCP framing."""
    logging.basicConfig(
        stream=sys.stderr,
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )


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
    """Configure the process and start the FastMCP server on stdio transport.

    This function is the entry point registered in ``pyproject.toml`` under
    ``[project.scripts]``.  It:

    1. Configures logging to stderr.
    2. Patches ``print`` to default to stderr.
    3. Imports the :data:`~whatsapp_mcp.server.mcp` instance (which registers
       all twelve tools).
    4. Calls ``mcp.run()`` which blocks on the stdio MCP transport loop until
       the parent process closes the pipe.
    """
    _configure_logging()
    _patch_print()

    logger = logging.getLogger(__name__)
    logger.info("WhatsApp MCP server initialising.")

    # Import here (after logging is configured) so any module-level log calls
    # during tool registration already have the correct handler installed.
    from whatsapp_mcp.server import mcp  # noqa: PLC0415

    mcp.run()


if __name__ == "__main__":
    main()
