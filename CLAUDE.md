# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands
- Run main script: `python turso_handler.py`
- Run with history update: `python turso_handler.py --hist`
- Performance testing: 
  - SQLite: `python perf-sqlite3.py`
  - LibSQL: `python perf-libsql.py`

## Code Style Guidelines
- **Imports**: Group standard library imports first, followed by third-party packages, then local modules
- **Formatting**: Use 4-space indentation
- **Types**: Use SQLAlchemy's type annotations with `Mapped[type]`
- **Naming**: snake_case for variables/functions, CamelCase for classes
- **Error Handling**: Use try/except blocks with specific error handling, log errors with logger
- **Logging**: Use the built-in logging module with both file and stream handlers
- **Database**: Use SQLAlchemy ORM for database operations
- **Comments**: Add docstrings for functions that aren't self-explanatory