"""
LeadHarvest API — startup script for Windows.

Uvicorn on Windows defaults to SelectorEventLoop, which does NOT support
subprocess spawning (asyncio.create_subprocess_exec raises NotImplementedError).
Playwright must spawn a Chromium subprocess, so it requires ProactorEventLoop.

This script sets WindowsProactorEventLoopPolicy BEFORE uvicorn starts its
event loop, which fixes the silent 'NotImplementedError' scrape failure.

Run with:
    venv/Scripts/python.exe run_api.py
"""

import asyncio
import sys

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

import uvicorn

if __name__ == "__main__":
    uvicorn.run("api:app", host="0.0.0.0", port=8000)
