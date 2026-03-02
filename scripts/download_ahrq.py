"""One-time download of AHRQ Compendium files using Playwright.

AHRQ uses AWS WAF bot protection. This script uses a real browser
to bypass the WAF and download the CSV files.

Usage:
    pip install playwright
    playwright install chromium
    python scripts/download_ahrq.py
"""

import asyncio
import sys
from pathlib import Path

CACHE_DIR = Path.home() / ".healthcare-data-mcp" / "cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

DOWNLOADS = [
    (
        "https://www.ahrq.gov/sites/default/files/wysiwyg/chsp/compendium/chsp-system-2023.csv",
        CACHE_DIR / "ahrq_system_2023.csv",
    ),
    (
        "https://www.ahrq.gov/sites/default/files/wysiwyg/chsp/compendium/chsp-hospital-linkage-2023.csv",
        CACHE_DIR / "ahrq_hospital_linkage_2023.csv",
    ),
]


async def download_with_playwright():
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        print("Install playwright: pip install playwright && playwright install chromium")
        sys.exit(1)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        for url, dest in DOWNLOADS:
            if dest.exists():
                print(f"Already cached: {dest}")
                continue

            print(f"Downloading: {url}")
            resp = await page.goto(url, wait_until="networkidle", timeout=60000)
            if resp and resp.ok:
                body = await resp.body()
                dest.write_bytes(body)
                print(f"Saved: {dest} ({len(body)} bytes)")
            else:
                print(f"FAILED: {url} — status {resp.status if resp else 'None'}")

        await browser.close()


if __name__ == "__main__":
    asyncio.run(download_with_playwright())
