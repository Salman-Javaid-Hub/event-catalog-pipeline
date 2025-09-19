# fetcher.py
import asyncio
import time
import random
from typing import Optional
import requests
from playwright.async_api import async_playwright

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
]

DEFAULT_TIMEOUT_MS = 30000

async def _fetch_with_playwright(url: str) -> Optional[str]:
    ua = random.choice(USER_AGENTS)
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent=ua,
            viewport={"width": 1366, "height": 850},
            java_script_enabled=True,
        )
        # Light stealth
        await context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
        )

        # Block heavy resources for speed
        async def route_block(route):
            if route.request.resource_type in ("image", "media", "font"):
                await route.abort()
            else:
                await route.continue_()
        await context.route("**/*", route_block)

        page = await context.new_page()
        try:
            await page.goto(url, timeout=DEFAULT_TIMEOUT_MS, wait_until="domcontentloaded")
            try:
                await page.wait_for_load_state("networkidle", timeout=7000)
            except Exception:
                pass
            html = await page.content()
            return html
        finally:
            await context.close()
            await browser.close()

def _fetch_with_requests(url: str) -> Optional[str]:
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
    }
    try:
        resp = requests.get(url, headers=headers, timeout=15)
        if resp.status_code == 200 and resp.text:
            return resp.text
    except Exception:
        pass
    return None

def fetch_page(url: str, tries: int = 3, requests_fallback: bool = True) -> Optional[str]:
    for attempt in range(1, tries + 1):
        try:
            html = asyncio.run(_fetch_with_playwright(url))
            if html:
                return html
        except Exception:
            if attempt == tries:
                break
        time.sleep(1.5 * attempt)

    if requests_fallback:
        return _fetch_with_requests(url)
    return None
