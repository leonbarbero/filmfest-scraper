# src/fetcher.py

import asyncio
import aiohttp  # type: ignore
from typing import Tuple

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager


async def fetch_page(
    url: str,
    retries: int = 3,
    timeout: int = 10,
    backoff_factor: float = 0.5,
    use_selenium_on_fail: bool = True
) -> Tuple[int, str]:
    """
    Async fetch a URL with retry + optional Selenium fallback on failure.
    Returns (status_code, response_text).
    """
    for attempt in range(1, retries + 1):
        try:
            timeout_cfg = aiohttp.ClientTimeout(total=timeout)
            async with aiohttp.ClientSession(timeout=timeout_cfg) as session:
                async with session.get(url) as resp:
                    text = await resp.text()
                    if resp.status == 403 and use_selenium_on_fail:
                        raise Exception("403 detected")
                    return resp.status, text
        except Exception as e:
            if attempt == retries:
                if use_selenium_on_fail:
                    print(f"  🔁 Switching to Selenium for {url}")
                    return fetch_with_selenium(url)
                raise
            await asyncio.sleep(backoff_factor * attempt)

def fetch_with_selenium(url: str) -> Tuple[int, str]:
    """
    Fallback fetch using Selenium (headless Chrome).
    """
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--window-size=1920,1080')
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    try:
        driver.get(url)
        html = driver.page_source
        return 200, html
    finally:
        driver.quit()

def smoke_test():
    """
    Verify that fetch_page can be awaited once.
    """
    print("  ▶ Running fetcher.smoke_test()…")
    status, text = asyncio.run(fetch_page("https://httpbin.org/get", use_selenium_on_fail=False))
    assert status == 200, f"Expected 200 but got {status}"
    assert 'url' in text, "Did not see expected content in response"
    print("  ✓ Fetcher module smoke test passed")
