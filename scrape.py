import asyncio
import json
import os
import random
import re

import gspread
from curl_cffi.requests import AsyncSession

# ---------- Google Sheets ----------
def get_gspread_client():
    raw = os.environ["GOOGLE_SA_JSON"]
    return gspread.service_account_from_dict(json.loads(raw))


# ---------- Parsing ----------
def parse_next_data(html: str) -> dict | None:
    match = re.search(
        r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
        html, re.DOTALL
    )
    if not match:
        return None
    data = json.loads(match.group(1))
    props = data.get("props", {}).get("pageProps", {})
    agent = props.get("agentProfile", {})
    agency = agent.get("agencyDetail", {})
    if not agent:
        return None
    return {
        "agent_id": agent.get("id"),
        "first_name": agent.get("firstName"),
        "last_name": agent.get("lastName"),
        "position": agent.get("position"),
        "email": agent.get("email"),
        "mobile": agent.get("mobile"),
        "phone": agent.get("phone"),
        "average_sold_price": agent.get("averageSoldPrice"),
        "sold_listings": agent.get("numberOfSoldListings"),
        "agency_id": agency.get("id"),
        "agency_name": agency.get("name"),
        "agency_address": agency.get("address"),
        "agency_phone": agency.get("phone"),
        "agency_website": agency.get("website"),
    }


# ---------- Is it a DataDome block page ----------
def is_datadome_block(resp) -> bool:
    headers = {k.lower(): v for k, v in resp.headers.items()}
    if headers.get("x-datadome", "").lower() == "protected":
        return True
    if "datadome=" in headers.get("set-cookie", "").lower():
        return True
    body = resp.text or ""
    return "js.datadome.co/tags.js" in body or "captcha-delivery.com" in body


# ---------- Single Scrape (Layered Fallback) ----------
async def scrape_one(session: AsyncSession, url: str, sem: asyncio.Semaphore):
    async with sem:
        await asyncio.sleep(random.uniform(1.5, 4.0))  # Basic rate control
        try:
            # Layer 1: curl_cffi (fast, works for most pages without active challenges)
            resp = await session.get(url, impersonate="chrome124", timeout=30)
            if resp.status_code == 200 and not is_datadome_block(resp):
                parsed = parse_next_data(resp.text)
                if parsed:
                    return {"url": url, **parsed}

            # Layer 2: StealthKit fallback (solves DataDome JS challenge)
            print(f"[FALLBACK] {url} -> StealthKit")
            from stealthkit import StealthSession
            sr = StealthSession(impersonate="chrome124")
            sr.solve_challenge(url)  # Solve JS challenge, obtain datadome cookie
            resp2 = sr.get(url)
            if resp2 and resp2.status_code == 200:
                parsed = parse_next_data(resp2.text)
                if parsed:
                    return {"url": url, **parsed}
            return {"url": url, "error": "blocked_after_fallback"}

        except Exception as e:
            return {"url": url, "error": str(e)}


# ---------- Batch Concurrent Scrape ----------
async def scrape_all(urls: list[str], concurrency: int = 3):
    sem = asyncio.Semaphore(concurrency)  # Start with 3, view.com.au is sensitive to concurrency
    async with AsyncSession() as session:
        tasks = [scrape_one(session, u, sem) for u in urls]
        return await asyncio.gather(*tasks)


# ---------- Main Flow ----------
def main():
    gc = get_gspread_client()
    sh = gc.open_by_key(os.environ["SHEET_ID"])
    ws = sh.worksheet("Sheet1")

    col_a = ws.col_values(1)
    urls = [u.strip() for u in col_a[1:] if u and u.strip()]
    if not urls:
        print("No URLs in A2:A")
        return

    print(f"Scraping {len(urls)} URLs with concurrency=3 ...")
    results = asyncio.run(scrape_all(urls, concurrency=3))

    headers = [
        "agent_id", "first_name", "last_name", "position", "email",
        "mobile", "phone", "average_sold_price", "sold_listings",
        "agency_id", "agency_name", "agency_address", "agency_phone",
        "agency_website", "error"
    ]
    ws.update("B1:P1", [headers])

    rows = []
    for r in results:
        rows.append([r.get(h, "") for h in headers])
    ws.update(f"B2:P{1 + len(rows)}", rows)
    print("Done.")


if __name__ == "__main__":
    main()
