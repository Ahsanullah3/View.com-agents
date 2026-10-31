import asyncio
import json
import os
import random
import re
import time

import gspread
import requests
from curl_cffi.requests import AsyncSession

# Configuration
MAX_URLS_PER_RUN = 90
CONCURRENCY = 3
BATCH_PAUSE = 8
HEADERS = [
    "agent_id", "first_name", "last_name", "position", "email",
    "mobile", "phone", "average_sold_price", "sold_listings",
    "agency_id", "agency_name", "agency_address", "agency_phone",
    "agency_website", "error"
]

def get_gspread_client():
    return gspread.service_account_from_dict(json.loads(os.environ["GOOGLE_SA_JSON"]))

def parse_next_data(html: str):
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

def is_datadome_block(resp) -> bool:
    headers = {k.lower(): v for k, v in resp.headers.items()}
    if headers.get("x-datadome", "").lower() == "protected":
        return True
    if "datadome=" in headers.get("set-cookie", "").lower():
        return True
    body = resp.text or ""
    return "js.datadome.co/tags.js" in body or "captcha-delivery.com" in body

async def scrape_one(session: AsyncSession, url: str, sem: asyncio.Semaphore):
    async with sem:
        await asyncio.sleep(random.uniform(1.5, 4.0))
        try:
            # Fast path
            resp = await session.get(url, impersonate="chrome124", timeout=30)
            if resp.status_code == 200 and not is_datadome_block(resp):
                parsed = parse_next_data(resp.text)
                if parsed:
                    return {"url": url, **parsed}
            
            # Fallback to StealthKit
            from stealthkit import StealthSession
            sr = StealthSession(impersonate="chrome124")
            sr.solve_challenge(url)
            resp2 = sr.get(url)
            if resp2 and resp2.status_code == 200:
                parsed = parse_next_data(resp2.text)
                if parsed:
                    return {"url": url, **parsed}
            return {"url": url, "error": "blocked_after_fallback"}
        except Exception as e:
            return {"url": url, "error": str(e)}

async def scrape_batch(session: AsyncSession, urls: list[str]):
    sem = asyncio.Semaphore(CONCURRENCY)
    tasks = [scrape_one(session, u, sem) for u in urls]
    return await asyncio.gather(*tasks)

def trigger_next_run(start_row: int):
    token = os.environ["GH_PAT"]
    repo = os.environ["GITHUB_REPOSITORY"]
    workflow_file = "scrape.yml"
    url = f"https://api.github.com/repos/{repo}/actions/workflows/{workflow_file}/dispatches"
    payload = {"ref": "main", "inputs": {"start_row": str(start_row)}}
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"}
    r = requests.post(url, json=payload, headers=headers, timeout=30)
    print(f"Triggered next run from row {start_row}: HTTP {r.status_code}")
    if r.status_code != 204:
        print(f"Trigger failed: {r.text}")

def main():
    gc = get_gspread_client()
    sh = gc.open_by_key(os.environ["SHEET_ID"])
    ws = sh.worksheet("Sheet1")

    start_row = int(os.environ.get("START_ROW", "2"))
    col_a = ws.col_values(1)
    
    # Find all URLs from start_row onwards
    all_rows = [(idx, u.strip()) for idx, u in enumerate(col_a, start=1)
                if idx >= start_row and u and u.strip()]

    if not all_rows:
        print(f"No URLs found from row {start_row}. All done.")
        return

    batch = all_rows[:MAX_URLS_PER_RUN]
    remaining = len(all_rows) - len(batch)
    print(f"Start row: {start_row}. Processing {len(batch)} URLs. {remaining} remaining after this run.")

    # Write headers
    ws.update("B1:P1", [HEADERS])

    async def run():
        async with AsyncSession() as session:
            for i in range(0, len(batch), 30):
                sub = batch[i:i + 30]
                urls = [u for _, u in sub]
                results = await scrape_batch(session, urls)
                updates = []
                for (orig_row, _), r in zip(sub, results):
                    values = [r.get(h, "") for h in HEADERS]
                    updates.append({"range": f"B{orig_row}:P{orig_row}", "values": [values]})
                ws.batch_update(updates, value_input_option="RAW")
                if i + 30 < len(batch):
                    time.sleep(BATCH_PAUSE)

    asyncio.run(run())

    # Trigger next batch if needed
    if remaining > 0:
        next_start_row = batch[-1][0] + 1
        print(f"Triggering next run from row {next_start_row}")
        trigger_next_run(next_start_row)
    else:
        print("All URLs processed. No further trigger needed.")

if __name__ == "__main__":
    main()
