import asyncio
import json
import logging
import os
import random
import re
import time

import gspread
import requests
from curl_cffi.requests import AsyncSession

# Configure logging to print timestamp, level, and message clearly
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# Configured to process 200 total per run with 20 parallel workers per chunk
MAX_URLS_PER_RUN = 200
CONCURRENCY = 20
BATCH_PAUSE = 2

# Write-back starts at column H (8th column). 15 fields -> H to V
START_COL_LETTER = "H"
START_COL_INDEX = 8
HEADERS = [
    "agent_id", "first_name", "last_name", "position", "email",
    "mobile", "phone", "average_sold_price", "sold_listings",
    "agency_id", "agency_name", "agency_address", "agency_phone",
    "agency_website", "error"
]
END_COL_LETTER = gspread.utils.rowcol_to_a1(1, START_COL_INDEX + len(HEADERS) - 1).rstrip("1")  # "V"


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


async def scrape_one(session: AsyncSession, row_idx: int, url: str, sem: asyncio.Semaphore):
    async with sem:
        logger.info(f"Row {row_idx}: Starting scrape -> {url}")
        await asyncio.sleep(random.uniform(0.2, 0.8))
        try:
            resp = await session.get(url, impersonate="chrome124", timeout=30)
            
            # Immediately skip and log any status codes other than 200 OK
            if resp.status_code != 200:
                logger.warning(f"Row {row_idx}: HTTP {resp.status_code} -> Invalid/broken link. Skipping immediately.")
                return {"url": url, "error": f"http_status_{resp.status_code}"}

            # Check for DataDome block
            if is_datadome_block(resp):
                logger.warning(f"Row {row_idx}: DataDome Blocked -> Skipping immediately without fallback.")
                return {"url": url, "error": "datadome_blocked"}

            parsed = parse_next_data(resp.text)
            if parsed:
                logger.info(
                    f"Row {row_idx}: SUCCESS | Agent: {parsed.get('first_name')} {parsed.get('last_name')} | "
                    f"Agency: {parsed.get('agency_name')} | Sold Listings: {parsed.get('sold_listings')}"
                )
                return {"url": url, **parsed}
            
            logger.warning(f"Row {row_idx}: Failed to parse Next.js data payload.")
            return {"url": url, "error": "invalid_payload"}

        except Exception as e:
            logger.error(f"Row {row_idx}: ERROR | {str(e)}")
            return {"url": url, "error": str(e)}


async def scrape_batch(session: AsyncSession, sub_batch: list[tuple[int, str]]):
    sem = asyncio.Semaphore(CONCURRENCY)
    tasks = [scrape_one(session, orig_row, u, sem) for orig_row, u in sub_batch]
    return await asyncio.gather(*tasks)


def trigger_next_run(start_row: int):
    token = os.environ["GITHUB_TOKEN"]
    repo = os.environ["GITHUB_REPOSITORY"]
    workflow_file = "scrape.yml"
    url = f"https://api.github.com/repos/{repo}/actions/workflows/{workflow_file}/dispatches"
    payload = {"ref": "main", "inputs": {"start_row": str(start_row)}}
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"}
    r = requests.post(url, json=payload, headers=headers, timeout=30)
    logger.info(f"Triggered next run from row {start_row}: HTTP {r.status_code}")
    if r.status_code != 204:
        logger.error(f"Trigger failed: {r.text}")


def main():
    logger.info("Initializing Google Sheets client...")
    gc = get_gspread_client()
    sh = gc.open_by_key(os.environ["SHEET_ID"])
    ws = sh.worksheet("Sheet1")

    start_row = int(os.environ.get("START_ROW", "2"))
    col_a = ws.col_values(1)

    all_rows = [(idx, u.strip()) for idx, u in enumerate(col_a, start=1)
                if idx >= start_row and u and u.strip()]

    if not all_rows:
        logger.info(f"No URLs found from row {start_row}. All done.")
        return

    batch = all_rows[:MAX_URLS_PER_RUN]
    remaining = len(all_rows) - len(batch)
    logger.info(f"Start row: {start_row}. Processing batch of {len(batch)} URLs. {remaining} remaining after this run.")

    # Fixed gspread update keyword syntax
    ws.update(values=[HEADERS], range_name=f"{START_COL_LETTER}1:{END_COL_LETTER}1")

    async def run():
        async with AsyncSession() as session:
            for i in range(0, len(batch), CONCURRENCY):
                sub = batch[i:i + CONCURRENCY]
                logger.info(f"--- Processing concurrent chunk: rows {sub[0][0]} to {sub[-1][0]} ({len(sub)} links) ---")
                
                results = await scrape_batch(session, sub)
                updates = []
                for (orig_row, _), r in zip(sub, results):
                    values = [r.get(h, "") for h in HEADERS]
                    updates.append({
                        "range": f"{START_COL_LETTER}{orig_row}:{END_COL_LETTER}{orig_row}",
                        "values": [values]
                    })
                
                ws.batch_update(updates, value_input_option="RAW")
                logger.info(f"Successfully wrote chunk (rows {sub[0][0]} to {sub[-1][0]}) to Google Sheets.")

                if i + CONCURRENCY < len(batch):
                    logger.info(f"Pausing for {BATCH_PAUSE} seconds before next chunk...")
                    time.sleep(BATCH_PAUSE)

    asyncio.run(run())

    if remaining > 0:
        next_start_row = batch[-1][0] + 1
        logger.info(f"Batch complete. Triggering next GitHub workflow run starting at row {next_start_row}...")
        trigger_next_run(next_start_row)
    else:
        logger.info("All URLs in the spreadsheet have been successfully processed. No further trigger needed.")


if __name__ == "__main__":
    main()
