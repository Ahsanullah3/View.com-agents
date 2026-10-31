import json
import os
import re
import gspread
from curl_cffi.requests import AsyncSession
import asyncio
from gspread.exceptions import APIError

# ---------- Google Sheets Connection ----------
def get_gspread_client():
    raw = os.environ["GOOGLE_SA_JSON"]
    return gspread.service_account_from_dict(json.loads(raw))

# ---------- Single Page Scraping ----------
async def scrape_one(session, url, sem):
    async with sem:
        try:
            # Key: Use curl_cffi to impersonate Chrome, bypass TLS fingerprint detection
            resp = await session.get(url, impersonate="chrome120", timeout=30)
            if resp.status_code != 200:
                return {"url": url, "error": f"HTTP {resp.status_code}"}

            html = resp.text
            match = re.search(
                r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
                html, re.DOTALL
            )
            if not match:
                return {"url": url, "error": "No __NEXT_DATA__"}

            data = json.loads(match.group(1))
            props = data.get("props", {}).get("pageProps", {})
            agent = props.get("agentProfile", {})
            agency = agent.get("agencyDetail", {})

            return {
                "url": url,
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
        except Exception as e:
            return {"url": url, "error": str(e)}

# ---------- Batch Concurrent Scraping ----------
async def scrape_all(urls, concurrency=5):
    sem = asyncio.Semaphore(concurrency)  # Recommended to start with 5, increase slowly after testing
    async with AsyncSession() as session:
        tasks = [scrape_one(session, u, sem) for u in urls]
        return await asyncio.gather(*tasks)

# ---------- Main Flow ----------
def main():
    gc = get_gspread_client()
    sh = gc.open_by_key(os.environ["SHEET_ID"])
    ws = sh.worksheet("Sheet1")

    # Read URLs from A2:A, filter out empty values
    col_a = ws.col_values(1)  # Column A, index starts at 1
    urls = [u.strip() for u in col_a[1:] if u and u.strip()]
    if not urls:
        print("No URLs found in A2:A")
        return

    print(f"Scraping {len(urls)} URLs...")
    results = asyncio.run(scrape_all(urls, concurrency=5))

    # Prepare write-back data: each row corresponds to a URL, starting from row 2
    # Header: Agent ID | First Name | Last Name | Position | Email | Mobile | Phone | Avg Sold Price | Sold Listings | Agency ID | Agency Name | Agency Address | Agency Phone | Agency Website | Error
    headers = [
        "agent_id", "first_name", "last_name", "position", "email", "mobile", "phone",
        "average_sold_price", "sold_listings", "agency_id", "agency_name",
        "agency_address", "agency_phone", "agency_website", "error"
    ]
    # Write headers to B1:P1
    ws.update("B1:P1", [headers])

    rows = []
    for r in results:
        rows.append([r.get(h, "") for h in headers])

    # Write to B2:P(N+1)
    ws.update(f"B2:P{1 + len(rows)}", rows)
    print("Done.")

if __name__ == "__main__":
    main()
