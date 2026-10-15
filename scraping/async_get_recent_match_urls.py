from camoufox.async_api import AsyncCamoufox
from browserforge.fingerprints import Screen
from bs4 import BeautifulSoup
from pathlib import Path
import pandas as pd
import asyncio
import random
import time
import json
import math

config = {
    "savefile_location": "data/recent_match_urls.csv",  # ".../.../example.csv" (file that will get created/updated when finished)
    "url_amount": 2000,  # amount of urls to scrape, rounded to next 100
    "headless": "virtual",  # hide browser
    "screen": Screen(max_width=1920, max_height=1080),
    "screen_amount": 4,  # only matters if headless = False
    "session_amount": 4,  # amount of parallel sessions (you will get rate limited if you set this too high)
    "session_timeout": 1,  # timeout between sessions ([0.8, 1.2] also possible for a random timeout in a range)
    "use_proxy": False,  # use proxy
    "use_proxy_once": False,  # enable this to use a different proxy for each session (if you have enough proxies)
    "proxy_location": "data/proxies.txt",  # location of the proxy list (format: server:port:username:password), 1 every line
    "user_agents_location": "data/user_agents.json",  # location of the user agents list # refer to the readme for more info
    "cookie_location": "data/autologin_cookie.json",  # location of the cookies incl. autologin
}


class bcolors:  # colors to print in color
    HEADER = "\033[95m"
    OKBLUE = "\033[94m"
    OKCYAN = "\033[96m"
    SUCCESS = "\033[92m"
    WARNING = "\033[93m"
    FAIL = "\033[91m"
    ENDC = "\033[0m"
    BOLD = "\033[1m"
    UNDERLINE = "\033[4m"


def status(msg, color=bcolors.ENDC):
    print(color + msg + bcolors.ENDC)


if config["url_amount"] > 0:
    config["url_amount"] = int(math.ceil(config["url_amount"] / 100.0) * 100)

recent_matches_url = "https://www.hltv.org/results"
match_urls = []  # list for all urls (will get saved to csv)
lock = asyncio.Lock()  # lock for thread-safety when writing to match_urls

if config["use_proxy"]:
    proxy_list = []
    with open(config["proxy_location"], "r") as file:
        for line in file:
            proxy_list.append(line.strip())


# Get random proxy from list
def get_proxy():
    proxy = random.choice(proxy_list)
    if config["use_proxy_once"]:
        proxy_list.remove(proxy)
    proxy = proxy.split(":")
    server = proxy[0] + ":" + proxy[1]
    username = proxy[2]
    password = proxy[3]
    proxy_dict = {"server": server, "username": username, "password": password}
    return proxy_dict


async def scrape_match_urls(session_id, offsets, headless, url=recent_matches_url):
    proxy = get_proxy() if config["use_proxy"] else None
    async with AsyncCamoufox(
        headless=headless,
        screen=config["screen"],
        geoip=True,
        proxy=proxy,
        os=["windows", "macos"],
        humanize=True,
    ) as browser:
        context = await browser.new_context()
        if config["use_proxy"]:
            status(
                f"[+] [Session {session_id}] Successfully connected with proxy ({proxy['server']})",
                bcolors.OKCYAN,
            )
        else:
            status(
                f"[+] [Session {session_id}] Successfully started without proxy",
                bcolors.OKCYAN,
            )


        # Auto-login & add necessary cookies
        with open(config["cookie_location"], "r") as f:
            cookies = json.load(f)
        await context.add_cookies(cookies)

        page = await context.new_page()

        for offset in offsets:
            try:
                start_time = time.perf_counter()

                offset_url = f"{url}?offset={offset}"
                response = await page.goto(offset_url, wait_until="domcontentloaded", timeout=30_000)
                if response.status != 200:
                    status(
                        f"[!] [Session {session_id}] HTTP status - {response.status if response else "None"}",
                        bcolors.WARNING
                    )

                try:
                    await page.wait_for_selector("div.results", state="attached", timeout=30_000)
                except Exception as e:
                    status(
                        f"[!] [Session {session_id}] Title - {await page.title()}",
                        bcolors.WARNING
                    )
                    await page.screenshot(path=f"debug/session-{session_id}.png", full_page=True, timeout=10_000)
                    raise

                html = await page.inner_html("div.results")
                soup = BeautifulSoup(html, "html.parser")

                recent_matches = soup.select_one("div.allres")
                new_urls = 0
                if recent_matches:
                    result_con = recent_matches.select("div.result-con")
                    before = len(match_urls)

                    new_links = []
                    for match in result_con:
                        result = match.find("a", href=True)
                        if result:
                            full_url = f"https://www.hltv.org{result['href']}"
                            new_links.append(full_url)

                    # thread-safe
                    async with lock:
                        for link in new_links:
                            if link not in match_urls:
                                match_urls.append(link)
                    after = len(match_urls)
                    new_urls = after - before

                end_time = time.perf_counter()
                elapsed = end_time - start_time

                status(
                    f"[+] [Session {session_id}] {new_urls} URLs scraped! ({len(match_urls)} / {config['url_amount']}) ({elapsed:.2f}s)",
                    bcolors.SUCCESS,
                )

                if type(config["session_timeout"]) == list:
                    await asyncio.sleep(
                        random.uniform(
                            config["session_timeout"][0], config["session_timeout"][1]
                        )
                    )
                else:
                    await asyncio.sleep(config["session_timeout"])

            except Exception as e:
                status(
                    f"[-] Session {session_id}: Exception at Offset {offset} - {e}",
                    bcolors.FAIL,
                )
                break


def distribute_offsets(url_amount, session_amount, step=100):
    # Create empty lists for each session
    offset_lists = [[] for _ in range(session_amount)]

    # Generate all offsets
    all_offsets = list(range(0, url_amount, step))

    for offset in all_offsets:
        # find the list with the smallest length
        smallest_list = min(offset_lists, key=len)
        # add offset to the smallest list
        smallest_list.append(offset)

    return offset_lists


async def main():
    total_start_time = time.perf_counter()

    if config["url_amount"] == -1:
        status(
            "[!] Scraping all URLs (will probably take forever, not recommended)",
            bcolors.WARNING,
        )
        return
    else:
        status(f"[!] Scraping {config['url_amount']} URLs", bcolors.WARNING)

    offsets_per_session = distribute_offsets(
        config["url_amount"], config["session_amount"]
    )

    tasks = []
    for i in range(config["session_amount"]):

        if not config["headless"]:
            is_headless = config["headless"] if i < config["screen_amount"] else True
        else:
            is_headless = True

        tasks.append(
            asyncio.create_task(
                scrape_match_urls(
                    session_id=i,
                    offsets=offsets_per_session[i],
                    headless=is_headless,
                    url=recent_matches_url,
                )
            )
        )

    await asyncio.gather(*tasks)

    total_elapsed = time.perf_counter() - total_start_time

    # save results
    final_df = pd.DataFrame(match_urls, columns=["match_url"])
    filepath = Path(config["savefile_location"])
    filepath.parent.mkdir(parents=True, exist_ok=True)
    final_df.to_csv(filepath, index=False)

    status(
        f"[+] Successfully saved to file ({config['savefile_location']}) (took {total_elapsed:.2f}s)",
        bcolors.SUCCESS,
    )


if __name__ == "__main__":
    asyncio.run(main())
