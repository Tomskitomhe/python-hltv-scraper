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
    "file_to_read": "data/recent_match_data.csv",  # ".../.../example.csv" (file to read from, e.g. urls)
    "savedir_location": "demos/",  # ".../.../example/" (directory that will get created/updated when finished)
    "match_amount": 4,  # -1 = all - Note: This will probably take a REALLY long time, depending on url amount
    "headless": True,  # hide browser
    "screen": Screen(max_width=1920, max_height=1080),
    "screen_amount": 1,  # only matters if headless = False
    "session_amount": 2,  # amount of parallel sessions (you will get rate limited if you set this too high)
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


df = pd.read_csv(config["file_to_read"])
match_data = []  # List that gets turned into the savefile
lock = asyncio.Lock()  # lock for thread-safety when writing to data list
demos_scraped = 0

if config["use_proxy"]:
    proxy_list = []
    with open(config["proxy_location"], "r") as file:
        for line in file:
            proxy_list.append(line.strip())


# Get random proxy from list
def get_proxy():
    proxy = random.choice(proxy_list).split(":")
    server = proxy[0] + ":" + proxy[1]
    username = proxy[2]
    password = proxy[3]
    proxy_dict = {"server": server, "username": username, "password": password}
    return proxy_dict



async def scrape_match(session_id, headless, url_list):
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

        for url in url_list:
            try:
                start_time = time.perf_counter()

                async with page.expect_download() as download_info:
                    try:
                        await page.goto(url)

                    except Exception as e:
                        if "Download is starting" not in str(e):
                            status(
                                f"[-] Session {session_id}: Exception - {e}"
                            )
                            break

                download = await download_info.value
                async with lock:
                    target = Path(config["savedir_location"]) / download.suggested_filename
                    if target.exists():
                        await download.save_as(target)
                        demos_scraped += 1
                        

                end_time = time.perf_counter()
                elapsed = end_time - start_time

                status(
                    f"[+] Session {session_id}: Successfuly downloaded match demo ({demos_scraped} / {config["match_amount"]}) ({elapsed:.2f}s)",
                    bcolors.SUCCESS
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
                    f"[-] Session {session_id}: Exception - {e}",
                    bcolors.FAIL,
                )
                break


def distribute_urls(df, match_amount, session_amount):
    url_lists = [[] for _ in range(session_amount)]
    urls = df["download_url"].to_list()
    urls = [url for url in urls if isinstance(url, str)]

    for url in urls[:match_amount]:
        smallest_list = min(url_lists, key=len)
        smallest_list.append(url)

    return url_lists


async def main():
    total_start_time = time.perf_counter()

    if config["match_amount"] == -1:
        status(
            f"[!] Scraping all matches ({len(df["download_url"])})",
            bcolors.WARNING,
        )
    else:
        status(f"[!] Scraping {config["match_amount"]} matches", bcolors.WARNING)

    urls_per_session = distribute_urls(
        df, config["match_amount"], config["session_amount"]
    )
    
    tasks = []
    for i in range(config["session_amount"]):

        if not config["headless"]:
            is_headless = False if i < config["screen_amount"] else True
        else:
            is_headless = True

        tasks.append(
            asyncio.create_task(
                scrape_match(
                    session_id=i,
                    headless=is_headless,
                    url_list=urls_per_session[i],
                )
            )
        )

    await asyncio.gather(*tasks)

    total_elapsed = time.perf_counter() - total_start_time


    status(
        f"[+] Successfully saved to directory ({config["savedir_location"]}) (took {total_elapsed:.2f}s)",
        bcolors.SUCCESS,
    )




if __name__ == "__main__":
    asyncio.run(main())