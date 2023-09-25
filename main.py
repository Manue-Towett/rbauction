import io
import re
import json
import time
import base64
import threading
from queue import Queue
from datetime import date
from typing import Optional

import requests
import pytesseract
import pandas as pd
from PIL import Image
from bs4 import BeautifulSoup

from utils import Logger

HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Content-Type": "application/x-www-form-urlencoded",
    "Dnt": "1",
    "Origin": "https://www.rbauction.com",
    "Pragma": "no-cache",
    "Sec-Fetch-Site": "same-origin",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36"
}

FORM_DATA = {
    "_58_redirect": "/home/auth",
    "_58_rememberMe": "true",
    "_58_breakpoint": "null",
    "_58_login": "writersauction@gmail.com",
    "_58_password": "F9zb3xvthfHt45f",
    "_58_chkRememberMe": "checked"
}

SEARCH_PARAMS = {
    "keywords": "",
    "searchParams": '{"id":"ar","category":"55279191388","region":"122805133758"}',
    "page": 2,
    "maxCount": 96,
    "trackingType": 2,
    "withResults": "true",
    "withFacets": "true",
    "withBreadcrumbs": "true",
    "catalog": "ar",
    "locale": "en_US",
    "userCurrency": "USD"
}

SEARCH_SLUG = "/rba-msapi/search"

ITEM_SLUG = "/rba-msapi/search/breadcrumb"

ITEM_PARAMS = {
    "catalog": "ar",
    "locale": "en_US",
    "equipmentId": "14147583"
}

OUTPUT_PATH = "./data/"

RAW_DATA_PATH = "./raw_data/"

class RbauctionScraper:
    """Scrapes equipements from https://www.rbauction.com"""
    def __init__(self) -> None:
        self.logger = Logger(__class__.__name__)
        self.logger.info("*****RbauctionScraper Started*****")

        self.base_url = "https://www.rbauction.com{}"

        self.equipements = []

        self.queue = Queue()

        with open("./settings/creds.json", "r") as file:
            creds = json.load(file)

        self.session = [self.__login(cred) for cred in creds]

    def __login(self, creds: dict[str, str]) -> requests.Session:
        """Logs into https://www.rbauction.com"""
        self.logger.info("Logging into rbauction...")

        FORM_DATA.update(creds)

        login_url = self.base_url.format("/myaccount?redirect=%2Fhome%2Fauth")

        while True:
            try:
                session = requests.Session()

                response = session.get(login_url, headers=HEADERS)

                soup = BeautifulSoup(response.text, "html.parser")

                for form in soup.select("form"):
                    if form["name"] == "_58_fm":
                        post_url = self.base_url.format(form["action"])
                        break
                
                response = session.post(post_url, data=FORM_DATA, headers=HEADERS)

                if response.ok:
                    self.logger.info("Login successful...")

                    return session
            except:
                self.logger.warn("Login failed. Retrying...")

    def __fetch_page(self, 
                     url_slug: str, 
                     params: Optional[dict[str, str|int|dict]]=None,
                     session: Optional[requests.Session] = None) -> requests.Response:
        """Fetches a particular page from the website"""
        url = self.base_url.format(url_slug)

        while True:
            try:
                response = session.get(
                    url, params=params, headers=HEADERS, timeout=30)

                if response.ok:
                    return response
            
            except:
                self.logger.error("")

    def __extract_equipements(self, 
                              response: requests.Response) -> list[dict[str, str|int]]:
        """Extracts equipements from the response from server"""
        self.logger.info("Extracting json data...")

        json_data = response.json()

        equipements = []

        for equipement in json_data["response"]["results"]:
            equipements.append({
                    "equipmentId": equipement["equipmentId"],
                    "name": equipement["name"],
                    "url": equipement["url"],
                    "meter": equipement["meter"],
                    "sellingOn": equipement["sellingOn"],
                    "price": equipement["price"]["sale"]})
        
        return equipements

    def __extract_equipement_specs(self, response: requests.Response) -> dict[str, str]:
        """Extract equipement specifications from the response object"""
        json_data: dict[str, str|list] = response.json()

        model = json_data.get("model", "")

        if not model.strip():
            model = json_data.get("name", "")

        return {"YEAR": json_data.get("year", ""),
                "MAKE": json_data.get("make", ""),
                "MODEL": model,
                "HOURS": "",
                "RBAUCTION PRICE": "",
                "SALE DATE": "",
                "PREVIOUS OWNER": "",
                "LINK TO LISTING": ""}

    def __read_image(self, base64_str: str) -> str:
        """Extracts the price from the image"""
        data = base64.b64decode(base64_str)

        image_path = "./overlay.png"

        image = Image.open(image_path)

        img = Image.open(io.BytesIO(data))

        image.paste(img, (1500, 2000), img)

        text: str = pytesseract.image_to_string(image, config=r'--psm 7')

        return int(text.strip())

    def __work(self, session: requests.Session) -> None:
        """Work to be done by the threads"""
        while True:
            item = self.queue.get()

            equipement_id = item["equipmentId"]

            params = {**ITEM_PARAMS,
                      "equipmentId": re.search(r"\d+", equipement_id).group(),
                      "catalog": re.search(r"[a-zA-Z]+", equipement_id).group()}

            response = self.__fetch_page(ITEM_SLUG, params, session)

            equipement = self.__extract_equipement_specs(response)

            equipement.update({"HOURS": item["meter"],
                               "RBAUCTION PRICE": self.__read_image(item["price"]),
                               "SALE DATE": item["sellingOn"],
                               "PREVIOUS OWNER": "",
                               "LINK TO LISTING": self.base_url.format(
                                                item["url"]).replace("amp;", "")})

            self.equipements= [equipement, *self.equipements]

            self.queue_len -= 1

            self.logger.info(f"Queue: {self.queue_len} || Crawled: {len(self.equipements)}")

            try:
                if len(self.equipements) % 100 == 0:
                    self.__save_to_excel(self.equipements)
            except:pass
            
            time.sleep(2)

            self.queue.task_done()

    def __save_to_excel(self, data: list[dict[str, str|int]]) -> None:
        """Saves data retrieved to excel"""
        self.logger.info("Saving data obtained to excel...")

        df = pd.DataFrame(data).drop_duplicates()

        df.fillna("", inplace=True)

        df["YEAR"] = df["YEAR"].astype("str")

        df["YEAR"] = [value.strip().replace(".0", "") for value in df["YEAR"]]

        filename = f"rbauction_data_{date.today()}.xlsx"

        writer = pd.ExcelWriter(path=f"{OUTPUT_PATH}{filename}",
                                engine="xlsxwriter",
                                engine_kwargs={"options": {"strings_to_urls": False}})

        df.to_excel(writer, index=False)

        writer.close()

        self.logger.info("{} records saved to {}".format(len(df), filename))

    def scrape(self) -> None:
        """Entry point to the scraper"""
        self.logger.info("Scraping equipements...")

        [threading.Thread(target=self.__work, 
                          daemon=True,
                          args=(session, )).start() for session in self.session]

        HEADERS.update({"Accept": "application/json, text/plain, */*"})

        HEADERS.pop("Content-Type")

        page = 1

        equipements = []

        with open(f"{RAW_DATA_PATH}raw_data.json", "r") as file:
            equipements: list = json.load(file)

            equipements_no = len(equipements)

        new_equipements = []

        while True:
            self.logger.info("Fetching equipements from page {}".format(page))

            SEARCH_PARAMS.update({"page": page})

            response = self.__fetch_page(SEARCH_SLUG, SEARCH_PARAMS, self.session[0])

            new_equipements.extend(self.__extract_equipements(response))

            for new_equipement in new_equipements:
                if new_equipement not in equipements:
                    equipements = [new_equipement, *equipements]

            json_response = response.json()

            max_page = json_response["response"]["Pagination"]["NofPages"]

            if page >= max_page or page == 40:
                break

            page += 1

            time.sleep(4)

        [new_equipements.append(equipement) for equipement in equipements 
         if not equipement in new_equipements]
        
        equipements = new_equipements

        self.logger.info("Equipements found: {} || Pages Remaining: {}".format(
            len(equipements), max_page - page))
        
        with open(f"{RAW_DATA_PATH}raw_data.json", "w") as file:
            json.dump(equipements, file, indent=4)
        

        HEADERS["Accept"] = "application/vnd.rba.search.v1+json"

        self.queue_len = len(equipements[:1400])

        self.equipements = pd.read_excel(
            "./rbauction_data_2023-09-16.xlsx").to_dict("records")
        
        [self.queue.put(equipement) for equipement in equipements[:1400]]
        self.queue.join()
        
        self.__save_to_excel(self.equipements)

        self.logger.info("Finished.")

if __name__ == "__main__":
    scraper = RbauctionScraper()
    scraper.scrape()