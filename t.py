import pandas as pd
from datetime import date

import re
import json
from urllib.parse import urlparse

print("here")
with open("./raw_data/raw_data.json", "r") as file:
    json_equipements = json.load(file)

print("json")
df = pd.read_excel("./rbauction_data_2023-09-16.xlsx")

print(len(df))

df.fillna("", inplace=True)

df["YEAR"] = df["YEAR"].astype("str")

df["YEAR"] = [value.strip().replace(".0", "") for value in df["YEAR"]]

equipements = df.to_dict("records")

print("excel")
results = []

for json_equipement in json_equipements:
    url_slug = json_equipement["url"]

    for equipement in equipements:

        # print(urlparse(equipement["LINK TO LISTING"]).path)

        if url_slug in equipement["LINK TO LISTING"]:
            results.append(equipement)

            print(len(results))

            break

    if len(results) == len(equipements): break


df = pd.DataFrame(results)

filename = f"rbauction_{date.today()}.xlsx"

writer = pd.ExcelWriter(
    filename, 
   engine='xlsxwriter',
   engine_kwargs={'options': {'strings_to_urls': False}}
)

df.to_excel(writer, index=False)

print(len(df))

writer.close()