import pandas as pd
import base64
import json
from bs4 import BeautifulSoup

# Read the data inside the xml file to a variable under the name  data
with open("source.txt", "r") as f:
    original_file = f.read()
    f.close()

# Pass the stored data inside the beautifulsoup parser
bs_src = BeautifulSoup(original_file[1:-1], "xml")

# Find body tag and decode on base-64
body_encoded = bs_src.find("Body").get_text()
body_decoded = base64.b64decode(body_encoded)

body_decoded = BeautifulSoup(body_decoded, "xml")
parent = body_decoded

result_dict = {}
tables = [
    # ["medications", "History of Medication Use"],
    # ["procedures", "History of Procedures"],
    # ["encounters", "History of Encounters"],
    # ["allergies", "Allergies, Adverse Reactions, Alerts"],
    ["immunizations", "History of Immunizations"],
    # ["vital_signs", "Vital Signs"],
    # ["payers", "Payers"],
]


def retrieve_data_from_table(displayName):
    body = ""
    obj = {"columns": [], "rows": []}
    for el in parent.find_all("component"):
        if (
            el.find("code") != None
            and el.find("code").get("displayName") == displayName
        ):
            body = el.find("table")
            break
    if body == None or body.replace(" ", ""):
        return -1
    for th in body.find("thead").find("tr").find_all("th"):
        obj["columns"].append(th.get_text())

    for tr in body.find("tbody").find_all("tr"):
        td_arr = []
        for td in tr.find_all("td"):
            td_arr.append(td.get_text())
        obj["rows"].append(td_arr)
    return obj


for table_arr in tables:
    data = retrieve_data_from_table(table_arr[1])
    if data != -1:
        result_dict[table_arr[0]] = data

for i in result_dict:
    pd.DataFrame(
        result_dict[i]["rows"],
        columns=result_dict[i]["columns"],
    ).to_excel(i + ".xlsx")

# with open("json.json", "w") as x:
#     json.dump(result_dict, x)
#     x.close()
