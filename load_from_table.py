from dataclasses import replace
import pyodbc
import pandas as pd
import json
import xmltodict
import base64
from bs4 import BeautifulSoup

# Connect to server
conn = pyodbc.connect(
    "Driver={SQL Server};"
    "Server=DESKTOP-OEF5MAB;"
    "Database=default;"
    "Trusted_Connection=yes;"
)

cursor = conn.cursor()
query = "SELECT * FROM PersonInformation"

# Insert into pandas dataframe
sql_src_df = pd.read_sql(query, conn)

# Read the data inside the xml file to a variable under the name  data
with open("large_source.txt", "r") as f:
    original_file = f.read()
    f.close()

# Pass the stored data inside the beautifulsoup parser
bs_src = BeautifulSoup(original_file[1:-1], "xml")

# Find body tag and decode on base-64
body_encoded = bs_src.find("Body").get_text()
body_decoded = base64.b64decode(body_encoded)

body_decoded = BeautifulSoup(body_decoded, "xml")
parent = body_decoded

tables = sql_src_df["Table"].unique()
results_dict = {}

for i in tables:
    results_dict[i] = {"columns": [], "rows": []}

for index, row in sql_src_df.iterrows():
    if row["Element_ID"].strip() == "":
        parent = body_decoded
        for i in row["Data_Source"].replace("cda:", "").replace(" ", "").split("/"):
            if i == "":
                continue
            parent = parent.find(i)
    else:
        results_dict[row["Table"]]["columns"].append(row["Section_Name"])
        c_row = parent.find(row["Data_Source"].replace("cda:", "").replace(" ", ""))
        c_row = c_row if c_row != None else ""
        results_dict[row["Table"]]["rows"].append(c_row)
print(results_dict)

with open("json.json", "w") as x:
    x.write(json.dumps(results_dict))
    x.close()
