import pandas as pd
import json
import xmltodict
import base64
from bs4 import BeautifulSoup

# Read the data inside the xml file to a variable under the name  data
with open("large_source.txt", "r") as f:
    original_file = f.read()
    f.close()

# Pass the stored data inside the beautifulsoup parser
bs_src = BeautifulSoup(original_file[1:-1], "xml")

# Find body tag and decode on base-64
body_encoded = bs_src.find("Body").get_text()
body_decoded = base64.b64decode(body_encoded)

# with open("xml.xml", "wb") as x:
#     x.write(body_decoded)
#     x.close()
# Convert xml file to dict
data_dict = xmltodict.parse(body_decoded)

# Generate the object using json.dumps()
json_data = json.dumps(data_dict)

# Convert to dataframe and export to xlsx file
df_full = pd.json_normalize(data_dict["ClinicalDocument"])
df_component = pd.json_normalize({})
df_documentationOf_serviceEvent_performer = pd.json_normalize({})

try:
    df_component = pd.json_normalize(
        data_dict["ClinicalDocument"]["component"]["structuredBody"]["component"]
    )
    df_documentationOf_serviceEvent_performer = pd.json_normalize(
        data_dict["ClinicalDocument"]["documentationOf"]["serviceEvent"]["performer"]
    )
except:
    print("Key does not exist in source file")

df_full.to_excel("full_data.xlsx")
df_component.to_excel("component_data.xlsx")
df_documentationOf_serviceEvent_performer.to_excel("documentationOf_performer.xlsx")
