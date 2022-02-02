import base64
from bs4 import BeautifulSoup

# Read the data inside the xml file to a variable under the name  data
with open("source.txt", "r") as f:
    original_file = f.read()

# Pass the stored data inside the beautifulsoup parser
bs_src = BeautifulSoup(original_file[1:-1], "xml")

# Find body tag and decode on base-64
body_encoded = bs_src.find("Body").get_text()
body_decoded = base64.b64decode(body_encoded)

src = BeautifulSoup(body_decoded, "xml")

# Pyspark dataframe
parts = {
    "patientRole": {
        "assigningAuthorityName": ["id"],
        "family": None,
        "given": None,
        "streetAddressLine": None,
        "city": None,
        "state": None,
        "postalCode": None,
        "country": None,
        "name": None,
        "displayName": ["administrativeGenderCode"],
        "value": ["birthTime", "telecom"],
        "translation": [
            "maritalStatusCode",
            "religiousAffiliationCode",
            "raceCode",
            "ethnicGroupCode",
        ],
    },
    "assignedAuthor": {
        "streetAddressLine": None,
        "city": None,
        "state": None,
        "postalCode": None,
        "country": None,
        "value": ["telecom"],
    },
    "assignedAuthoringDevice": {
        "manufacturerModelName": None,
        "softwareName": None,
    },
    "author representedOrganization": {
        "name": None,
        "value": ["telecom"],
        "streetAddressLine": None,
        "city": None,
        "state": None,
        "postalCode": None,
        "country": None,
    },
    "custodian assignedCustodian representedCustodianOrganization": {
        "name": None,
        "value": ["telecom"],
        "streetAddressLine": None,
        "city": None,
        "state": None,
        "postalCode": None,
        "country": None,
    },
    "legalAuthenticator": {
        "value": ["time", "telecom"],
        "code": ["signatureCode"],
        "streetAddressLine": None,
        "city": None,
        "state": None,
        "postalCode": None,
        "country": None,
        "assignedPerson": None,
    },
    "legalAuthenticator assignedEntity representedOrganization": {
        "name": None,
        "value": ["telecom"],
        "streetAddressLine": None,
        "city": None,
        "state": None,
        "postalCode": None,
        "country": None,
    },
    "encounter": {
        "displayName": ["code"],
        # "encounter "
    }
}

columns = []
rows = []

for val in parts:
    part = src
    c_header = val.split()
    for i in c_header:
        part = src.find(i)
    c_header = "" if len(c_header) == 1 else "".join(x.capitalize() for x in c_header)

    for i in parts[val]:
        c = parts[val][i]
        if c:
            for j in c:
                if i == "translation":
                    columns.append(c_header + j.capitalize())
                    rows.append(part.find(j).find("translation").get("displayName"))
                else:
                    columns.append(c_header + i.capitalize())
                    rows.append(part.find(j).get(i))
        else:
            columns.append(c_header + i.capitalize())
            rows.append(part.find(i).get_text())
print(columns)
print(rows)
