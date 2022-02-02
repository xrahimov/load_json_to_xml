import base64
import json
import xmltodict
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

src = BeautifulSoup(body_decoded, "xml")

# Convert xml file to dict
data_dict = xmltodict.parse(body_decoded)

# Generate the object using json.dumps()
json_data = json.dumps(data_dict)

# Get data from json_data
columns = []
rows = []


def get_nested_value(data):
    for key, value in data.items():
        # print(str(key) + "->" + str(value))

        columns.append(str(key))
        columns.append(str(value))

        if type(value) == type(dict()):
            get_nested_value(value)
        elif type(value) == type(list()):
            for val in value:
                if type(val) == type(str()):
                    pass
                elif type(val) == type(list()):
                    pass
                else:
                    get_nested_value(val)
        print(columns)
        print(rows)


column = ""


def json_extract(obj, key):
    """Recursively fetch values from nested JSON."""
    arr = []

    def extract(obj, arr, key):
        """Recursively search for values of key in JSON tree."""
        global column
        if isinstance(obj, dict):
            for k, v in obj.items():
                if isinstance(v, (dict, list)):
                    extract(v, arr, key)
                    column += k.capitalize()
                elif k == key:
                    columns.append(column)
                    column = ""
                    arr.append(v)
        elif isinstance(obj, list):
            for item in obj:
                extract(item, arr, key)
        return arr

    values = extract(obj, arr, key)
    return values


# get_nested_value(data_dict)
print(json_extract(data_dict, "@displayName"))
print(columns)

# with open("file.json", "w") as json_file:
#     json_file.write(json_data)
#     json_file.close()
