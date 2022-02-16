import logging

# from azure.storage.blob import BlobServiceClient
import pandas as pd
import base64
from bs4 import BeautifulSoup
import pyodbc

# import azure.functions as func

server = ""
database = ""
username = ""
password = ""
driver = "{ODBC Driver 17 for SQL Server}"
account_url = ""

src_container = ""
arch_container = ""
account_name = ""
account_key = ""

# blob_service = BlobServiceClient.from_connection_string(
#     ""
# )
# generator = blob_service.get_container_client(src_container).list_blobs()
# container_client = blob_service.get_container_client(src_container)


tables = [
    # ["Test_CCDA_Medications", "History of Medication Use"],
    # ["Test_CCDA_Procedures", "History of Procedures"],
    # ["Test_CCDA_Encounter", "History of Encounters"],
    # ["Test_CCDA_Allergies", "Allergies, Adverse Reactions, Alerts"],
    # ["Test_CCDA_Immunizations", "History of Immunizations"],
    # ["Test_CCDA_Vital_Signs", "Vital Signs"],
    # ["Test_CCDA_Payers", "Payers"],
    # ["Test_CCDA_Problems", "Problem list"],
    ["Test_CCDA_Results", "Relevant diagnostic tests and/or laboratory data"]
]

parts = {
    "patientRole": {
        "family": None,
        "given": None,
        "streetAddressLine": None,
        "city": None,
        "state": None,
        "postalCode": None,
        "country": None,
        "displayName": ["administrativeGenderCode"],
        "value": ["birthTime", "telecom"],
        "translation": [
            "maritalStatusCode",
            "religiousAffiliationCode",
            "raceCode",
            "ethnicGroupCode",
            "sdtc:raceCode",
            "ethnicGroupCode",
        ],
    },
}

performer_parts = {
    "performer": {
        "value": ["low", "high", "telecom"],
        "displayName": ["functionCode"],
        "family": None,
        "given": None,
        "suffix": None,
        "streetAddressLine": None,
        "city": None,
        "state": None,
        "postalCode": None,
        "country": None,
    }
}

# Move inserted file into archive
# def move_files(src, dest, blob_name):
#     source_blob = f"https://{account_name}.blob.core.windows.net/{src}/{blob_name}"
#     target_blob = blob_service.get_blob_client(dest, blob_name)
#     target_blob.start_copy_from_url(source_blob)

#     # Delete the source file
#     remove_blob = blob_service.get_blob_client(src, blob_name)
#     remove_blob.delete_blob()


def get_patient_info(src, parts, mrn):
    obj = {"columns": ["MRN"], "rows": [mrn]}
    for val in parts:
        part = src
        c_header = val
        for i in parts[val]:
            c = parts[val][i]
            if c:
                for j in c:
                    if i == "translation":
                        obj["columns"].append(c_header + j.capitalize())
                        obj["rows"].append(
                            part.find(j).find("translation").get("displayName")
                        )
                    else:
                        obj["columns"].append(
                            c_header + j.capitalize() + i.capitalize()
                        )
                        obj["rows"].append(part.find(j).get(i))
            else:
                obj["columns"].append(c_header + i.capitalize())
                obj["rows"].append(
                    part.find(i).get_text() if part.find(i) != None else ""
                )

    return obj


def get_performer(src, performers_data, mrn):
    performers = src.find("documentationOf").find("serviceEvent").find_all("performer")
    for performer in performers:
        obj = get_patient_info(performer, performer_parts, mrn)
        if len(performers_data["columns"]) == 0:
            performers_data["columns"] = obj["columns"]
        performers_data["rows"].append(obj["rows"])


def retrieve_data_from_table(displayName, parent, mrn):
    body = ""
    obj = {"columns": [], "rows": []}
    for el in parent.find_all("component"):
        if (
            el.find("code") != None
            and el.find("code").get("displayName") == displayName
        ):
            body = el.find("table")
            break
    if body == None:
        return -1
    if body == "":
        return -1

    obj["columns"].append("MRN")

    for th in body.find("thead").find("tr").find_all("th"):
        obj["columns"].append(th.get_text())
    for tr in body.find("tbody").find_all("tr"):
        td_arr = [mrn]
        for td in tr.find_all("td"):
            td_arr.append(td.get_text())
        obj["rows"].append(td_arr)

    return obj


def retrieve_data_from_results(displayName, parent, mrn):
    body = ""
    obj = {"columns": [], "rows": [], "atomic_columns": ["MRN", "Atomic_ID"], "atomic_rows": []}
    for el in parent.find_all("component"):
        if (
            el.find("code") != None
            and el.find("code").get("displayName") == displayName
        ):
            body = el.find("table")
            break
    if body == None:
        return -1
    if body == "":
        return -1

    obj["columns"].append("MRN")

    for th in body.find("thead").find("tr").find_all("th"):
        obj["columns"].append(th.get_text())

    for th in body.find("tbody").find("tr").find("thead").find("tr").find_all("th"):
        obj["atomic_columns"].append(th.get_text())

    for tr in body.find("tbody").find_all("tr"):
        try:
            td_arr = [mrn]
            atomic_id = tr.get("ID")
            inner_td_arr = [mrn, atomic_id]

            for innter_td in tr.find("table").find("tbody").find("tr").find_all("td"):
                inner_td_arr.append(innter_td.get_text())

            tr.find("list").replace_with(atomic_id)

            for td in tr.find_all("td"):
                td_arr.append(td.get_text())

            obj["rows"].append(td_arr)
            obj["atomic_rows"].append(inner_td_arr)
        except AttributeError:
            continue

    print(obj["columns"])
    print(obj["rows"])
    print(obj["atomic_columns"])
    print(obj["atomic_rows"])
    return obj


def main():
    # logging.info(
    #     f"Python blob trigger function processed blob \n"
    #     f"Name: {myblob.name}\n"
    #     f"Blob Size: {myblob.length} bytes"
    # )

    # for blob in generator:
    # original_file = (
    #     container_client.get_blob_client(blob.name).download_blob().readall()
    # )
    with open("large_source.txt", "r") as f:
        original_file = f.read()
        f.close()

    performers_data = {"columns": [], "rows": []}

    # Pass the stored data inside the beautifulsoup parser
    bs_src = BeautifulSoup(original_file[1:-1], "xml")

    # Find body tag and decode on base-64
    body_encoded = bs_src.find("Body").get_text()
    mrn = bs_src.find("SubscriberMRN").get_text()
    body_decoded = base64.b64decode(body_encoded)

    body_decoded = BeautifulSoup(body_decoded, "xml")
    parent = body_decoded

    result_dict = {}

    for table_arr in tables:
        data = retrieve_data_from_table(table_arr[1], parent, mrn)
        if data != -1:
            result_dict[table_arr[0]] = data

    patient_info = get_patient_info(parent, parts, mrn)
    get_performer(parent, performers_data, mrn)

    retrieve_data_from_results(
        "Relevant diagnostic tests and/or laboratory data", parent, mrn
    )

    # with open("decoded.txt", "wb") as g:
    #     g.write(body_decoded)
    #     g.close()

    # with pyodbc.connect(
    #     "DRIVER="
    #     + driver
    #     + ";SERVER=tcp:"
    #     + server
    #     + ";PORT=1433;DATABASE="
    #     + database
    #     + ";UID="
    #     + username
    #     + ";PWD="
    #     + password
    # ) as conn:
    #     with conn.cursor() as cursor:
    #         cursor.fast_executemany = True
    #         # load patient info
    if len(patient_info["rows"]) != 0:
        df_p = pd.DataFrame(
            [patient_info["rows"]],
            columns=patient_info["columns"],
        ).reset_index(drop=True, inplace=False)

        cols_p = ",?" * (len(patient_info["columns"]))
        insert_table_p = (
            f"INSERT INTO dbo.[Test_CCDA_Patient_Info] VALUES ({cols_p[1:]})"
        )
        # cursor.executemany(insert_table_p, df_p.values.tolist())

    # load doctors
    if len(performers_data["rows"]) != 0:
        df_p = pd.DataFrame(
            performers_data["rows"],
            columns=performers_data["columns"],
        ).reset_index(drop=True, inplace=False)

        cols_p = ",?" * (len(performers_data["columns"]))
        insert_table_p = f"INSERT INTO dbo.[Test_CCDA_Doctors] VALUES ({cols_p[1:]})"
        # cursor.executemany(insert_table_p, df_p.values.tolist())

    # load tables
    # for i in result_dict:
    #     if len(result_dict[i]["rows"]) == 0:
    #         continue
    #     df = pd.DataFrame(
    #         result_dict[i]["rows"],
    #         columns=result_dict[i]["columns"],
    #     ).reset_index(drop=True, inplace=False)

    #     cols = ",?" * (len(result_dict[i]["columns"]))
    #     insert_table = f"INSERT INTO dbo.{i} VALUES ({cols[1:]})"
    # cursor.executemany(insert_table, df.values.tolist())

    # move_files(src_container, arch_container, blob.name)


main()
