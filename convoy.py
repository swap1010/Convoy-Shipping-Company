import pandas as pd
import re
import sqlite3
import json
file_name = input("Input file name\n")
if ".xlsx" in file_name and ".s3db" not in file_name:
    df = pd.read_excel(file_name, sheet_name='Vehicles',dtype=str)
    csv_file = file_name.split(".")[0] + ".csv"
    df.to_csv(csv_file, index=None, header=True)
    row, col = df.shape
    char = " was"
    if row > 1:
        char = "s were"
    print(f"{row} line{char} added to {csv_file}")
elif ".s3db" not in file_name:
    df = pd.read_csv(file_name)
if "[CHECKED]" not in file_name and ".s3db" not in file_name:
    row, col = df.shape
    count = 0
    for i in range(row):
        for j in range(col):
            if not isinstance(df.iloc[i, j], int):
                digit = re.findall(r'\d+', df.iloc[i, j])[0]
                if df.iloc[i, j] != digit:
                    count += 1
                    df.iloc[i, j] = digit
    csv_file = file_name.split(".")[0] + "[CHECKED]" + ".csv"
    df.to_csv(csv_file, index=None, header=True)
    char = " was"
    if count > 1:
        char = "s were"
    print(f"{count} cell{char} corrected in {csv_file}")
if ".s3db" not in file_name:
    if "[CHECKED]" in file_name:
        db = f'{file_name.split("[CHECKED]")[0]}.s3db'
        file = file_name
        js_file = f'{file_name.split("[CHECKED]")[0]}.json'
    else:
        db = f'{file_name.split(".")[0]}.s3db'
        file = file_name.split(".")[0] + "[CHECKED]" + ".csv"
        js_file = f'{file_name.split(".")[0]}.json'
    conn = sqlite3.connect(db)
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS convoy(vehicle_id int primary key , engine_capacity int not null, fuel_consumption int not null, maximum_load int not null,score int not null ); ")
    sql_df = pd.read_csv(file)
    records = len(list(df.itertuples()))
    for row in df.itertuples():
        scoring = 0
        distance = 450 / 100
        score1 = (distance * int(row.fuel_consumption)) / int(row.engine_capacity)
        s1 = 0 if score1 > 2 else 1 if score1 >= 1 else 2
        s2 = 2 if distance * int(row.fuel_consumption) <= 230 else 1
        s3 = 2 if int(row.maximum_load) >= 20 else 0
        scoring = s1 + s2 + s3
        cur.execute('''
                    INSERT INTO convoy (vehicle_id, engine_capacity, fuel_consumption, maximum_load,score)
                    VALUES (?,?,?,?,?)
                    ''',
                    (row.vehicle_id, row.engine_capacity, row.fuel_consumption, row.maximum_load,scoring)
                    )
        conn.commit()
    char = " was"
    if records > 1:
        char = "s were"
    print(f"{records} record{char} inserted into {db}")
    conn.close()

if ".s3db" in file_name:
    db = file_name
    js_file = file_name.split("s3db")[0] + 'json'

conn = sqlite3.connect(db)
json_df = pd.read_sql_query("select * from convoy ;", conn)
xml_df = json_df[json_df["score"] <= 3]
json_df = json_df[json_df["score"] > 3]
json_df.drop(["score"], inplace=True, axis=1)
xml_df.drop(["score"], inplace=True, axis=1)
records = len(list(json_df.itertuples()))
char = " was"
if records > 1 or records == 0:
    char = "s were"
result = json_df.to_json(orient="records")
vehicle_dict = {"convoy": json.loads(result)}
with open(js_file, "w") as json_file:
    json.dump(vehicle_dict, json_file)
xml = "<convoy>"
print(f"{records} vehicle{char} saved into {js_file}")
result = xml_df.to_json(orient="records")
for vehicle in json.loads(result):
    xml += "<vehicle>"
    for k, v in vehicle.items():
        xml += f"<{k}>{v}</{k}>"
    xml += "</vehicle>"
xml += "</convoy>"
with open(js_file.split(".")[0]+".xml", "w") as xml_file:
    xml_file.write(xml)
records = len(list(xml_df.itertuples()))
char = " was"
if records > 1 or records == 0:
    char = "s were"
print(f"{records} vehicle{char} saved into {js_file.split('.')[0]}.xml")