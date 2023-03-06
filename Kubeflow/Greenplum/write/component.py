from kfp.components import InputPath , OutputPath
from typing import Optional
def write_data(greenplum_details: dict,table_details: dict,greenplum_input_data_path: Optional["JsonObject"] = None ):
    import psycopg2
    import json
    from json import load
    import time
    # load text data
    print(greenplum_input_data_path)
    if greenplum_input_data_path != None and greenplum_input_data_path != [] :
        data = greenplum_input_data_path
    elif greenplum_input_data_path == None and greenplum_input_data_path != []:
        print(greenplum_input_data_path)
        print("writing last modified time to db")
        epoch_ts=int(time.time() * 1000)
        data = [{table_details["schema"].split(" ")[0][1:]: epoch_ts}]
    elif greenplum_input_data_path == []:
        data=greenplum_input_data_path

    if data != []:
        # Connect to Greenplum
        conn = psycopg2.connect(
            host=greenplum_details["host"],
            database=greenplum_details["database"],
            user=greenplum_details["user_name"],
            password=greenplum_details["password"],
            port=greenplum_details["port"]
        )
        # Set autocommit to True to start a new transaction
        conn.autocommit = True
        cur = conn.cursor()

        mode=table_details["mode"]
        table_name=table_details["table_name"]
        schema=table_details["schema"]
        primary_key=table_details["primary_key"].split(',')

        # def bulk_load(data, table_name, schema, cur):
        if mode == "bulk_mode":
            cur.execute(f"DROP TABLE IF EXISTS {table_name}")
            cur.execute(f"""
                    CREATE TABLE {table_name} {schema};""")

            for d in data:
                keys=tuple(d.keys())
                keys = str(keys)
                value = tuple(d.values())
                value_format = []
                for cou in range(len(value)):
                    value_format.append('%s')

                keys = keys.replace("'", "")
                value_format = str(tuple(value_format))
                value_format = value_format.replace("'", "")
                print(keys,value_format,table_name)
                if len(tuple(d.keys())) == 1 :
                    keys=keys.replace(",","")
                    value_format=value_format.replace(",","")
                print(keys, value_format, table_name)
                cur.execute(f"""
                    INSERT INTO {table_name} {keys}
                    VALUES {value_format};
                """, value)
            conn.commit()  # commit the transaction
            conn.close()  # Close the connection
        else:
            try:
                cur.execute(f"""
                        CREATE TABLE {table_name} {schema};""")
            except:
                pass
            for d in data:
                pk_values = tuple(d[col] for col in primary_key)
                try:
                    cur.execute(
                        f"SELECT EXISTS (SELECT 1 FROM {table_name} WHERE {' AND '.join([f'{col}=%s' for col in primary_key])})",
                        pk_values)
                    exists = cur.fetchone()[0]
                    if exists:
                        # print(f"Record with primary key {pk_values} already exists, updating...")
                        # Update the record
                        update_cols = [f"{col}=%s" for col in d.keys() if col not in primary_key]
                        update_values = tuple(d[col] for col in d.keys() if col not in primary_key)
                        update_query = f"UPDATE {table_name} SET {', '.join(update_cols)} WHERE {' AND '.join([f'{col}=%s' for col in primary_key])}"
                        cur.execute(update_query, update_values + pk_values)
                    else:
                        # print(f"Record with primary key {pk_values} does not exist, inserting...")
                        # Insert the record
                        insert_cols = list(primary_key) + [col for col in d.keys() if col not in primary_key]
                        insert_values = tuple(d[col] for col in insert_cols)
                        insert_query = f"INSERT INTO {table_name} ({', '.join(insert_cols)}) VALUES ({', '.join(['%s' for _ in insert_cols])})"
                        cur.execute(insert_query, insert_values)
                    conn.commit()
                except Exception as e:
                    conn.rollback()

            conn.commit()
            conn.close()
    else:
        print("Data fetched is empty. So no records written to data base")


if __name__ == '__main__':
    from kfp.components import create_component_from_func

    create_component_from_func(write_data, output_component_file='component.yaml',
                               base_image='python:3.11', packages_to_install=['psycopg2'])