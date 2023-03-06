from kfp.components import InputPath , OutputPath


def query_maker(table_details: dict ,greenplum_details: dict,table_query_trans:dict,table_query_agg:dict,transformed_query_trans: OutputPath('JsonObject'),transformed_query_agg: OutputPath('JsonObject')):
    import time
    import json
    from json import load
    import pandas as pd
    import psycopg2

    print("correct pipeline")
    conn = psycopg2.connect(
        host=greenplum_details["host"],
        database=greenplum_details["database"],
        user=greenplum_details["user_name"],
        password=greenplum_details["password"],
        port=greenplum_details["port"]
    )

    conn.autocommit = True
    cur = conn.cursor()
    table_name=table_details["table_name"]
    schema=table_details["schema"]
    column_name = schema.strip('()').split(' ')[0]
    schema_type= schema.strip('()').split(' ')[1]
    print(schema,column_name)
    try:
        cur.execute('''
             CREATE TABLE {} ({} {});
         '''.format(table_name,column_name,schema_type))

        print(table_name,column_name,schema)
        cur.execute('''INSERT INTO {} ({}) VALUES (0);'''.format(table_name,column_name))
        print("success")
    except:
        print("Table Exist")

    cur.execute("SELECT {} FROM {}".format(column_name,table_name))
    rows = cur.fetchall()
    for row in rows:
        print(row)
        value = int(row[0])  # or int(output[0])

    if table_details["fetch"] == "All":
        last_modified= 0
    else:
        last_modified = value

    print(last_modified)
    table_query_trans["query"] = {column_name: {"$gt":0}}
    table_query_agg["query"] = {column_name: {"$gt":0}}
    json.dump(table_query_trans, open(transformed_query_trans, 'w'))
    json.dump(table_query_agg, open(transformed_query_agg, 'w'))

if __name__ == '__main__':
    from kfp.components import create_component_from_func

    create_component_from_func(query_maker, output_component_file='component.yaml',
                               base_image='python:3.11', packages_to_install=['pandas','psycopg2'])



