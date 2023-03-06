from kfp.components import InputPath , OutputPath


def prepare_topics(Transcription_data_path: InputPath('JsonObject') , topic_transformed_path : OutputPath('JsonObject') ):
    import json
    from json import load
    import pandas as pd

    null="null"
    # load text data
    print('debug log')
    with open(Transcription_data_path, 'r') as file:
        data = load(file)
    file.close()
    # data=topic_data

    # Create an empty dataframe with the desired columns
    df = pd.DataFrame(columns=["meeting_id", "deal_id", "topic_name", "sub_topic_name", "sub_topic_count", "duration"])
    print("debug log")
    if data != []:
        for meeting in data:
            meeting_id = meeting["object"]["meeting_id"]
            deal_id = meeting["object"]["deal_ids"]
            if meeting["object"]["topics"] == "null" or meeting["object"]["topics"] == None:
                df = df.append({"meeting_id": meeting_id,
                                "deal_id": deal_id,
                                "topic_name": None,
                                "sub_topic_name": None,
                                "sub_topic_count": None,
                                "duration": None}, ignore_index=True)

            else:
                for topic_name, topic_data in meeting["object"]["topics"].items():
                    for sub_topic in topic_data["sub_topics"]:
                        sub_topic_name = sub_topic["sub_topic"]
                        sub_topic_count = sub_topic["sub_topic_rate"]
                        duration = sub_topic["sub_topic_duration"]
                        df = df.append({"meeting_id": meeting_id,
                                        "deal_id": deal_id,  # No deal ID is provided in the data
                                        "topic_name": topic_name,
                                        "sub_topic_name": sub_topic_name,
                                        "sub_topic_count": sub_topic_count,
                                        "duration": duration}, ignore_index=True)
        df["deal_id"]=df['deal_id'].str[0]

        print("Number of records :",df.shape[0])
        print("Number of meeting_ids :",df["meeting_id"].count())
        print("Number of deal_ids :",df["deal_id"].count())
        print("Number of unique meeting_ids and deal_ids :",df["meeting_id"].nunique(),df["deal_id"].nunique())
        print("Number of aspect ",df["topic_name"].count())
    else:
        print("Empty Data")

    json_str = df.to_json(orient='records')
    json_str=json.loads(json_str)
    json.dump(json_str, open(topic_transformed_path, 'w'))

if __name__ == '__main__':
    from kfp.components import create_component_from_func

    create_component_from_func(prepare_topics, output_component_file='component.yaml',
                               base_image='python:3.11', packages_to_install=['pandas'])