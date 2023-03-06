from kfp.components import InputPath , OutputPath

def prepare_keywords(Transcription_data_path: InputPath('JsonObject') , keyword_transformed_path : OutputPath('JsonObject') ):
    import json
    from json import load
    import pandas as pd

    null = "null"
    # load text data
    with open(Transcription_data_path, 'r') as file:
        opps_data = load(file)
    file.close()
    data=opps_data

    df = pd.DataFrame(columns=["meeting_id", "deal_id", "keyword", "keyword_count"])
    if data != []:
        for item in data:
            meeting_id = item["object"]["meeting_id"]
            deal_ids = item["object"]["deal_ids"]
            for keyword in item["object"]["keywords"]:
                count = 0
                for word_dict in item["object"]["keywords"][keyword]:
                    count = count + len(word_dict["wordPositions"])
                df = df.append({"meeting_id": meeting_id, "deal_id": deal_ids, "keyword": keyword, "keyword_count": count},
                               ignore_index=True)
        df["deal_id"] = df['deal_id'].str[0]

        print("Number of records :",df.shape[0])
        print("Number of meeting_ids :",df["meeting_id"].count())
        print("Number of deal_ids :",df["deal_id"].count())
        print("Number of unique meeting_ids and deal_ids :",df["meeting_id"].nunique(),df["deal_id"].nunique())
        print("Number of keywords ",df["keyword"].count())
    else:
        print("Empty Data")

    json_str = df.to_json(orient='records')
    json_str=json.loads(json_str)
    json.dump(json_str, open(keyword_transformed_path, 'w'))

if __name__ == '__main__':
    from kfp.components import create_component_from_func
    create_component_from_func(prepare_keywords, output_component_file='component.yaml',
                               base_image='python:3.11', packages_to_install=['pandas'])