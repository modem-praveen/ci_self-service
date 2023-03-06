from kfp.components import InputPath , OutputPath

def prepare_aspects(Transcription_data_path: InputPath('JsonObject') , aspect_transformed_path : OutputPath('JsonObject') ):
    import json
    from json import load
    import pandas as pd

    null = "null"
    with open(Transcription_data_path, 'r') as file:
        aspect_data = load(file)
    file.close()
    data=aspect_data

    if data != []:
        rows = []
        for obj in data:
            meeting_id = obj["object"]["meeting_id"]
            deal_ids = obj["object"]["deal_ids"]
            positive_aspects = []
            negative_aspects = []
            if "positive_aspects" in obj["object"]:
                if obj["object"]["positive_aspects"] == "null" or obj["object"]["positive_aspects"] == None:
                    word_positions = None
                    word_count = None
                    aspect = None
                else:
                    for aspect, values in obj["object"]["positive_aspects"].items():
                        word_positions = []
                        word_count = 0
                        for value in values:
                            word_positions += value["wordPositions"]
                            word_count += len(value["wordPositions"])
                        positive_aspects.append({"aspect": aspect, "word_count": word_count, "word_positions": word_positions})
            if "negative_aspects" in obj["object"]:
                if obj["object"]["negative_aspects"] == "null" or obj["object"]["negative_aspects"] == None:
                    word_positions = None
                    word_count = None
                    aspect = None
                else:
                    for aspect, values in obj["object"]["negative_aspects"].items():
                        # loop through each value and extract the word positions and word count
                        word_positions = []
                        word_count = 0
                        for value in values:
                            word_positions += value["wordPositions"]
                            word_count += len(value["wordPositions"])
                        negative_aspects.append(
                            {"aspect": aspect, "word_count": word_count, "word_positions": word_positions})
            if not positive_aspects and not negative_aspects:
                rows.append({"meeting_id": meeting_id, "deal_id": deal_ids, "aspect": None, "aspect_type": None,
                             "aspect_count": None})
            for aspect_dict in positive_aspects:
                rows.append({"meeting_id": meeting_id, "deal_id": deal_ids, "aspect": aspect_dict["aspect"],
                             "aspect_type": "Positive", "aspect_count": aspect_dict["word_count"]})
            for aspect_dict in negative_aspects:
                rows.append({"meeting_id": meeting_id, "deal_id": deal_ids, "aspect": aspect_dict["aspect"],
                             "aspect_type": "Negative", "aspect_count": aspect_dict["word_count"]})

        df = pd.DataFrame(rows)
        df["deal_id"] = df['deal_id'].str[0]
        print("Number of records :",df.shape[0])
        print("Number of meeting_ids :",df["meeting_id"].count())
        print("Number of deal_ids :",df["deal_id"].count())
        print("Number of unique meeting_ids and deal_ids :",df["meeting_id"].nunique(),df["deal_id"].nunique())
        print("Number of aspect ",df["aspect"].count())
        print("Number of aspect ", df["aspect_type"].value_counts())
    else:
        print("Empty Data")
        df=pd.DataFrame()

    json_str = df.to_json(orient='records')
    json_str=json.loads(json_str)
    json.dump(json_str, open(aspect_transformed_path, 'w'))


if __name__ == '__main__':
    from kfp.components import create_component_from_func

    create_component_from_func(prepare_aspects, output_component_file='component.yaml',
                               base_image='python:3.11', packages_to_install=['pandas'])