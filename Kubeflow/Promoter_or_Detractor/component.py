from kfp.components import InputPath , OutputPath


def prepare_keywords(agg_metric_data_path: InputPath('JsonObject') , promotor_dectractor_transformed_path : OutputPath('JsonObject') ):
    import json
    from json import load
    import pandas as pd

    null = "null"
    # load text data
    with open(agg_metric_data_path, 'r') as file:
        promotor_dectractor_data = load(file)
    file.close()
    data=promotor_dectractor_data
    # Create an empty list to store the data
    output_data = []
    if data != []:
        for obj in data:
            meeting_id = obj['object']['meeting_id']
            deal_ids = obj['object']['deal_ids']
            # Iterate over each speaker in the user map
            for speaker, speaker_data in obj['object']['user_map'].items():
                speaker_name = speaker_data['name']
                is_buyer = []
                if "is_buyer" in list(speaker_data.keys()):
                    is_buyer.append(speaker_data['is_buyer'])
                else:
                    is_buyer.append(None)
                speaker_sentiment = obj['object']['speaker_sentiment_time_count'][speaker]
                positive = speaker_sentiment['POSITIVE']
                negative = speaker_sentiment['NEGATIVE']
                neutral = speaker_sentiment['NEUTRAL']
                score = (positive - negative) / (positive + negative + neutral) if positive + negative + neutral > 0 else 0
                if score >= 0.5 :
                    Promotor_or_Detractor="Promoter"
                elif score<0.5:
                    Promotor_or_Detractor="Detractor"
                # Append the speaker data to the output list
                output_data.append({
                    'meeting_id': meeting_id,
                    'deal_id': deal_ids,
                    'speaker_tag': speaker,
                    'speaker_sentiment_score': score,
                    'speaker_name': speaker_name,
                    'promoter_or_detractor':Promotor_or_Detractor,
                    'is_buyer': str(is_buyer[0])
                })
        # Create a DataFrame from the output data
        df = pd.DataFrame(output_data, columns=['meeting_id', 'deal_id', 'speaker_tag', 'speaker_sentiment_score', 'speaker_name',"promoter_or_detractor","is_buyer"])
        df["deal_id"] = df['deal_id'].str[0]
        print("Number of records :",df.shape[0])
        print("Number of meeting_ids :",df["meeting_id"].count())
        print("Number of deal_ids :",df["deal_id"].count())
        print("Number of unique meeting_ids and deal_ids :",df["meeting_id"].nunique(),df["deal_id"].nunique())
        print("Number of aspect ",df["speaker_tag"].count())
        print("Number of aspect ", df["is_buyer"].count())

    else:
        print("Empty Data")
        df=pd.DataFrame()
    json_str = df.to_json(orient='records')
    json_str=json.loads(json_str)
    json.dump(json_str, open(promotor_dectractor_transformed_path, 'w'))


if __name__ == '__main__':
    from kfp.components import create_component_from_func

    create_component_from_func(prepare_keywords, output_component_file='component.yaml',
                               base_image='python:3.11', packages_to_install=['pandas'])