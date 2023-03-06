from kfp.components import InputPath , OutputPath

def prepare_sentiment(Transcription_data_path: InputPath('JsonObject') , sentiment_transformed_path : OutputPath('JsonObject') ):
    import json
    from json import load
    import pandas as pd
    import datetime
    import math

    null = "null"
    # load text data
    with open(Transcription_data_path, 'r') as file:
        sentiment_data = load(file)
    file.close()
    data = sentiment_data

    df = pd.DataFrame(columns=["meeting_id", "deal_id", "meeting_date", "sentiment_score", "sentiment_sategory"])

    if data != []:
        for meeting in data:
            meeting_id = meeting["object"]["meeting_id"]
            deal_id = meeting["object"]["deal_ids"]
            date = meeting["object"]["meeting_date"]
            try:
                sentiment_score = meeting["object"]["buyer_call_sentiment"]
                if sentiment_score is not None:
                    if sentiment_score >= 0.6:
                        sentiment_category = 'Strongly Positive'
                    elif 0.2 <= sentiment_score < 0.6:
                        sentiment_category = 'Positive'
                    elif -0.2 < sentiment_score < 0.2:
                        sentiment_category = 'Neutral'
                    elif -0.6 < sentiment_score <= -0.2:
                        sentiment_category = 'Negative'
                    else:
                        sentiment_category = 'Strongly Negative'
                else:
                    sentiment_category = None
            except:
                sentiment_score = None
                sentiment_category = None
            df = df.append({"meeting_id": meeting_id,
                            "deal_id": deal_id,
                            "meeting_date": date,
                            "sentiment_score": sentiment_score,
                            "sentiment_category": sentiment_category
                            }, ignore_index=True)

        # define lambda function to convert Unix timestamp to date string
        unix_to_date = lambda x: '' if math.isnan(x) else datetime.datetime.utcfromtimestamp(x).strftime('%d-%m-%Y')
        df["meeting_date"] = pd.to_datetime(df["meeting_date"], unit='ms', errors='coerce')
        df["deal_id"] = df['deal_id'].str[0]

        print("Number of records :",df.shape[0])
        print("Number of meeting_ids :",df["meeting_id"].count())
        print("Number of deal_ids :",df["deal_id"].count())
        print("Number of unique meeting_ids and deal_id :",df["meeting_id"].nunique(),df["deal_id"].nunique())
        print("Number of Date ",df["meeting_date"].count())
    else:
        print("Empty Data")

    json_str = df.to_json(orient='records')
    json_str=json.loads(json_str)
    json.dump(json_str, open(sentiment_transformed_path, 'w'))

if __name__ == '__main__':
    from kfp.components import create_component_from_func

    create_component_from_func(prepare_sentiment, output_component_file='component.yaml',
                               base_image='python:3.11', packages_to_install=['pandas'])