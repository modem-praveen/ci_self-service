from kfp import dsl
from kfp.components import load_component_from_file, load_component_from_url
from configs.load_configs import kf_utils_config
from kfp.dsl import ContainerOp
fetch_data_mongo = load_component_from_url(kf_utils_config['mongo']['fetch'])
keywords_transformed_data_load = load_component_from_file('Keywords/component.yaml')
topic_transformed_data_load = load_component_from_file('Topics/component.yaml')
sentiment_transformed_data_load = load_component_from_file('Sentiment/component.yaml')
aspect_transformed_data_load = load_component_from_file('Aspects/component.yaml')
promotor_dectractor_transformed_data_load = load_component_from_file('Promoter_or_Detractor/component.yaml')
greenplum_details_load = load_component_from_file('Greenplum/write/component.yaml')
query_maker_load = load_component_from_file('Query_Maker/component.yaml')

def macheye_pipeline(Transcription_query:dict,agg_metric_query : dict,table_details_promotor:dict,greenplum_details:dict,table_details_keywords:dict,table_details_topics:dict,table_details_sentiment:dict,table_details_aspect:dict,table_details_last_modified:dict ):

    import json

    query_maker = query_maker_load(table_details_last_modified,greenplum_details,Transcription_query,agg_metric_query).set_display_name(
        name='Modify_Config')
    query_maker.execution_options.caching_strategy.max_cache_staleness = "P0D"

    agg_metric_data=fetch_data_mongo(query_maker.outputs["transformed_query_agg"]).set_display_name('Fetching agg_metric')
    agg_metric_data.execution_options.caching_strategy.max_cache_staleness = "P0D"

    Transcription_data = fetch_data_mongo(query_maker.outputs["transformed_query_trans"]).set_display_name('Fetching Transcription')
    Transcription_data.execution_options.caching_strategy.max_cache_staleness = "P0D"

    keywords_transformed_data = keywords_transformed_data_load(Transcription_data.output).set_display_name(
                                                                name='Keywords_transformation')
    keywords_transformed_data.execution_options.caching_strategy.max_cache_staleness = "P0D"

    topic_transformed_data = topic_transformed_data_load(Transcription_data.output).set_display_name(
                                                                name='Topic_transformation')
    topic_transformed_data.execution_options.caching_strategy.max_cache_staleness = "P0D"


    sentiment_transformed_data = sentiment_transformed_data_load(Transcription_data.output).set_display_name(
                                                                name='Sentiment_transformation')

    sentiment_transformed_data.execution_options.caching_strategy.max_cache_staleness = "P0D"

    aspect_transformed_data = aspect_transformed_data_load(Transcription_data.output).set_display_name(
                                                                name='Aspects_transformation')
    aspect_transformed_data.execution_options.caching_strategy.max_cache_staleness = "P0D"


    promotor_dectractor_transformed_data = promotor_dectractor_transformed_data_load(agg_metric_data.output).set_display_name(
                                                                name='promoter_or_detractor_transformation').set_memory_request('8G').set_cpu_request('2')
    promotor_dectractor_transformed_data.execution_options.caching_strategy.max_cache_staleness = "P0D"

    promotor_dectractor_db = greenplum_details_load(greenplum_details,table_details_promotor,promotor_dectractor_transformed_data.output).set_display_name(
                                                                name='promoter_or_detractor_to_greenplum')
    promotor_dectractor_db.execution_options.caching_strategy.max_cache_staleness = "P0D"


    keywords_db= greenplum_details_load(greenplum_details,table_details_keywords,keywords_transformed_data.output).set_display_name(
                                                                name='keywords_to_greenplum')
    keywords_db.execution_options.caching_strategy.max_cache_staleness = "P0D"


    topics_db= greenplum_details_load(greenplum_details,table_details_topics,topic_transformed_data.output).set_display_name(
                                                                name='topics_to_greenplum')
    topics_db.execution_options.caching_strategy.max_cache_staleness = "P0D"
    print(topics_db.outputs)

    sentiment_db = greenplum_details_load(greenplum_details, table_details_sentiment,sentiment_transformed_data.output).set_display_name(name='sentiment_to_greenplum')
    sentiment_db.execution_options.caching_strategy.max_cache_staleness = "P0D"

    aspect_db = greenplum_details_load(greenplum_details, table_details_aspect,aspect_transformed_data.output).set_display_name(name='aspect_to_greenplum')
    aspect_db.execution_options.caching_strategy.max_cache_staleness = "P0D"

    last_modified = greenplum_details_load(greenplum_details, table_details_last_modified).set_display_name(
        name='last_modified_time_to_greenplum')
    last_modified.execution_options.caching_strategy.max_cache_staleness = "P0D"
    last_modified.after(topics_db, sentiment_db, topics_db, keywords_db, promotor_dectractor_db)

    # keywords_transformed_data.execution_options.caching_strategy.max_cache_staleness = "P0D"
# cleanup of all pods after run completion - 2 mins

    dsl.get_pipeline_conf().set_ttl_seconds_after_finished(120)
    dsl.get_pipeline_conf().set_image_pull_policy('IfNotPresent')
if __name__ == '__main__':
    from kfp.compiler import Compiler
    Compiler().compile(pipeline_func=macheye_pipeline, package_path='macheye_pipeline.yaml')