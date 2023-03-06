def update_tenant_db(config, tenant_details, etl_replace='etl'):
    updated_config = config.copy()
    if updated_config['db_in'] in ['postgres_fm', 'postgres_ms']:
        updated_config['tenant_name'] = tenant_details['tenant']
        if updated_config['db_in'] == 'postgres_ms':
            updated_config['db_name'] = updated_config['db_name'].replace('stack', tenant_details['stacks']['relationships'])
            updated_config['db_name'] = updated_config['db_name'].replace('-', '_')
    elif updated_config['db_in'] == 'etl':
        updated_config['db_name'] = updated_config['db_name'].replace('tenant',
                                                                      tenant_details['tenant'].replace('.com', ''))
        updated_config['db_name'] = updated_config['db_name'].replace('stack', tenant_details['stacks'][etl_replace])
        updated_config['collection_name'] = updated_config['collection_name'].replace('tenant',
                                                                                      tenant_details['tenant'])
    elif updated_config['db_in'] == 'gbm':
        updated_config['db_name'] = updated_config['db_name'].replace('tenant',
                                                                      tenant_details['tenant'].replace('.com', ''))
        updated_config['db_name'] = updated_config['db_name'].replace('stack', tenant_details['stacks']['gbm'])
        updated_config['collection_name'] = updated_config['collection_name'].replace('tenant',
                                                                                      tenant_details['tenant'])
    elif updated_config['db_in'] == 'fm_app':
        updated_config['db_name'] = updated_config['db_name'].replace('tenant',
                                                                      tenant_details['tenant'].replace('.com', ''))
        updated_config['db_name'] = updated_config['db_name'].replace('stack', tenant_details['stacks']['fm_app'])

    return updated_config


def manage_runs(kf_endpoint='local', recurring_run=False):
    from pprint import pprint
    from configs.load_configs import tenants, secrets, pipeline_config
    from configs.utils import query_builder
    from macheye_pipeline import macheye_pipeline
    from kfp import Client
    from time import sleep

    if kf_endpoint == 'prod':
        secrets_ = secrets['prod']
        tenants_ = tenants['prod']
    else:
        secrets_ = secrets['dev']
        tenants_ = tenants['dev']

    if kf_endpoint == 'local':
        host = "http://localhost:8080"
        client = Client(host)
    elif kf_endpoint in ['dev', 'prod']:
        alb_session_cookie0 = secrets_['kubeflow']['alb_session_cookie0']
        alb_session_cookie1 = secrets_['kubeflow']['alb_session_cookie1']
        host = f"https://{secrets_['kubeflow']['endpoint']}/pipeline"

        client = Client(host=host,
                        cookies=f"AWSELBAuthSessionCookie-0={alb_session_cookie0}; "
                                f"AWSELBAuthSessionCookie-1={alb_session_cookie1}",
                        namespace=secrets_['kubeflow']['namespace'])
    else:
        raise ValueError('not a valid kf_endpoint')

    print(f'submmitting run on {host}')

    for _, tenant_details in tenants_.items():
        tenant_name = tenant_details['tenant']
        stacks = tenant_details['stacks']

        arguments = dict()

        opps_query_config = {**secrets_['etl'],
                             **update_tenant_db(pipeline_config['fetch_data']['Opportunity'], tenant_details)}
        Transcription_query_config = {**secrets_['etl'],
                             **update_tenant_db(pipeline_config['fetch_data']['Transcription'], tenant_details)}
        agg_metric_query_config = {**secrets_['etl'],
                             **update_tenant_db(pipeline_config['fetch_data']['aggregated_metrics_ci'], tenant_details)}

        arguments['Transcription_query'] = Transcription_query_config
        arguments["agg_metric_query"] = agg_metric_query_config
        arguments["table_details_promotor"]= pipeline_config["fetch_table_details"]["promotor_dectractor_table"]
        arguments["table_details_keywords"] = pipeline_config["fetch_table_details"]["keywords_table"]
        arguments["table_details_topics"] = pipeline_config["fetch_table_details"]["topics_table"]
        arguments["table_details_sentiment"] = pipeline_config["fetch_table_details"]["sentiment_table"]
        arguments["table_details_aspect"] = pipeline_config["fetch_table_details"]["aspect_table"]
        arguments["table_details_last_modified"] = pipeline_config["fetch_table_details"]["details_table"]
        arguments["greenplum_details"]= secrets["greenplum_write_dev"]
        # setting kubeflow run name
        run_name = 'macheye' + tenant_name


        if recurring_run:
            # resp_id = client.get_pipeline_id("macheye_pipeline1")
            # print(resp_id)
            resp_id=None
            if resp_id is None:
                upload_pi_resp = client.upload_pipeline('macheye_pipeline.yaml', 'macheye_pipeline2')
                print('upload_pi_resp:', upload_pi_resp)
                resp_id = upload_pi_resp.id
            experiment_resp = client.create_experiment(name="macheye_pipeline_recurring")
            print('experiment_resp:', experiment_resp)
            pi_versions = client.list_pipeline_versions(resp_id)
            print('pi_versions:', pi_versions)

            # This job starts everyday at 11:30 IST / 06:00 GMT.
            # Refer https://docs.oracle.com/cd/E12058_01/doc/doc.1014/e12030/cron_expressions.htm
            client.create_recurring_run(experiment_id=experiment_resp.id,
                                        job_name=run_name,
                                        cron_expression='0 * * * *',
                                        pipeline_id=resp_id,
                                        version_id=pi_versions.versions[0].id,
                                        params=arguments)
        else:
            client.create_run_from_pipeline_func(macheye_pipeline, arguments=arguments,
                                               experiment_name='macheye_recurring_run', run_name=run_name)

    sleep(20)


if __name__ == '__main__':
    manage_runs('dev',False)