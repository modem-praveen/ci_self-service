def query_builder(config):
    # raise errors given the following conditions
    fields_to_fetch = config['fields_to_fetch']
    config_table = config['config_table']
    category = config['category']
    config_name = config['config_name']
    tenant_name = config['tenant_name']
    if not config_table:
        raise Exception('please define the table name to get the config from')
    if not category:
        raise Exception('please define category of config required')
    if not config_name:
        raise Exception('please define the configs to fetch')
    if not tenant_name or not isinstance(tenant_name, (str, int, float)):
        raise Exception('tenant_name is not defined correctly')
    # fields to fetch
    if fields_to_fetch:
        fields_to_fetch = ",".join(fields_to_fetch)
    else:
        fields_to_fetch = "*"
    # category
    if isinstance(category, list):
        category = ",".join(["'" + cat + "'" for cat in category])
    elif isinstance(category, str):
        category = "'" + category + "'"
    else:
        raise Exception('please pass category field as either a list or a string')
    # config_name
    if isinstance(config_name, list):
        config_name = ",".join([ "'" + cat + "'" for cat in config_name ])
    elif isinstance(config_name, str):
        config_name = "'" + config_name + "'"
    else:
        raise Exception('please pass config_name field as either a list or a string')
    # create the query
    query = "SELECT {} FROM {} WHERE category IN ({}) AND config_name in ({}) AND tenant_id IN (SELECT _id FROM tenant WHERE name='{}');".format(
        fields_to_fetch,
        config_table,
        category,
        config_name,
        tenant_name
    )
    return query
