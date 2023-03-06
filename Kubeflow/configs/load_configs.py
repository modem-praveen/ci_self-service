from json import load
from .utils import query_builder

tenants_config_file = open('configs/tenants.json')
tenants = load(tenants_config_file)
tenants_config_file.close()
del tenants_config_file


secrets_config_file1 = open('configs/secrets.json')
secrets = load(secrets_config_file1)
secrets_config_file1.close()
del secrets_config_file1

pipeline_config_file = open('configs/pipeline.json')
pipeline_config = load(pipeline_config_file)
pipeline_config_file.close()
del pipeline_config_file

kf_utils_config_file = open('configs/kf_utils.json')
kf_utils_config = load(kf_utils_config_file)
kf_utils_config_file.close()
del kf_utils_config_file

#updating github token in utils components
for key in kf_utils_config.keys():
    for subkey in kf_utils_config[key].keys():
        kf_utils_config[key][subkey] = kf_utils_config[key][subkey].replace('https://', 'https://x-access-token:' +
                                                                            secrets['github_token']+'@')
