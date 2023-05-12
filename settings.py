import yaml

with open("config.yaml", 'r')as f:
    cf = yaml.safe_load(f)

ES_USERNAME = cf['es']['username']
ES_PASSWORD = cf['es']['password']
ES_HOST = cf['es']['host']
ES_PORT = cf['es']['port']

MASTER = cf['spark']['master']
APP_NAME = cf['spark']['appName']
CHECKPOINT_LOCATION = cf['spark']['checkpoint']
DATA_SOURCE = cf['spark']['dataSource']
OUTPUT_MODE = cf['spark']['outputMode']
ES_INDEX = cf['spark']['esIndex']
ES_DOC_TYPE = cf['spark']['esDocType']
