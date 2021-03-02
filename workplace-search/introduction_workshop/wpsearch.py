import argparse
import json
import yaml
from elastic_enterprise_search import WorkplaceSearch


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('source', help='source name',
                        choices={'employees','issues','trello'})
    parser.add_argument('-p', '--purge', action='store_true',
                        help='purge all data for the source')
    return parser.parse_args()

def get_config(filename='config.yaml'):
    with open(filename) as f:
        return yaml.safe_load(f)

def get_data(filename):
    with open(filename) as f:
        return [json.loads(line) for line in f]

def upload_data():
    data = get_data(source['file'])    
    client.index_documents(content_source_id=id,documents=data)

def purge_data():
    data = get_data(source['file'])
    ids = [str(i) for i in range(1,len(data)+1)]
    client.delete_documents(id, ids)

args = parse_args()
config = get_config()
deployment =config['deployment']
source = get_config()[args.source]
id = source['id']
base_url = deployment['endpoint']
client = WorkplaceSearch(base_url, http_auth=deployment['access_token'])
if args.purge:
    purge_data()
else:
    upload_data()



