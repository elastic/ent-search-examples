# wpsearch_workshop

wpsearch.py is a helper script to load some sample data into elastic work place search that's intended to be used for workshops.

requirements: python 3.8, elastic_workplace_search python client

usage: wpsearch.py [-h] [-p] {employees,trello,issues}

- Create an enterprise search deployment in cloud.elastic.co
- Create 3 custom api sources named employees, issues and trello
- Clone repo
- **Copy endpoint, access_token and custom sources id for individual sources and paste into sections in config.yaml**
- Install Requirements

`python -m pip install -r requirements.txt`
- Load data one source at a time using the script

`python wpsearch.py employees`


`python wpsearch.py issues`


`python wpsearch.py trello`

- When done, optionally purge data

`python wpsearch.py employees -p`


`python wpsearch.py issues -p`


`python wpsearch.py trello -p`



