from elastic_enterprise_search import WorkplaceSearch
import csv
import json
import datetime
import requests
 
 
endpoint = "[ENT_SEARCH_ENDPOINT]"
username = "elastic"
password = "[PASSWORD]"
user="[USER]"

#Connecting to an instance of Enterprise Search
workplace_search = WorkplaceSearch(endpoint, http_auth=(username,password))

#Create a new Custom Api Source and save the Custom Api Source id of it
content_source_id = workplace_search.create_content_source(
    body={
        "name": "blogs",
        #Define schema for the Custom Api Source.
        "schema": {
            "author":"text",
            "title":"text",
            "seo_title":"text",
            "url":"text",
            "content":"text",
            "locales":"text",
            "category":"text",
            "publish_date":"date"
        },
        #Define the display settings Custom Api Source.
        "display": { 
            #Set the fields for search results            
            "title_field": "title",
            "subtitle_field": "category",
            "description_field": "content",
            "url_field": "url",
            #Set the result details
            "detail_fields": [
            {
                "field_name": "author",
                "label": "Author"
            },
            {
                "field_name": "title",
                "label": "Title"
            },
            {
                "field_name": "seo_title",
                "label": "SEO Title"
            },
            {
                "field_name": "url",
                "label": "URL"
            },
            {
                "field_name": "content",
                "label": "Content"
            },
            {
                "field_name": "locales",
                "label": "Locales"
            },
            {
                "field_name": "category",
                "label": "Category"
            },
            {
                "field_name": "publish_date",
                "label": "Publish Date"
            }
            ],
             #Set orange as a color for this source
            "color": "#E7664C"
        },          
        "is_searchable": True
    }
)['id']

#Open the CSV file, transform it to json in a list of docs
with open('blogs.csv') as f:
   reader = csv.DictReader(f,delimiter=';')
   list_docs = list(reader)

#Divide the rows from the csv in arrays of 100 - which is the maximum size of elements for a batch request in Workplace Search.
batches = [[]]
for document in list_docs:
    # Current batch is full (100 documents) create a new one
    if len(batches[-1]) >= 100:
        batches.append([])
    #Add permissions to the documents
    if document['category'] == "Engineering":
        document['_allow_permissions'] = ["engineering_group"]
    else:
        document['_deny_permissions'] = ["engineering_group"]    
    batches[-1].append(document)
 

#Add the permission to the user
workplace_search.add_user_permissions(
    content_source_id=content_source_id,
    user=user,
    body={
        "permissions": ["engineering_group"]
    }
)

#Index the data in batches of 100 in Workplace Search
for batch_100 in batches:
    workplace_search.index_documents(
        content_source_id=content_source_id,
        documents=batch_100
    )