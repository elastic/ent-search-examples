require 'csv'
require 'json'
require 'elastic-enterprise-search'

endpoint = "[ENT_SEARCH_ENDPOINT]"
username = "elastic"
password = "[PASSWORD]"
user="[USER]"

#Connecting to an instance of Enterprise Search
ent_client = Elastic::EnterpriseSearch::Client.new(host: endpoint)
workplace_search = ent_client.workplace_search(http_auth: {user: username, password: password})

#Create a new Custom Api Source and save the Custom Api Source id of it
content_source_id = workplace_search.create_content_source(
    body: {
        "name": "blogs",
        #Define schema for the Custom Api Source
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
        #Define the display settings Custom Api Source       
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
        "is_searchable": true
    }
).body['id']

#Open the CSV file, transform it to json in a list of docs
rows = []
CSV.foreach('blogs.csv',:headers => true, :header_converters => :symbol, :converters => :all, col_sep: ";", quote_char: nil) do |row|  
  #Add permissions to the documents
  if row[:category] == "Engineering" then
    row[:_allow_permissions] = ["engineering_group"]
  else
    row[:_deny_permissions] = ["engineering_group"]
  end    
  rows << row.to_hash
end

#Divide the rows from the csv in arrays of 100 - which is the maximum size of elements for a batch request in Workplace Search
workplace_search.add_user_permissions(
  content_source_id,
  { 
    permissions: ['engineering_group'], 
    user: user 
  }
)

#Add the permission to the user
rows.each_slice(100) do |documents|
  #Index the data in batches of 100 in Workplace Search
  workplace_search.index_documents(
    content_source_id, 
    documents: documents.to_json
  )  
end