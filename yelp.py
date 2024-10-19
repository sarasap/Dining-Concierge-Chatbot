import requests
import json
import os
from decimal import Decimal,Context
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import boto3
from datetime import datetime
import os

os.environ['AWS_ACCESS_KEY_ID'] = ''
os.environ['AWS_SECRET_ACCESS_KEY'] = ''
def check_if_none(val):
    try:
        if val is None or len(str(val)) == 0:
            return True
        return False
    except:
        return True

def get_business_attributes(business, location, cuisine_type):
    attributes_dictionary={}
    attributes_dictionary['id']=business['id']
    attributes_dictionary['location']=location
    attributes_dictionary['cuisine_type']=cuisine_type
    attributes_dictionary['name']=business['name']
    attributes_dictionary['url']=business['url']
    if not check_if_none(business.get("rating",None)):
        attributes_dictionary["rating"] =  str(business["rating"])
    if not check_if_none(business.get("phone",None)):
        attributes_dictionary["contact"] = business["phone"]
    if not check_if_none(business.get("review_count",None)):
        attributes_dictionary["review_count"] = business["review_count"]
    if not check_if_none(business.get("price",None)):
        attributes_dictionary["price"] = business["price"]
    if business.get('location', None) is not None:
        temp=""
        for line in business['location']['display_address']:
            temp+=line
        attributes_dictionary['address']=temp
        attributes_dictionary["zip_code"]= business['location']['zip_code']
    if not check_if_none(business.get("coordinates",None)):
        attributes_dictionary['latitude']=str(business['coordinates']['latitude'])
        attributes_dictionary['longitude']=str(business['coordinates']['longitude'])
    attributes_dictionary['insertedAtTimestamp'] = datetime.now().isoformat()

    return attributes_dictionary


def scrape_yelp_data(api, api_key, cuisine_type, location):
    query= "?location={}".format(location)+"&categories={}".format(cuisine_type)+"&limit=50"
    yelp_api=api+query
    headers= {"Authorization": "Bearer " + api_key}
    #get all the responses
    response= requests.get(yelp_api, headers=headers).json()
    offset=0
    total_responses=response['total']
    businesses=[]
    #loop untill the you reach end of all the responses
    while(total_responses>=0):
        #but json has pages. So, loop through all the pages untill its none
        if response.get("businesses", None) is not None:
            response_businesses=response["businesses"]
            #loop through businesses in the current page
            responses_in_current_page=len(response_businesses)
            #for every business in the current page, get the attribute and put it in the business array
            for business in response_businesses:
                business_attributes=get_business_attributes(business, location, cuisine_type)
                businesses.append(business_attributes)
            #Decreased total responses by total responses parsed.
            total_responses-=responses_in_current_page
            #And increase the offset by number of businesses parsed
            offset+=responses_in_current_page
            #call the next page like this
            response=requests.get(yelp_api+query+str(offset), headers=headers).json()
        else:
            break
    return businesses

def put_data_to_open_search(response_restaurants, esClient):
    db = boto3.resource('dynamodb',region_name='us-east-1')
    table=db.Table('yelp-restaurants')
    total_restaurants=len(response_restaurants)
    batch_size=total_restaurants//10
    remaining_batches = batch_size
    start_index = -batch_size

    while remaining_batches!=0 :
        start_index = start_index+batch_size
        with table.batch_writer() as batch:
            for restaurant in response_restaurants[start_index:start_index+batch_size]:
                batch.put_item(Item=restaurant)
        for restaurant in response_restaurants[start_index:start_index + batch_size]:
            esClient.index(index='restaurant',id=restaurant["id"], body={
                "id" : restaurant["id"],
                "cuisine" : restaurant["cuisine_type"],
            })
        remaining_batches = remaining_batches-1


if __name__=='__main__':
    api_key = ''
    api='https://api.yelp.com/v3/businesses/search'
    service='es'
    credentials = boto3.Session(region_name='us-east-1', aws_access_key_id='', aws_secret_access_key='').get_credentials()
    awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, 'us-east-1', service)
    response_restaurants=scrape_yelp_data(api, api_key, "indpak", "manhattan")+scrape_yelp_data(api, api_key, "mexican", "manhattan")+scrape_yelp_data(api, api_key, "chinese", "manhattan")
    esClient=Elasticsearch([{'host': "search-diningopensearch-ct3nznfv55eeyexbdclrio5poq.us-east-1.es.amazonaws.com",'port':443}],
    use_ssl=True,
    verify_certs=True, 
    connection_class=RequestsHttpConnection,
    http_auth=awsauth)
    put_data_to_open_search(response_restaurants, esClient)
   
   

