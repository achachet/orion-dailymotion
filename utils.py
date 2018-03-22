from bs4 import BeautifulSoup
import requests
from dateutil.parser import parse
import time
import io
import gzip
import multiprocessing
import json
from google.cloud import bigtable
import hashlib

def gunzip_bytes_obj(bytes_obj):
    in_ = io.BytesIO()
    in_.write(bytes_obj)
    in_.seek(0)
    with gzip.GzipFile(fileobj=in_, mode='rb') as fo:
        gunzipped_bytes_obj = fo.read()

    return gunzipped_bytes_obj.decode()

def timestamp_str_to_unix(timestamp):
    return time.mktime(parse(timestamp).timetuple())

def video_data(xid):
    url = "https://api.dailymotion.com/video/%s?fields=allow_embed,audience,audience_total,channel,channel.created_time,channel.description,channel.id,channel.name,channel.slug,country,created_time,custom_classification,description,duration,embed_url,explicit,filmstrip_60_url,genres,geoblocking,geoloc,id,language,owner,owner.description,owner.facebook_url,owner.googleplus_url,owner.id,owner.instagram_url,owner.language,owner.linkedin_url,owner.parent,owner.pinterest_url,owner.screenname,owner.twitter_url,owner.username,owner.verified,owner.website_url,password_protected,private,soundtrack_isrc,tags,thumbnail_1080_url,thumbnail_120_url,thumbnail_240_url,thumbnail_480_url,thumbnail_60_url,tiny_url,title,url,verified"
    url = url % xid
    r = requests.get(url)
    return json.loads(r.text)



### Pub/Sub Utilities
#!pip install google-auth google-auth-httplib2 google-api-python-client
from google.cloud import pubsub_v1
from oauth2client.client import GoogleCredentials
import googleapiclient.discovery
from base64 import b64decode, b64encode


project_name      = 'orion-175415'
topic_name        = 'catalogs'
subscription_name = 'catalogs'
subscriber        = pubsub_v1.SubscriberClient()
publisher         = pubsub_v1.PublisherClient()
topic_path        = subscriber.topic_path(project_name, topic_name)
subscription_path = subscriber.subscription_path(project_name, subscription_name)

credentials  = GoogleCredentials.get_application_default()

def publish_pubsub_messages(messages, topic_path):
    items = [{'data': b64encode(message.encode('utf8')).decode('utf8')} for message in messages]
    service = googleapiclient.discovery.build('pubsub', 'v1', credentials=credentials) 
    request = service.projects().topics().publish(topic=topic_path, 
                                                          body={'messages' : [items]})
    response = request.execute()
    return response


def get_pubsub_messages(subscription_path, nb_messages=10):
    service = googleapiclient.discovery.build('pubsub', 'v1', credentials=credentials) 
    request = service.projects().subscriptions().pull(subscription=subscription_path, 
                                                      body={'maxMessages' : nb_messages})
    response = request.execute()
    if 'receivedMessages' in response.keys():
        return response['receivedMessages']
    else:
        return []

def acknowledge(message, subscription_path):
    ackId=message['ackId']
    service = googleapiclient.discovery.build('pubsub', 'v1', credentials=credentials) 
    request = service.projects().subscriptions().acknowledge(subscription=subscription_path, 
                                                             body={"ackIds": [ackId]})
    response = request.execute()
    return response

# BigTable initialization
project_id                = 'orion-175415'
instance_id               = 'webref-ssd'
table_id                  = 'catalogs'
product_column_family_id  = 'product'

client = bigtable.Client(project=project_id, admin=True)
instance = client.instance(instance_id)
table = instance.table(table_id)


def maybe_create_db():
    try:
    
        print('Checking BigTable setup')
        table.create()
        print('Created BigTable table: ' + table_id)
    except:
        pass
    
    try:
        metadata_column = table.column_family(video_column_family_id)
        metadata_column.create()
        print('Created column falily: ' + video_column_family_id)
    except:
        pass

#maybe_create_db()

def write_video(data):
    
    p={}
    p['merchant']     = 'Dailymotion'
    p['domain']       = 'www.dailymotion.com'
    p['page_type']    = 'product_page'
    p['product_id']   = data['id']
    p['product_name'] = data['title']
    p['product_url']  = data['url']
    p['category_id']   = data['owner.id']
    p['category_name'] = data['owner.screenname']
    p['category_url']  = "https://www.dailymotion.com/" + data['owner.username']
    p['category_path'] = data['owner.screenname']
    p['category_id_trail'] = json.dumps([data['owner.id']])
    p['category_name_trail'] = json.dumps([data['owner.screenname']])
    p['category_href_trail'] = json.dumps(["https://www.dailymotion.com/" + data['owner.username']])
    p['product_json'] = json.dumps(data
                                  )
    key  = (p['merchant']+'-'+p['product_id']).encode('utf-8')
    key  = hashlib.sha1(key).hexdigest().encode('utf-8')
    
    row = table.row(key)
    for k in p.keys():
        row.set_cell(product_column_family_id.encode('utf-8'), # column_family
                     k.encode('utf-8'),                             # column
                     p[k].encode('utf-8')                           # value
                    )       

    row.commit()
    