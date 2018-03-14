from utils import *
import hashlib


def process_message(msg):
    try:
        xid = b64decode(msg['message']['data']).decode('utf-8')
        data = video_data(xid)
        write_video(data)
    except:
        print('Error')
    acknowledge(msg, subscription_path)
    print(xid)
    
messages_returned=1
        
while messages_returned>0:
    messages=get_pubsub_messages(subscription_path, nb_messages=1000)
    messages_returned = len(messages)
    print("Got %s messages" % messages_returned)
    for message in messages: process_message(message)