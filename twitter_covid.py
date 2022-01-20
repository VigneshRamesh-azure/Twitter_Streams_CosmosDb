import json
import requests
from keys import *
import time
from time import sleep
import pydocumentdb
from pydocumentdb import document_client
from pydocumentdb import documents


consumer_key = API_KEY
consumer_Secrets = API_SECRET_KEY


connectionPolicy = documents.ConnectionPolicy()
connectionPolicy.EnableEndpointDiscovery
connectionPolicy.PreferredLocations = preferredLocations


client = document_client.DocumentClient(host, {'masterKey': masterKey}, connectionPolicy)
dbLink = 'dbs/' + databaseId
collLink = dbLink + '/colls/' + collectionId


stream_url = 'https://api.twitter.com/2/tweets/search/stream'
rules_url = 'https://api.twitter.com/2/tweets/search/stream/rules'


def get_bearer_token():

    response = requests.post(
        "https://api.twitter.com/oauth2/token",
        auth=(consumer_key, consumer_Secrets),
        data={'grant_type': 'client_credentials'})

    if response.status_code is not 200:
        raise Exception(f"Cannot get a Bearer token (HTTP %d): %s" % (response.status_code, response.text))


    body = response.json()
    return body['access_token']

def create_rules():
    # Add the values you need to track here
    payload = {
        "add": [
                {"value": ""},
                {"value": ""},
                {"value": ""},
                {"value": ""},
                {"value": ""}
                ]
    }



    response = requests.post(rules_url,
                              headers={"Authorization": "Bearer {}".format(get_bearer_token())},json=payload)

    if response.status_code == 201:
        print("Response : {}".format(response.text))

    else:
        print("Cannot create rules(HTTP{}): {}".format(response.status_code, response.text))

def stream_connect(token):
    response = requests.get(stream_url,headers={"Authorization": "Bearer {}".format(get_bearer_token())},stream=True)


    for response_line in response.iter_lines():
        if response_line:
            tweet = json.loads(response_line)
            text = tweet['data']['id']
            print(text)
            tweet['id'] = str(tweet['data']['id'])
            print(tweet)
            print(tweet['id'])
            #print(str(tweet['data']['id']))
            client.CreateDocument(collLink, tweet)
            return True


def main():
    token = get_bearer_token()
    timeout = 0
    while True:
        stream_connect(token)
        sleep(2**timeout)
        timeout +=1


if __name__ == "__main__":
   # Run the create_rules(): to create rules before running the main code #
   main()
