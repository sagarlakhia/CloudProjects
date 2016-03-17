#Team of 2
#Name:  Sagar Lakhia (1001123182)
#Team Member : Abhitej Date (1001113870)
#Course Number : CSE 6331-002
#Lab Number : Programming Assignment 3
#Description : Copy data to Cloud from website
#import statements.
import csv
import urllib2
import argparse
import httplib2
import os
import sys
import json
import time
import datetime
import io
#Google apliclient (Google App Engine specific) libraries.
from apiclient import discovery
from oauth2client import file
from oauth2client import client
from oauth2client import tools
from apiclient import http
from apiclient.http import MediaIoBaseDownload

_BUCKET_NAME = 'BUCKETNAME' #name of your google bucket.
_API_VERSION = 'v1'

# Parser for command-line arguments.
parser = argparse.ArgumentParser(
    description=__doc__,
    formatter_class=argparse.RawDescriptionHelpFormatter,
    parents=[tools.argparser])


# client_secret.json is the JSON file that contains the client ID and Secret.
#You can download the json file from your google cloud console.
CLIENT_SECRETS = os.path.join(os.path.dirname(__file__), 'client_secret.json')

# Set up a Flow object to be used for authentication.
# Add one or more of the following scopes. 
# These scopes are used to restrict the user to only specified permissions (in this case only to devstorage) 
FLOW = client.flow_from_clientsecrets(CLIENT_SECRETS,
  scope=[
      'https://www.googleapis.com/auth/devstorage.full_control',
      'https://www.googleapis.com/auth/devstorage.read_only',
      'https://www.googleapis.com/auth/devstorage.read_write',
    ],
    message=tools.message_if_missing(CLIENT_SECRETS))


def downloadData():
    # Retrieve the webpage as a string
    response = urllib2.urlopen('http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.csv')
    csv = response.read()
    csvstr = str(csv).strip("b'")
    return csvstr
    
#Puts a object into file after encryption and deletes the object from the local PC.
def put(service):
    start_time = time.time()
    csvstr=downloadData()
    media = http.MediaIoBaseUpload(io.BytesIO(csvstr), 'text/csv')
    req = service.objects().insert(
    bucket=_BUCKET_NAME,
    name='all_month1.csv',
    media_body=media)
    resp = req.execute()
    print json.dumps(resp, indent=2)
    print("---Time  %s in seconds to Upload file to Cloud ---" % (time.time() - start_time))
    print("---Time  %s in seconds to insert data to CloudSQL ---" % (time.time() - start_time))
    print "File is Succeesfully uploaded"

def Exit(service):
    print "Exit"
def main(argv):
  # Parse the command-line flags.
  flags = parser.parse_args(argv[1:])

  
  #sample.dat file stores the short lived access tokens, which your application requests user data, attaching the access token to the request.
  #so that user need not validate through the browser everytime. This is optional. If the credentials don't exist 
  #or are invalid run through the native client flow. The Storage object will ensure that if successful the good
  # credentials will get written back to the file (sample.dat in this case). 
  storage = file.Storage('sample.dat')
  credentials = storage.get()
  if credentials is None or credentials.invalid:
    credentials = tools.run_flow(FLOW, storage, flags)

  # Create an httplib2.Http object to handle our HTTP requests and authorize it
  # with our good Credentials.
  http = httplib2.Http()
  http = credentials.authorize(http)

  # Construct the service object for the interacting with the Cloud Storage API.
  service = discovery.build('storage', _API_VERSION, http=http)
  option=1;
  while option!=2:
      optionsDetail=['Upload File on Cloud','Exit']
      #This is kind of switch equivalent in C or Java.
      #Store the option and name of the function as the key value pair in the dictionary.
      options = {1: put, 2:Exit}
      for key, value in options.iteritems() :
        print key, optionsDetail[key-1]
      option =input("Please enter Your Option: ")
        
      #Take the input from the user to perform the required operation.  
      #for example if user gives the option 1, then it executes the below line as put(service) which calls the put function defined above.
      options[option](service)


if __name__ == '__main__':
  main(sys.argv)
# [END all]


