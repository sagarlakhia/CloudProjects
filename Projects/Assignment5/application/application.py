# We need to import request to access the details of the POST request
# and render_template, to render our templates (form and response)
# we'll use url_for to get some URLs for the app on the templates
from flask import Flask, render_template, request, url_for
import boto
import urllib2
import sys
import argparse
import time
import datetime
import csv, StringIO
import os
import time
from random import randint
import boto.dynamodb2
from boto.dynamodb2.fields import HashKey, RangeKey, KeysOnlyIndex, GlobalAllIndex
from boto.dynamodb2.table import Table
from boto.dynamodb2.types import NUMBER

# Initialize the Flask application
application = Flask(__name__)

# Define a route for the default URL, which loads the form
@application.route('/')
def form():
    insertData(state) 
    return render_template('form_submit.html')

def downloadData():
    s3 = boto.connect_s3()
    key = s3.get_bucket('cse6331bucket').get_key('examples/Consumer_Complaints.csv')
    reader = csv.reader(StringIO.StringIO(key.get_contents_as_string()), csv.excel)
    return reader
def createTable():
    consumer_complaint = Table.create('consumer_complaint', schema=[
    HashKey('Complaint_ID'), # defaults to STRING data_type

    ], throughput={
    'read': 5,
    'write': 15,
    }, global_indexes=[
    GlobalAllIndex('EverythingIndex', parts=[
     HashKey('State'),
    ],
    throughput={
    'read': 1,
    'write': 1,
    })
    ],
    # If you need to specify custom parameters, such as credentials or region,
    # use the following:
    connection=boto.dynamodb2.connect_to_region('us-west-2')
    )
    return consumer_complaint
def insertData(state):
    conn = boto.dynamodb.connect_to_region('us-west-2');
    #createTable()
    consumer_complaint = createTable()#Table('consumer_complaint')
    
    isTableActive='false'
    while isTableActive=='true':
        tdescr=conn.describe_table('consumer_complaint')
        if(((tdescr['Table'])['TableStatus']) == 'ACTIVE'):
            start_time = time.time()
            reader=downloadData()
            i=0
            for row in reader:
                            if i==0:
                                i=i+1
                            else:
                                with consumer_complaint.batch_write() as batch:
                                    batch.put_item(data={
                                            'Complaint_ID' : row[0],
                                            'Product' : row[1],
                                            'Sub-product' : row[2],
                                            'Issue' : row[3],
                                            'State' : row[4],
                                            'ZIP code' : row[5],
                                            'Company' : row[6],
                                            'Company response' : row[7],
                                            'Timely response?' : row[8],
                                            'Consumer disputed': row[9],
                                    })
            #print("--- Time %s in seconds for Insert Query ---" % (time.time() - start_time))
            isTableActive='true'
    
    consumer_complaint.delete()

# Define a route for the action of the form, for example '/hello/'
# We are also defining which type of requests this route is 
# accepting: POST requests in this case
@application.route('/getData/', methods=['POST'])
def getData():
    state=request.form['state']
                          
    query=request.form['query']
    return render_template('form_action.html', state="hello", query=query)

# Run the app :)
if __name__ == '__main__':
  application.run( 
        host="localhost",
        port=int("8888")
  )
