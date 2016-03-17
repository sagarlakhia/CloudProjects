#Team of 3
#Team Member :Abhitej Date (1001113870), Sagar Lakhia (1001123182),Rasika Dhanurkar 1001110582
#Course Number : CSE 6331-002
#Lab Number : Programming Assignment 9


#import statements.
from flask import Flask, render_template, request, url_for,redirect

import argparse
import os
import tinys3
import sys
import boto
from boto.s3.connection import S3Connection
import pylab
from pylab            import plot,show
from numpy            import vstack,array
from numpy.random     import rand
from scipy.cluster.vq import kmeans, vq, whiten
import csv
import urllib2
application = Flask(__name__)

@application.route('/')
def home():
    
    return render_template('home.html')

@application.route('/request', methods=['POST'])
def displayResult():
    access_key='Amazon access Key'
    secret_key='Amazon secrete Key'
    conn1 = boto.connect_s3(
        aws_access_key_id = access_key,
        aws_secret_access_key = secret_key,
        #host = 'objects.dreamhost.com',
        is_secure=False,               # uncomment if you are not using ssl
        calling_format = boto.s3.connection.OrdinaryCallingFormat(),
        )
  
    noOfCluster=0
    #Get Radio button input to check user choice
    chart = request.form['radio']
    #If user choice is cluster
    if chart == 'cluster':
        noOfCluster =long(request.form['cluster'])
        data_arr = []
        meal_name_arr = []
        #Url of data csv
        url='https://storage.googleapis.com/cloudbucket12/imptry4.csv'
        response=urllib2.urlopen(url)
        reader = csv.reader(response)
        i=0
        for row in reader:
                if row[5] is None:
                    row[5]=0
                if row[5]=='':
                    row[5]=0
                if "," in row[6] :  
                    rowVal=row[6].split(",")
                    row[6]=rowVal[0]+''+rowVal[1]
                    row[6]=float(row[6])    
                if row[6]=='':
                    row[6]=0
                if row[6]=='N' :
                    row[6]=0
                
                data_arr.append([float(x) for x in row[5:]])
                meal_name_arr.append([row[0]])

    
    #print data_arr
    
        data = vstack( data_arr )
        
        meal_name = vstack(meal_name_arr)
    # normalization
        data = whiten(data)#Before running k-means, it is beneficial to rescale each feature dimension of the observation set with whitening.
    #Each feature is divided by its standard deviation across all observations to give it unit variance.

    # computing K-Means with K (clusters)
        centroids, distortion = kmeans(data,noOfCluster)

    
    # assign each sample to a cluster
        idx,_ = vq(data,centroids)
    
    # some plotting using numpy's logical indexing
        listOfColor=['ob','or','og','oc','om','ok','oy']
        for index in range(noOfCluster):
            plot(data[idx==index,0], data[idx==index,1],listOfColor[index])
        for index in range(noOfCluster):
            result_names = meal_name[idx==index, 0]
        print "================================="
        print "Cluster " + str(index+1)
        for name in result_names:
            print name

        plot(centroids[:,0],
             centroids[:,1],
             'oy',markersize=8)
        #saving file to temp image
        pylab.savefig('temp.jpg')
        pylab.clf()
        noOfCluster=0
        index=0
        f = open('temp.jpg','rb')

        
   
#Exits the loop
        image=os.path.abspath('temp.jpg')
        #saving file to AWS bucket
        bucket = conn1.get_bucket('cse6331bucket')
        key=bucket.new_key('cluster.jpg')
        key.set_contents_from_file(f)
        key.set_canned_acl('public-read')
        hello_key=bucket.get_key('cluster.jpg')
        hello_url = hello_key.generate_url(0, query_auth=False, force_http=True)
        image=hello_url
    
    
        return render_template('home.html',image=image,url=url)

    #If user choice is barchart
    elif chart=="barchart":
        
        return render_template('barchartNew.html')
    else:#If user choice is scatter
        return render_template('scatter.html')
     

##if __name__ == '__main__':
##  application.run( 
##        host="localhost",
##        port=int("8882"),debug=True
##  )
if __name__ == '__main__':
  application.run(
        "0.0.0.0",debug=True
  )
#References:
#1] https://gist.github.com/d3noob
#2] www.stackoverflow.com
#3] http://glowingpython.blogspot.com/2012/04/k-means-clustering-with-scipy.html
#4]http://bl.ocks.org/mbostock/3887118
#5]https://github.com/mbostock/d3/wiki/Gallery  
  
  

