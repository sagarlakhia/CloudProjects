#Team
#Abhitej Date (1001113870)
#Rasika Dhanurkar (1001110582)
#Sagar Lakhia (1001123182)
#Assignment8
#Course : 6331 Lab -002 Time: 1:00-3:00

#import statements.

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


def displayResult():
    
    noOfCluster=0
    #Taking the number of cluster as an input from the user
    noOfCluster =input("Enter the number of Clusters:")
    
    data_arr = []
    dataid = []

    url='https://storage.googleapis.com/cloudbucket12/imptry4.csv'
    response=urllib2.urlopen(url)
    reader = csv.reader(response)
    i=0
    for row in reader:
        #Removing null spaces and unnecessary commas
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
            dataid.append([row[0]])

    
    
    data = vstack( data_arr )
    
    data_id = vstack(dataid)
    
    # normalization
    data = whiten(data)
    
    # computing K-Means with K (clusters)
    centroids, distortion = kmeans(data,noOfCluster)
    
    # assign each sample to a cluster
    idx,_ = vq(data,centroids)

    # some plotting using numpy's logical indexing
    listOfColor=['ob','or','og','oc','om','ok','oy']
    for index in range(noOfCluster):
        plot(data[idx==index,0], data[idx==index,1],listOfColor[index])
    for index in range(noOfCluster):
        result_names = data_id[idx==index, 0]

        #printing points that come under the cluster
        print "================================="
        print "Cluster " + str(index+1)
        for name in result_names:
            print name

    #Plotting Centroids
    plot(centroids[:,0],
         centroids[:,1],
         'oy',markersize=8)

    noOfCluster=0
    index=0
    #Showing the data
    show()
    pylab.clf()

   
#Exits the loop 
def Exit():
    print "Exit"


def main(argv):
  option=0;
  while option!=2:#Repeat the option till user enter exit option
      optionsDetail=['displayResult','Exit']
      #This is kind of switch equivalent in C or Java.
      #Store the option and name of the function as the key value pair in the dictionary.
      options = {1: displayResult,2:Exit}
      for key, value in options.iteritems() :
        print key, optionsDetail[key-1]
      option =input("Please Enter Your Option: ") #Take the input from the user to perform the required operation.  
      #for example if user gives the option 1, then it executes the below line as put(service) which calls the put function defined above.
      options[option]()




if __name__ == '__main__':
  main(sys.argv)


#References:
#[1] http://glowingpython.blogspot.com/2012/04/k-means-clustering-with-scipy.html
#[2] stackoverflow.com
