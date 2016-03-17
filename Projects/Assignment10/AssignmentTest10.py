#Team of 2
#Team Member :Abhitej Date (1001113870), Sagar Lakhia (1001123182)
#Course Number : CSE 6331-002
#Lab Number : Programming Assignment 10


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
from mpl_toolkits.mplot3d import Axes3D
import matplotlib.pyplot as plt
app = Flask(__name__)

@app.route('/')
def home():

    return render_template('home.html',display='display:none;')

@app.route('/request', methods=['POST'])
def displayResult():


    noOfCluster=0
    #Get Radio button input to check user choice
    chart = request.form['radio']
    #If user choice is cluster
    if chart == 'cluster':
        noOfCluster =long(request.form['cluster'])
        data_arr = []
        meal_name_arr = []
        #Url of data csv
        url='https://storage.googleapis.com/cloudbucket786/imptry4.csv'
        response=urllib2.urlopen(url)
        reader = csv.reader(response)
        
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
                if "," in row[7] :
                    rowVal=row[7].split(",")
                    row[7]=rowVal[0]+''+rowVal[1]
                    row[7]=float(row[6])
                if row[7]=='':
                    row[7]=0
                if row[7]=='N' :
                    row[7]=0
                data_arr.append([float(x) for x in row[5:]])#adding data to data_array
                meal_name_arr.append([row[0]])#adding ids to second array



    #print data_arr
        fig = plt.figure()
        ax = fig.add_subplot(111, projection='3d')#We are using 3D projection as we are plotting 3D data
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
            plot(data[idx==index,0], data[idx==index,1],data[idx==index,2],listOfColor[index])# using 3 objects for 3D projection
        for index in range(noOfCluster):
            result_names = meal_name[idx==index, 0]
        print "================================="
        print "Cluster " + str(index+1)
        for name in result_names:
            print name

        plot(centroids[:,0],
             centroids[:,1],
             centroids[:,2],
             'oy',markersize=8)
        #saving file to temp image
        #Assigning labels to axis
        ax.set_xlabel('X Label')
        ax.set_ylabel('Y Label')
        ax.set_zlabel('Z Label')
        pylab.savefig('temp.jpg')
        pylab.clf()
        

        image="https://www.pythonanywhere.com/user/abhitej/files/home/abhitej/temp.jpg"
        #Overwrites the image on pythonanywhere.com

        return render_template('home.html',image=image,display='display:block;')

    else:
        list=[]
        words=request.form['words']
        list=words.split(",")

        list1=[]
        for s in list:
            list1.append(s.encode('ascii','ignore'))

        return render_template('home.html',list1=list1,display='display:none;')# Assigning display none for cluster if user chooce wordcloud



#References:
#1] https://gist.github.com/d3noob
#2] www.stackoverflow.com
#3] http://glowingpython.blogspot.com/2012/04/k-means-clustering-with-scipy.html
#4]http://bl.ocks.org/mbostock/3887118
#5]https://github.com/mbostock/d3/wiki/Gallery
#6]https://www.jasondavies.com
#7]http://glowingpython.blogspot.com    



