#Team of 2
#Name: Sagar Lakhia (1001123182)
#Team Member : Abhitej Date (1001113870) 
#Course Number : CSE 6331-002
#Lab Number : Programming Assignment 4

import boto
import urllib2
import sys
import argparse
import time
import datetime
import csv, StringIO
import os
import MySQLdb
import time
from random import randint
import memcache




def downloadData():
    s3 = boto.connect_s3()
    key = s3.get_bucket('BUCKETNAME').get_key('Consumer_Complaints.csv')
    reader = csv.reader(StringIO.StringIO(key.get_contents_as_string()), csv.excel)
    
#    print reader
    return reader

def insertData():
    
    db = dbconnect()
    cursor = db.cursor()
    cursor.execute('CREATE TABLE consumer_complaint(Complaint VARCHAR(255),Product VARCHAR(255),Subproduct VARCHAR(255),Issue VARCHAR(255),State VARCHAR(255),ZIPcode VARCHAR(255),Company VARCHAR(255),Companyresponse VARCHAR(255),Timelyresponse VARCHAR(255),Consumerdisputed VARCHAR(255));')
    start_time = time.time()
    reader=downloadData()
    i=0
    for row in reader:
                    if i==0:
                        i=i+1
                    else:
                        #print i
                        
                        cursor.execute('INSERT INTO consumer_complaint(Complaint, Product, Subproduct,Issue,State,ZIPcode,Company,Companyresponse,Timelyresponse,Consumerdisputed) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',(row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9]))
                        #i=i+1
    print("---Time %s in seconds for insertions  ---" % (time.time() - start_time))
      

    db.commit()
    db.close()
    
def uploadData():
    start_time = time.time()
    s3 = boto.connect_s3()
    bucket = s3.create_bucket('BUCKETNAME')  # bucket names must be unique
    key = bucket.new_key('Consumer_Complaints.csv')
    #key.set_contents_from_filename('Consumer_Complaints.csv')
    response = urllib2.urlopen('https://storage.googleapis.com/BUCKETNAME/Consumer_Complaints.csv')
    csv = response.read()
    csvstr = str(csv).strip("b'")
    key.set_contents_from_string(csvstr)
    key.set_acl('public-read')
    print("---Time  %s in seconds to Upload file to Cloud ---" % (time.time() - start_time))
    print "File is Succeesfully uploaded"
    
def randomqueries():
    insertData()
    queries=['SELECT DISTINCT Issue from consumer_complaint;','SELECT DISTINCT State from consumer_complaint;','SELECT Complaint from consumer_complaint WHERE state="TX";','SELECT COUNT(*) from consumer_complaint;']
    reader=downloadData()
    db = MySQLdb.connect(host='ENDPOINT ADDRESS', port=3306, db='DATABASE', user='USERNAME',passwd='PASSWORD')
    cursor = db.cursor()
    start_time = time.time()
    for x in range(999):
        random = randint(0,3)
        cursor.execute(queries[random])
    print "Time taken to complete 1000 queries is"
    print (time.time() - start_time)

    start_time2 = time.time()
    for x in range(4999):
        random = randint(0,3)
        cursor.execute(queries[random])
    print "Time taken to complete 5000 queries is"
    print (time.time() - start_time2)

    start_time3 = time.time()
    for x in range(19999):
        random = randint(0,3)
        cursor.execute(queries[random])
    print "Time taken to complete 20000 queries is"
    print (time.time() - start_time3)
    cursor.execute('DROP TABLE consumer_complaint;')
    db.commit()
    db.close()
    
def randomqueriesWithMamCached():
    insertData()
    queries=['SELECT DISTINCT Issue from consumer_complaint;','SELECT DISTINCT State from consumer_complaint;','SELECT Complaint from consumer_complaint WHERE state="TX";','SELECT COUNT(*) from consumer_complaint;']
    reader=downloadData()
    memc = memcache.Client(['MEMCACHEADDRESS'],debug=0)
    db = dbconnect()
    for index in range (3):
        memc.delete(str(index))
    cursor = db.cursor()
    start_time = time.time()
    for x in range(999):
        random = randint(0,3)
        queryResultPresent = memc.get(str(random))
        if not queryResultPresent:
            cursor.execute(queries[random])
            rows = cursor.fetchall()
            memc.set(str(random),rows,0)
        
    print "Time taken in seconds to complete 1000 queries is"
    print (time.time() - start_time)
    start_time2 = time.time()
    for x in range(4999):
        random = randint(0,3)
        queryResultPresent = memc.get(str(random))
        if not queryResultPresent:
            cursor.execute(queries[random])
            rows = cursor.fetchall()
            memc.set(str(random),rows,60)
    print "Time taken in seconds to complete 5000 queries is"
    print (time.time() - start_time2)

    start_time3 = time.time()
    for x in range(19999):
        random = randint(0,3)
        queryResultPresent = memc.get(str(random))
        if not queryResultPresent:
            cursor.execute(queries[random])
            rows = cursor.fetchall()
            memc.set(str(random),rows,60)
        else:
            continue    
    print "Time taken in seconds to complete 20000 queries is"
    print (time.time() - start_time3)
    for index in range (4):
        memc.delete(str(index))
    cursor.execute('DROP TABLE consumer_complaint;')
    db.commit()
    db.close()

def dbconnect():
    db = MySQLdb.connect(host='ENDPOINTADDRESS', port=3306, db='DATABASE', user='USERNAME',passwd='PASSWORD')
    return db
def queryOntupleWithMamcached():
    db=dbconnect()
    cursor = db.cursor()
    cursor.execute('CREATE TABLE consumer_complaint_tuple(Complaint VARCHAR(255),Product VARCHAR(255),Subproduct VARCHAR(255),Issue VARCHAR(255),State VARCHAR(255),ZIPcode VARCHAR(255),Company VARCHAR(255),Companyresponse VARCHAR(255),Timelyresponse VARCHAR(255),Consumerdisputed VARCHAR(255));')
    tuples = input("Enter the number of tuples you want to run your queries on:")

    reader=downloadData()
    i=0
    tupleinserted=0
    insert_time=time.time()
    for row in reader:
                    if i==0:
                        i=i+1
                    else:
                        #print i
                        cursor.execute('INSERT INTO consumer_complaint_tuple(Complaint, Product, Subproduct,Issue,State,ZIPcode,Company,Companyresponse,Timelyresponse,Consumerdisputed) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',(row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9]))
                        tupleinserted=tupleinserted+1
                        if tupleinserted==tuples:
                            break
                        
                             
    print "Time taken in seconds to insert " + str(tuples) + " tuples to database is "
    print (time.time()-insert_time)
    
    queries=['SELECT  distinct Issue from consumer_complaint_tuple;','SELECT DISTINCT State from consumer_complaint_tuple;','SELECT Complaint from consumer_complaint_tuple WHERE State="TX";','SELECT COUNT(*) from consumer_complaint_tuple;']
    memc = memcache.Client(['MEMCACHEADDRESS'],debug=1)
    start_time = time.time()
    for x in range(999):
        random = randint(0,3)
        queryResultPresent = memc.get(str(random))
        if not queryResultPresent:
            cursor.execute(queries[random])
            rows = cursor.fetchall()
            memc.set(str(random),rows,60)
             
    print "Time taken in seconds to complete 1000 queries for " + str(tuples) + " tuples is"
    print (time.time() - start_time)


    start_time2 = time.time()
    for x in range(4999):
        random = randint(0,3)
        queryResultPresent = memc.get(str(random))
        if not queryResultPresent:
            cursor.execute(queries[random])
            rows = cursor.fetchall()
            memc.set(str(random),rows,60)
    print "Time taken in seconds to complete 5000 queries for " + str(tuples) + " tuples is"
    print (time.time() - start_time2)

    start_time3 = time.time()
    for x in range(19999):
        random = randint(0,3)
        queryResultPresent = memc.get(str(random))
        if not queryResultPresent:
            cursor.execute(queries[random])
            rows = cursor.fetchall()
            memc.set(str(random),rows,60)
    print "Time taken in seconds to complete 20000 queries for " + str(tuples) + " tuples is"
    print (time.time() - start_time3)

    for index in range (4):
        memc.delete(str(index))
    cursor.execute('Drop Table consumer_complaint_tuple;')
    db.commit()
    db.close()
    
def queries2():

    db=dbconnect()
    cursor = db.cursor()
    cursor.execute('CREATE TABLE consumer_complaint_tuple(Complaint VARCHAR(255),Product VARCHAR(255),Subproduct VARCHAR(255),Issue VARCHAR(255),State VARCHAR(255),ZIPcode VARCHAR(255),Company VARCHAR(255),Companyresponse VARCHAR(255),Timelyresponse VARCHAR(255),Consumerdisputed VARCHAR(255));')
    tuples = input("Enter the number of tuples you want to run your queries on:")

    reader=downloadData()
    i=0
    tupleinserted=0
    insert_time=time.time()
    for row in reader:
                    if i==0:
                        i=i+1
                    else:
                        #print i
                        cursor.execute('INSERT INTO consumer_complaint_tuple(Complaint, Product, Subproduct,Issue,State,ZIPcode,Company,Companyresponse,Timelyresponse,Consumerdisputed) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',(row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9]))
                        tupleinserted=tupleinserted+1
                        if tupleinserted==tuples:
                            break
                        
                             
    print "Time taken in seconds to insert " + str(tuples) + " tuples to database is "
    print (time.time()-insert_time)
    
    queries=['SELECT  distinct Issue from consumer_complaint_tuple;','SELECT DISTINCT State from consumer_complaint_tuple;','SELECT Complaint from consumer_complaint_tuple WHERE State="TX";','SELECT COUNT(*) from consumer_complaint_tuple;']
    random = randint(0,3)

    start_time = time.time()
    for x in range(999):
        cursor.execute(queries[random])
        
    print "Time taken in seconds to complete 1000 queries for " + str(tuples) + " tuples is"
    print (time.time() - start_time)


    start_time2 = time.time()
    for x in range(4999):
        cursor.execute(queries[random])
    print "Time taken in seconds to complete 5000 queries for " + str(tuples) + " tuples is"
    print (time.time() - start_time2)

    start_time3 = time.time()
    for x in range(19999):
        cursor.execute(queries[random])
    print "Time taken in seconds to complete 20000 queries for " + str(tuples) + " tuples is"
    print (time.time() - start_time3)

    
    cursor.execute('Drop Table consumer_complaint_tuple;')
    db.commit()
    db.close()
    
def Exit():
    
    print "Exit"
def main(argv):
  
  option=1;
  while option!=6:
      optionsDetail=['Upload File on Cloud','Insert data and Random Queries','Insert data and Random Queries with MemCached','Queries on tuple','Queries on tuple with MemCached','Exit']
      #This is kind of switch equivalent in C or Java.
      #Store the option and name of the function as the key value pair in the dictionary.
      options = {1: uploadData,2:randomqueries,3:randomqueriesWithMamCached,4:queries2,5:queryOntupleWithMamcached,6:Exit}
      for key, value in options.iteritems() :
        print key, optionsDetail[key-1]
      option =input("Please enter Your Option: ")
        
      
      options[option]()


if __name__ == '__main__':
  main(sys.argv)
# [END all]


