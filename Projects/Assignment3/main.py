#Team of 2
#Name:  Sagar Lakhia (1001123182)
#Team Member : Abhitej Date (1001113870)
#Course Number : CSE 6331-002
#Lab Number : Programming Assignment 3
#Description : Copy data from CSV file present in cloud to CloudSQL database and perform select query on that data

import logging
import os
import cloudstorage as gcs
import webapp2
import re
import csv
import cgi
import time
from google.appengine.api import app_identity
from google.appengine.runtime import DeadlineExceededError
import cgi
from google.appengine.ext.webapp.util import run_wsgi_app
import datetime
import MySQLdb
import os
import jinja2



my_default_retry_params = gcs.RetryParams(initial_delay=0.2,
                                          max_delay=5.0,
                                          backoff_factor=2,
                                          max_retry_period=25)
gcs.set_default_retry_params(my_default_retry_params)
# Configure the Jinja2 environment.
JINJA_ENVIRONMENT = jinja2.Environment(
  loader=jinja2.FileSystemLoader(os.path.dirname(__file__)),
  autoescape=True,
  extensions=['jinja2.ext.autoescape'])

# Define your production Cloud SQL instance information.
_INSTANCE_NAME = 'INSTANCENAME'

def getResultData(result,isDataPresent,k):
        for weekKey in result.keys():
            if weekKey == k:
                  result[weekKey] =result[weekKey]+1
                  isDataPresent=1
                  break;
        if isDataPresent==0:
                result[k]=1
        return result

class HtmlPage(webapp2.RequestHandler):
        def get(self):

                variables = {'timerequired': 'Enter Magnitude or  Location here'}
                template = JINJA_ENVIRONMENT.get_template('main1.html')
                self.response.write(template.render(variables))
        

class MainPage(webapp2.RequestHandler):

  def post(self):
    bucket_name = 'BUCKETNAME'#Bucket Name
    name1= cgi.escape(self.request.get('magnitude'))#Get magnitude from html
    location= cgi.escape(self.request.get('location'))#Get Location from html
    

    bucket = '/' + bucket_name
    filename = bucket + '/all_month.csv'
    self.tmp_filenames_to_clean_up = []

    try:
      self.read_file(filename,name1,location)
    except Exception, e:
      logging.exception(e)
      self.response.write('\n\nThere was an error running the program! ddd'
                          'Please check the logs for more details.\n')

    else:
      self.response.write('<br />')

  
#Create table,inset data into table and perform select query
  def read_file(self, filename,name1,location):
    try:
        if (os.getenv('SERVER_SOFTWARE') and
            os.getenv('SERVER_SOFTWARE').startswith('Google App Engine/')):
            
            db = MySQLdb.connect(unix_socket='/cloudsql/' + _INSTANCE_NAME, db='DATABASE', user='USERNAME')
        else:
                
                # Alternatively, connect to a Google Cloud SQL instance using:
            db = MySQLdb.connect(host='HOSTIP', port=3306, db='DATABASE', user='USERNAME',passwd='PASSWORD')
        cursor = db.cursor()
        try:
            #Create Table allmonthEQ
            cursor.execute('CREATE TABLE allmonthEQ(  time VARCHAR(255),  latitude VARCHAR(255), longitude VARCHAR(255), depth VARCHAR(255),  mag VARCHAR(255), magType VARCHAR(255),  nst VARCHAR(255),gap VARCHAR(255),  dmin VARCHAR(255), rms VARCHAR(255),  net VARCHAR(255),id VARCHAR(255), updated VARCHAR(255),  place VARCHAR(255), type VARCHAR(255));')
            
        except Exception:
            print "Error"
        start_timeForInsert = time.time()#Start time of insert query
        i=0
        with gcs.open(filename) as gcs_file:
            reader = csv.reader(gcs_file)
            for row in reader:
                if i==0:
                    i=i+1
                else:
                    cursor.execute('INSERT INTO allmonthEQ (time, latitude, longitude,depth,mag,magType,nst,gap,dmin,rms,net,id,updated,place,type) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',(row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10],row[11],row[12],row[13],row[14]))
                    

        gcs_file.close()
        self.response.write('---Time in seconds to insert data to CloudSQL ---'+str((time.time() - start_timeForInsert)))
        self.response.write('<br />')
        start_time = time.time()
        cursor.execute('SELECT time,mag,type,place FROM allmonthEQ;')
      
            # Create a list of guestbook entries to render with the HTML.
        placelist = [];
        
        magTwoResult = {}
        
        magData={}
        name=name1.encode('utf-8')
        location1=location.encode('utf-8')
        #Check if magnitude is enter by user
        if name != '':
                for row in cursor.fetchall():
                      if (row[1] ==name):
                                   placelist.append(dict([('time',cgi.escape(row[0])),
                                                 ('mag',cgi.escape(row[1])),
                                                 ('type',cgi.escape(row[2])),('place',(row[3]))
                                                 ]))
                      m=re.split('[- T]',cgi.escape(row[0]))
                      k=datetime.date(int(m[0]),int(m[1]),int(m[2])).isocalendar()[1]
                      
                      isDataPresent=0
                      if (row[1] == name):
                            magTwoResult=getResultData(magTwoResult,isDataPresent,k)
                            magData[name]=magTwoResult

                      isDataPresent=0
                      timeRequired='End'
        elif location1!='':   #Check if location is enter by user      
                for row in cursor.fetchall():
                      if location1.lower() in row[3].lower():#Convert string to lowerCase
                                   placelist.append(dict([('time',cgi.escape(row[0])),
                                                 ('mag',cgi.escape(row[1])),
                                                 ('type',cgi.escape(row[2])),('place',(row[3]))
                                                 ]))
                      m=re.split('[- T]',cgi.escape(row[0]))
                      k=datetime.date(int(m[0]),int(m[1]),int(m[2])).isocalendar()[1]
                      
                      isDataPresent=0
                      if location1.lower() in row[3].lower():#Convert string to lowerCase
                            magTwoResult=getResultData(magTwoResult,isDataPresent,k)
                            magData[location1]=magTwoResult

                      isDataPresent=0
                      timeRequired='End'
        if name == '':
                name=location1
                output='Location'
        else:
                output='magnitude'        
        variables = {'timerequired': timeRequired,'placelist':placelist}
        self.response.write('Number of EarthQuake For Each')
        self.response.write('<br />')
        self.response.write('For Week with '+output+'='+str(name))
        self.response.write('<br />')
        for weekKey in magData[name].keys():
              self.response.write('For week ==='+str(weekKey))
              self.response.write('=== count is ==='+str(magData[name][weekKey]))
              self.response.write('<br />')
        self.response.write('---Time in seconds to get data from CloudSQL ---'+ str( (time.time() - start_time)))
        self.response.write('<br />')
        template = JINJA_ENVIRONMENT.get_template('main1.html')
        self.response.write(template.render(variables))
        cursor.execute('TRUNCATE allmonthEQ;')
        db.commit()
        db.close()
    except DeadlineExceededError, error_message:
        logging.exception('Failed, exception happened - %s' % error_message)
        self.response.write('<br />')


app = webapp2.WSGIApplication([('/', HtmlPage),
                               ('/sign', MainPage)],
                              debug=True)
def main():

   
    app = webapp2.WSGIApplication([('/', HtmlPage),
                                           ('/sign', MainPage)],
                                          debug=True)
    
    run_wsgi_app(app)

if __name__ == "__main__":
    main()

#References:
#https://cloud.google.com/appengine/docs/python/googlecloudstorageclient/getstarted
#https://cloud.google.com/appengine/docs/python/cloud-sql/?hl=en_US&_ga=1.35097889.1450009567.1433876171
#Stackoverflow.com for various doubts and questions    
