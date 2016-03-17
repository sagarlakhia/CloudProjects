#Team of 2
#Name: Sagar Lakhia (1001123182)
#Team Member :  Abhitej Date (1001113870)
#Course Number : CSE 6331-002
#Lab Number : Programming Assignment 3
#Description : Copy data from CSV file present in cloud to CloudSQL database and perform select query on that data

import logging
import os
import cloudstorage as gcs
import webapp2
import re
import time
import csv
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

class MainPage(webapp2.RequestHandler):

  def get(self):
    bucket_name = 'BUCKETNAME'#Bucket Name
    bucket = '/' + bucket_name
    filename = bucket + '/all_month.csv'
    self.tmp_filenames_to_clean_up = []

    try:
      self.read_file(filename)
      
    except Exception, e:
      logging.exception(e)
      self.response.write('\n\nThere was an error running the demo! '
                          'Please check the logs for more details.\n')

    else:
      self.response.write('\n\nThe Program ran successfully!\n')
      self.response.write('<br />')

  
  #Create table,inset data into table and perform select query
  def read_file(self, filename):
    try:
        if (os.getenv('SERVER_SOFTWARE') and
            os.getenv('SERVER_SOFTWARE').startswith('Google App Engine/')):
            
            db = MySQLdb.connect(unix_socket='/cloudsql/' + _INSTANCE_NAME, db='DATABASE', user='USERNAME')
        else:
                # Alternatively, connect to a Google Cloud SQL instance using:
             db = MySQLdb.connect(host='HOSTIP', port=3306, db='DATABASENAME', user='USERNAME',passwd='PASSWORD')

        cursor = db.cursor()
        try:
            
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
        cursor.execute('SELECT time,mag,type FROM allmonthEQ WHERE type="earthquake"')
      
            
        magTwoResult = {}
        magThreeResult= {}
        magFourResult= {}
        magFiveResult= {}
        magFGResult= {}
        magData={}
        for row in cursor.fetchall():
              m=re.split('[- T]',cgi.escape(row[0]))
              
              k=datetime.date(int(m[0]),int(m[1]),int(m[2])).isocalendar()[1]
              
              isDataPresent=0
              if row[1] == '2':
                    magTwoResult=getResultData(magTwoResult,isDataPresent,k)
                    magData[2]=magTwoResult
              elif row[1] == '3':
                    magThreeResult=getResultData(magThreeResult,isDataPresent,k)
                    magData[3]=magThreeResult
              elif row[1] == '4':
                    magFourResult=getResultData(magFourResult,isDataPresent,k)
                    magData[4]=magFourResult
              elif row[1] == '5':
                    magFiveResult=getResultData(magFiveResult,isDataPresent,k)
                    magData[5]=magFiveResult
              elif row[1] > '5':
                    magFGResult=getResultData(magFGResult,isDataPresent,k)
                    magData[6]=magFGResult
              isDataPresent=0
              timeRequired='Entered'
              
        
        variables = {'timerequired': timeRequired}
        self.response.write('Number of EarthQuake For Each')
        self.response.write('<br />')
        self.response.write('For Week with magnitude 2')
        self.response.write('<br />')
        for weekKey in magData[2].keys():
              self.response.write('For week ==='+str(weekKey))
              self.response.write('=== count is ==='+str(magData[2][weekKey]))
              self.response.write('<br />')
        self.response.write('For Week with magnitude 3')
        self.response.write('<br />')
        for weekKey in magData[3].keys():
              self.response.write('For week ==='+str(weekKey))
              self.response.write('=== count is ==='+str(magData[3][weekKey]))
              self.response.write('<br />')
        self.response.write('For Week with magnitude 4')
        self.response.write('<br />')
        for weekKey in magData[4].keys():
              self.response.write('For week ==='+str(weekKey))
              self.response.write('=== count is ==='+str(magData[4][weekKey]))
              self.response.write('<br />')
        self.response.write('For Week with magnitude 5')
        self.response.write('<br />')
        for weekKey in magData[5].keys():
              self.response.write('For week ==='+str(weekKey))
              self.response.write('=== count is ==='+str(magData[5][weekKey]))
              self.response.write('<br />')
        self.response.write('For Week with magnitude greater than 5')
        self.response.write('<br />')
        for weekKey in magData[6].keys():
              self.response.write('For week ==='+str(weekKey))
              self.response.write('=== count is ==='+str(magData[6][weekKey]))
              self.response.write('<br />')       
        self.response.write('<br />')
        self.response.write('---Time in seconds to get data from CloudSQL ---'+ str( (time.time() - start_time)))
        self.response.write('<br />')
        print "File is Succeesfully uploaded"
        template = JINJA_ENVIRONMENT.get_template('main1.html')
        self.response.write(template.render(variables))
        cursor.execute('TRUNCATE allmonthEQ;')
        db.commit()
        db.close()
    except DeadlineExceededError, error_message:
        logging.exception('Failed, exception happened - %s' % error_message)   


app = webapp2.WSGIApplication([('/', MainPage),
                               ('/sign', MainPage)],
                              debug=True)
def main():

   
    app = webapp2.WSGIApplication([('/', MainPage),
                                           ('/sign', MainPage)],
                                          debug=True)
    
    run_wsgi_app(app)

if __name__ == "__main__":
    main()
    
#References:
#https://cloud.google.com/appengine/docs/python/googlecloudstorageclient/getstarted
#https://cloud.google.com/appengine/docs/python/cloud-sql/?hl=en_US&_ga=1.35097889.1450009567.1433876171  
