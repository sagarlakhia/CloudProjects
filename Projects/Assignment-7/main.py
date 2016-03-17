#Team of 3
#Team Member :Abhitej Date (1001113870), Sagar Lakhia (1001123182),Rasika Dhanurkar(001110582)
#Course Number : CSE 6331-002
#Lab Number : Programming Assignment 7


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
from google.appengine.ext import db
from google.appengine.api import files
import datetime
import MySQLdb
import os
import jinja2
import urllib2
from google.appengine.api import files

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
username=''
# Define your production Cloud SQL instance information.
_INSTANCE_NAME = 'INSTANCENAME'
class HtmlPage(webapp2.RequestHandler):
        def get(self):
                #Calling login.html
                template = JINJA_ENVIRONMENT.get_template('login.html')
                self.response.write(template.render())
#Calling Signup functionality                
class SignupPage(webapp2.RequestHandler):
  def get(self):
        #Calling Signup functionality     
        template = JINJA_ENVIRONMENT.get_template('sign_up.html')
        self.response.write(template.render())
  def post(self):
    username= cgi.escape(self.request.get('username'))#Get username from html
    password= cgi.escape(self.request.get('password'))#Get password from html
    cnpassword= cgi.escape(self.request.get('cnpassword'))#Get password from html        
    try:
              self.signUp(username,password,cnpassword)
    except Exception, e:
              logging.exception(e)
              self.response.write('\n\nThere was an error running the program! ddd'
                                  'Please check the logs for more details.\n')
            
 
#Inserting new user details inside table
  def signUp(self,username,password,cnpassword):
    try:
        if (os.getenv('SERVER_SOFTWARE') and
                    os.getenv('SERVER_SOFTWARE').startswith('Google App Engine/')):
                    db = MySQLdb.connect(unix_socket='/cloudsql/' + _INSTANCE_NAME, db='DATABASE', user='USERNMAE')
        else:
                    db = MySQLdb.connect(host='HOSTIP', port=3306, db='DATABASE', user='USERNAME',passwd='PASSWORD')
        cursor = db.cursor()
        start_timeForInsert = time.time()#Start time of insert query
        #Inserting new user details inside table
        cursor.execute('INSERT INTO user (username,password) VALUES (%s,%s)',(username,password))
        self.response.write('<br />')
        start_time = time.time()
        
        template = JINJA_ENVIRONMENT.get_template('sign_up.html')
        self.response.write(template.render())

        db.commit()
        db.close()
    except DeadlineExceededError, error_message:
        logging.exception('Failed, exception happened - %s' % error_message)
        self.response.write('<br />')
   
#Inserting Comment into database
class CommentEnter(webapp2.RequestHandler):
        def get(self):
                url = self.request.url
        def post(self):
                try:
                      self.addComment()
              
                except Exception, e:
                      logging.exception(e)
                      self.response.write('\n\nThere was an error running the Program ! '
                                         'Please check the logs for more details.\n')

                else:
                      self.response.write('<br />')
        def addComment(self):
                if (os.getenv('SERVER_SOFTWARE') and
                    os.getenv('SERVER_SOFTWARE').startswith('Google App Engine/')):
                    db = MySQLdb.connect(unix_socket='/cloudsql/' + _INSTANCE_NAME, db='DATABASE', user='USERNAME')
                else:
                    db = MySQLdb.connect(host='HOSTIP', port=3306, db='DATABASE', user='USERNAME',passwd='PASSWORD')                        
              commentsNew=self.request.get('comments')
              threadname=self.request.get('threadname')
              loggedUserName=self.request.get('usernameHd')
              imageref='Nothing'
              userName=''
              comments=''
              cursor = db.cursor()
              list=[]
              cursor.execute('SELECT * FROM thread WHERE thread_name = (%s)',threadname)
              for data in cursor.fetchall():
                        thread_name=data[2]
                        userName=data[0]
                        imageref=data[1]
                        comments=data[3]
              if comments is not None:    
                      comments=comments+'ed=ed'+loggedUserName+'cmnet'+commentsNew
                      comments_split=comments.split("ed=ed")#Comments are added in format as username+cmnet+comment+ed=ed+username2+cmnet+comment+ed=ed
                      for items in comments_split:
                             list.append(items.split('cmnet'))
              else:
                      comments=loggedUserName+'cmnet'+commentsNew
                      list.append(comments.split('cmnet'))
              
                         
              try:#Updating table to insert Comment into database

                      cursor.execute ('UPDATE thread SET comments=%s WHERE thread_name=%s',(comments,threadname))
              except:
                     self.response.write('Error in update')
              db.commit()
              db.close()
              variables = {'imageLink': imageref,'userName': userName,'commentsList':list,'threadname':threadname,'usernameSaved':loggedUserName} 
              template = JINJA_ENVIRONMENT.get_template('thread.html')
              self.response.write(template.render(variables))
#Remove photo from database              
class RemovePhoto(webapp2.RequestHandler):
        def post(self):
             threadname = self.request.get('threadnametorm')
             loggedUserName=self.request.get('usernameLoggedIn')
             if (os.getenv('SERVER_SOFTWARE') and
                    os.getenv('SERVER_SOFTWARE').startswith('Google App Engine/')):
                    db = MySQLdb.connect(unix_socket='/cloudsql/' + _INSTANCE_NAME, db='DATABASE', user='USERNAME')
             else:
                    db = MySQLdb.connect(host='HOSTIP', port=3306, db='DATABASE', user='USERNAME',passwd='PASSWORD')
        
             cursor = db.cursor()
             cursor.execute("DELETE FROM thread WHERE thread_name = '%s';"% threadname.strip())
             db.commit()
             cursor.execute('SELECT thread_name FROM thread')
             threadlist=[]
             for row1 in cursor.fetchall():
                        threadlist.append(row1[0])
             db.close()
             variables={'threadlist' :threadlist,'usernameSaved':loggedUserName,'threadname':threadname}
             template = JINJA_ENVIRONMENT.get_template('home.html')
             self.response.write(template.render(variables))
#Callig thread page to display image       
class ThreadCall(webapp2.RequestHandler):
        def post(self):
             #Getting data from the html form
             threadname = self.request.get('threadnameIdHidden')
             loggedUserName=self.request.get('loggedUserId')

             if (os.getenv('SERVER_SOFTWARE') and
                    os.getenv('SERVER_SOFTWARE').startswith('Google App Engine/')):
                    db = MySQLdb.connect(unix_socket='/cloudsql/' + _INSTANCE_NAME, db='DATABASE', user='USERNAME')
             else:
                    db = MySQLdb.connect(host='HOSTIP', port=3306, db='DATABASE', user='USERNAME',passwd='PASSWORD')
        
             cursor = db.cursor()
             comments=''
             imageref=''
             userName=''
             #retrieve given thread data
             cursor.execute('SELECT * FROM thread WHERE thread_name = (%s)',threadname)
             for data in cursor.fetchall():
                        thread_name=data[2]
                        userName=data[0]
                        imageref=data[1]
                        comments=data[3]
             list=[]           
             if comments is not None:        
                     comments_split=comments.split("ed=ed")
                     for items in comments_split:
                             list.append(items.split('cmnet'))
             isRmActive='False'
             if loggedUserName == userName :
                     isRmActive='True'
                     
             

             variables = {'imageLink': imageref,'userName': userName,'commentsList':list,'threadname':threadname,'usernameSaved':loggedUserName,'isRmActive':isRmActive}           
             template = JINJA_ENVIRONMENT.get_template('thread.html')
             self.response.write(template.render(variables))
             
#retrieve given thread data
class ThreadPage(webapp2.RequestHandler): 
     def get(self):
             url = self.request.url

     def post(self):
            bucket_name = 'cloudbucket12'#Bucket Name
            bucket = '/' + bucket_name
            name=self.request.get('name')
            fileName = bucket+'/'+name
            self.tmp_filenames_to_clean_up = []
            try:
              self.write_file(fileName,name)
              
            except Exception, e:
              logging.exception(e)
              self.response.write('\n\nThere was an error running the Program ! '
                                  'Please check the logs for more details.\n')

            else:
              self.response.write('\n\nThe Program ran successfully!\n')
              self.response.write('<br />')
     def write_file(self,fileName,name):
              if (os.getenv('SERVER_SOFTWARE') and
                    os.getenv('SERVER_SOFTWARE').startswith('Google App Engine/')):
                    db = MySQLdb.connect(unix_socket='/cloudsql/' + _INSTANCE_NAME, db='DATABASE', user='USERNAME')
              else:
                    db = MySQLdb.connect(host='HOSTIP', port=3306, db='DATABASE', user='USERNAME',passwd='PASSWORD')

              cursor = db.cursor()
    
              
              threadname= cgi.escape(self.request.get('threadNm'))#Get threadname from html 
              username=self.request.get('usernameHd')#Get username from html 
              img_img = image = self.request.get("image")
              #self.response.write('Creating file %s\n' % filename)
              write_retry_params = gcs.RetryParams(backoff_factor=1.1)
              gcs_file = gcs.open(fileName,
                                'w',
                                content_type='image/jpg',
                                retry_params=write_retry_params,
                                options={'x-goog-acl': 'public-read'}  
                                )
              gcs_file.write(img_img)
              gcs_file.close()
              #url of where image going to upload
              url='https://console.developers.google.com/m/cloudstorage/b/cloudbucket12/o/'+name
              try:#Inseting url,username and thread name to table
                      cursor.execute('INSERT INTO thread (username,imageref,thread_name) VALUES (%s,%s,%s)',(username,url,threadname))
              except:
                     self.response.write('Thread Already Exists')
              db.commit()
              cursor.execute('SELECT thread_name FROM thread')
                
              threadlist=[]
              for row1 in cursor.fetchall():
                        threadlist.append(row1[0])
              db.close()
              
              
              self.tmp_filenames_to_clean_up.append(fileName)
              #passing threadlist,username and threadname
              variables={'threadlist' :threadlist,'usernameSaved':username,'threadname':threadname}
              template = JINJA_ENVIRONMENT.get_template('home.html')
              self.response.write(template.render(variables))

               
#Login page for user
class signedin(webapp2.RequestHandler):
  

  def post(self):
    username= cgi.escape(self.request.get('username'))#Get username from html
    password= cgi.escape(self.request.get('password'))#Get password from html
    
    try:
      self.signIn(username,password)
    except Exception, e:
      logging.exception(e)
      self.response.write('\n\nThere was an error running the program! ddd'
                          'Please check the logs for more details.\n')

    else:
      self.response.write('<br />')

  
#Validate user authentication
  def signIn(self,username,password):
    try:
        if (os.getenv('SERVER_SOFTWARE') and
                    os.getenv('SERVER_SOFTWARE').startswith('Google App Engine/')):
                    db = MySQLdb.connect(unix_socket='/cloudsql/' + _INSTANCE_NAME, db='DATABASE', user='USERNAME')
        else:
                    db = MySQLdb.connect(host='HOSTIP', port=3306, db='DATABASE', user='USERNAME',passwd='PASSWORD')
        cursor = db.cursor()
        start_timeForInsert = time.time()#Start time of insert query
        

        start_time = time.time()
        #Validate user authentication by selecting username from table
        cursor.execute('SELECT username,password FROM user where username IN (%s)',username)
        threadlist=[]
        for row in cursor.fetchall():
                if row[0]==username:
                        if row[1]==password:
                                cursor.execute('SELECT thread_name FROM thread')
                                for row1 in cursor.fetchall():
                                        threadlist.append(row1[0])
                                variables = {'usernameSaved':username,'threadlist' :threadlist}
                                template = JINJA_ENVIRONMENT.get_template('home.html')
                                self.response.write(template.render(variables))
                        else:
                                self.response.write('Please Enter Correct details')
        #db.commit()
        db.close()
    except DeadlineExceededError, error_message:
        logging.exception('Failed, exception happened - %s' % error_message)
        self.response.write('<br />')


app = webapp2.WSGIApplication([('/', HtmlPage),
                               ('/signedin', signedin),('/sign_up', SignupPage),('/registered', SignupPage),('/thread', ThreadPage),('/CommentEnter', CommentEnter),('/ThreadCall', ThreadCall),('/RemovePhoto', RemovePhoto)],
                              debug=True)
def main():

   
    app = webapp2.WSGIApplication([('/', HtmlPage),
                                           ('/signedin', signedin),('/sign_up', SignupPage),('/registered', SignupPage),('/thread', ThreadPage),('/CommentEnter', CommentEnter),('/ThreadCall', ThreadCall),('/RemovePhoto', RemovePhoto)],
                                          debug=True)
    
    run_wsgi_app(app)

if __name__ == "__main__":
    main()

#References:
#https://cloud.google.com/appengine/docs/python/googlecloudstorageclient/migrate
#https://cloud.google.com/storage/docs/access-control
#http://stackoverflow.com/questions/1307378/python-mysql-update-statement    
