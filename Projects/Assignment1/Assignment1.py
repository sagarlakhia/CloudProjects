#Team of 2
#Name : Sagar Lakhia (1001123182)
#Team Member : Abhitej Date (1001113870)
#Course Number : CSE 6331 002
#Lab Number : Programming Assignment 1
#The main 4 funtions i.e. put,get,list obj and delete have been implemented.
#The encrypt_file and decrypt_file are the functions that encrypt and decrypt the file locally and returns the respected result file name
#So the total functions we implemented are 6

# -*- coding: cp1252 -*-
'''Copyright (c) 2015 HG,DL,UTA
   Python program runs on local host, uploads, downloads, encrypts local files to google.
   Please use python 2.7.X, pycrypto 2.6.1 and Google Cloud python module '''

#import statements.
import argparse
import httplib2
import os
import sys
import json
import time
import datetime
import io
import hashlib
#Google apliclient (Google App Engine specific) libraries.
from apiclient import discovery
from oauth2client import file
from oauth2client import client
from oauth2client import tools
from apiclient import http
from apiclient.http import MediaIoBaseDownload
#pycry#pto libraries.
from Crypto import Random
from Crypto.Cipher import AES
# Encryption using AES
#You can read more about this in the following link
#http://eli.thegreenplace.net/2010/06/25/aes-encryption-of-files-in-python-with-pycrypto

uploadFilefolderPath='./'
decryptedFileFolderPath='./'
#we can use os.getcwd() to save the file to current directory

#Delete content of a file
def deleteContent(pfile):
    pfile.seek(0)
    pfile.truncate()

#Create key from password given by user
def createKey(password):
    key = hashlib.sha256(password).digest()
    return key;

#this implementation of AES works on blocks of "text", put "0"s at the end if too small.
def pad(s):
    return s + b"\0" * (AES.block_size - len(s) % AES.block_size)

#Function to encrypt the message
def encrypt(message, key, key_size=256):
    message = pad(message)
    #iv is the initialization vector
    iv = Random.new().read(AES.block_size)
    #encrypt entire message
    cipher = AES.new(key, AES.MODE_CBC, iv)
    return iv + cipher.encrypt(message)

#Function to decrypt the message
def decrypt(ciphertext, key):
    iv = ciphertext[:AES.block_size]
    cipher = AES.new(key, AES.MODE_CBC, iv)
    plaintext = cipher.decrypt(ciphertext[AES.block_size:])
    return plaintext.rstrip(b"\0")

#Function to encrypt a given file on the same file which will be used to upload and then after will be deleted
def encrypt_file1(file_name, key):
    #Open file to read content in the file, encrypt the file data and
    #create a new file and then write the encrypted data to it, return the encrypted file name.
        txt = open(uploadFilefolderPath+file_name,"rb+") 
        enc =encrypt(txt.read(), key, key_size=256)
        deleteContent(txt)
        txt.write(enc)
        txt.close()
        return file_name

#Function to encrypt a given file and save it as enc_filename
def encrypt_file(service):
    print "Listing all files..."
#listing all available files to encrypt
    
    for file in os.listdir(os.getcwd()):
        print(file)
    encrypt1 = raw_input("Enter the file you want to encrypt: ")
    if(os.path.isfile(encrypt1)):
        encrypt_password = raw_input("Enter your desired password: ")
        print "Encrypting...."
        key =hashlib.sha256(encrypt_password).digest()
        fo = open(encrypt1,'rb')
        encrypted_data= encrypt(fo.read(),key,key_size=256)
        fo.close()
        fa =open ("enc_"+encrypt1,'wb')
        fa.write(encrypted_data)
        fa.close()
        print "Encrypted"
        print ' Encrytped File Name: enc_' +encrypt1
    else:
        print "Please enter a valid file name"


#Function to decrypt a given file.
def decrypt_file(service):
    print "Displaying list of encrypted files"
    for file in os.listdir(os.getcwd()):
        if file.startswith("enc_"):
                print(file)
    decrypt1= raw_input("Enter the file you want to decrypt: ")
    if(os.path.isfile(decrypt1)):
        decrypt_password = raw_input("Enter password: ")
        key =hashlib.sha256(decrypt_password).digest()
        fo = open (decrypt1,'rb')
        decrypted_data = decrypt(fo.read(),key)    
        fo.close()
        fa = open("dec_" + decrypt1,'wb')
        fa.write(decrypted_data)
        fa.close()
        print " Data Decrypted, File Name: dec_" +decrypt1
    else:
        print "Enter a valid file name"
        

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

#Downloads the specified object from the given bucket and deletes it from the bucket.
#Issue: If user forget the password used for encryption it is not possible for
#       user to get that password and without password file can not be decrypted.
def get(service):
  #User can be prompted to input file name(using raw_input) that needs to be be downloaded, 
  #as an example file name is hardcoded for this function.
  print "List of files on cloud"
  listobj(service)
  fileName = raw_input("Please enter fileName: ")
  try:
# Get Metadata
        try:
            
            req = service.objects().get(
                    bucket=_BUCKET_NAME,
                    object=fileName,
                    fields='bucket,name,metadata(my-key)',    
        
                    )
            resp = req.execute()
            print json.dumps(resp, indent=2)

            
# Get Payload Data
            req = service.objects().get_media(
                bucket=_BUCKET_NAME ,
                object=fileName,
            )
            

# The BytesIO object may be replaced with any io.Base instance.
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, req, chunksize=1024*1024) #show progress at download
            done = False
            while not done:
                status, done = downloader.next_chunk()
                if status:
                    print 'Download %d%%.' % int(status.progress() * 100)
                print 'Download Complete!'
            password = raw_input("Please enter Password for decryption: ")
            #Calling decrypt function to decrypt data retrieve from cloud
            dec = decrypt(fh.getvalue(),createKey(password))
            with open(decryptedFileFolderPath+fileName, 'wb') as fo:
                 fo.write(dec)
            print json.dumps(resp, indent=2)
            print "File is Succeesfully Downloaded"
            print "File is decrypted using the password you provided"
        except:
           print "File Not Found"
            
  except (client.AccessTokenRefreshError):
    print ("Error in the credentials")

  
#Reference: https://cloud.google.com/storage/docs/json_api/v1/objects/insert
#Puts a object into file after encryption and deletes the object from the local PC.
def put(service):
    print 'Listing all files in current directory'
    list_local_file(service)
    fileName = raw_input("Please enter fileName: ")
    if os.path.isfile(fileName) == True:
            password = raw_input("Please enter Password for encryption: ")
            en = encrypt_file1(fileName,createKey(password))
            req = service.objects().insert(
             bucket=_BUCKET_NAME,
             name=fileName,
             media_body=en)
            resp = req.execute()
            print json.dumps(resp, indent=2)
            removeLocalFile(en)
            print "File is Succeesfully uploaded"
    else:
            print "Enter a valid file"
    #Puts a object into file after encryption and deletes the object from the local PC.

#Reference: http://stackoverflow.com/questions/11968976/list-files-in-only-the-current-directory
#list  local files
def list_local_file(service):
        files = [f for f in os.listdir(uploadFilefolderPath) if os.path.isfile(f)]
        for f in files:
            print f

#Reference: http://stackoverflow.com/questions/185936/delete-folder-contents-in-python            
#This deletes the file from the local m/c
def removeLocalFile(fileName):
    os.remove(fileName)
    
#Reference: https://cloud.google.com/storage/docs/json_api/v1/objects/list
#Lists all the objects from the given bucket name
#Issue: This method gives list of files from bucket in json form instead of this it should
#       display file names only.    
def listobj(service):
    '''List all the objects that are present inside the bucket. '''
    fields_to_return = 'nextPageToken,items(bucket,name,metadata(my-key))'
    req = service.objects().list(
        bucket=_BUCKET_NAME,
        fields=fields_to_return,    # optional
        maxResults=42)              # optional
    
# Reference: Google cloud documentation, Sub topic :objects
#https://cloud.google.com/storage/docs/json_api/v1/objects

# If you have too many items to list in one request, list_next() will
# automatically handle paging with the pageToken.
    while req is not None:
            resp = req.execute()
            print json.dumps(resp, indent=2)
            req = service.objects().list_next(req, resp)
        
#Reference: https://cloud.google.com/storage/docs/json_api/v1/objects/delete
#This deletes the object from the bucket
def deleteobj(service):
    '''Prompt the user to enter the name of the object to be deleted from your bucket.
        Pass the object name to the delete() method to remove the object from your bucket'''
    listobj(service)
    fileName = raw_input("Please enter fileName: ")
    try:
        service.objects().delete(   
            bucket=_BUCKET_NAME,
            object=fileName).execute()
        print "File is Succeesfully Deleted"
    except:
        print "No Such File exists"
        

def Exit(service):
    print "Exit"
#Exits the loop
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
  while option!=5:#Repeat the option till user enter exit option
      optionsDetail=['Upload File on Cloud :Put','Download File from Cloud :Get','List all Files from Cloud :Listobj','Delete File from Cloud :Deletobj','Exit','Encrypt file locally','Decrypt file locally']
      #This is kind of switch equivalent in C or Java.
      #Store the option and name of the function as the key value pair in the dictionary.
      options = {1: put, 2: get, 3:listobj, 4:deleteobj, 5:Exit, 6:encrypt_file, 7:decrypt_file}
      for key, value in options.iteritems() :
        print key, optionsDetail[key-1]
      option =input("Please Enter Your Option: ") #Take the input from the user to perform the required operation.  
      #for example if user gives the option 1, then it executes the below line as put(service) which calls the put function defined above.
      options[option](service)


if __name__ == '__main__':
  main(sys.argv)
# [END all]


