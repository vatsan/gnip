'''
	This program is based on https://github.com/gnip/support/blob/master/Premium%20Stream%20Connection/Python/StreamingConnection.py
	1) I have extended it to read base-64 encoded credentials from file in $HOME
	2) Also adding support to handle "connection reset by peer" errors
        3) The extract data will automatically be saved to a file of the form <date-month-year.json> in the directory provided in the output. 
           If such a file exists, it will be appended to the same.
        Srivatsan Ramanujam <vatsan.cs@utexas.edu>
'''
import urllib2,socket
import base64
import zlib,httplib
import threading
from threading import Lock
import json
import sys
import ssl
import os
from datetime import datetime
import time

# Tune CHUNKSIZE as needed.  The CHUNKSIZE is the size of compressed data read
# For high volume streams, use large chuck sizes, for low volume streams, decrease
# CHUNKSIZE.  Minimum practical is about 1K.
CHUNKSIZE = 4*1024
GNIPKEEPALIVE = 30  # seconds
NEWLINE = '\r\n'

#Should be in the user's home directory.
USER_CREDENTIALS_FILE = os.path.join(os.path.expanduser('~'),'.gnip_credentials.secret')

#Example: https://stream.gnip.com:443/accounts/abcd/Prod.json
URL = '<gnip_curl_url>'

print_lock = Lock()
err_lock = Lock()
prev_date = ''

class procEntry(threading.Thread):
    def __init__(self, buf):
        self.buf = buf
        threading.Thread.__init__(self)

    def run(self):
        for rec in [x.strip() for x in self.buf.split(NEWLINE) if x.strip() <> '']:
            try:
                jrec = json.loads(rec.strip())
                tmp = json.dumps(jrec)
                with print_lock:
                    print(tmp)
            except ValueError, e:
                with err_lock:
                    sys.stderr.write("Error processing JSON: %s (%s)\n"%(str(e), rec))

def stdoutRedirector(output_folder):
    '''
        This function  when invoked via a Thread, periodically checks for the time of the day and redirects stdout to a file of the form: output_folder/<current_date>.txt
        Inputs:
        =======
             output_folder : path in the file system where the output files will be written
        Outputs:
        =========
             Merely replaces sys.stdout to a file output_folder/<current_date>.txt
    '''
    global prev_date
    while(True):
        fname = os.path.join(output_folder,datetime.utcnow().strftime('%d-%b-%Y')+'.txt')
        if(fname != prev_date):
            try:
               with print_lock:
                   sys.stdout = open(fname,'a')
                   #Close the previous file handle
                   if(hasattr(prev_date,'close')):
                       prev_date.close()
                   prev_date = fname
            except IOError, e:
               #Display error
               with err_lock:
                   sys.stderr.write("Error redirecting to file: %s"%(str(e)))
        time.sleep(60)

def fetchUserCredentials():
    '''
	The user credentials file should contain just one line which is of the form base64.encodestring('%s:%s' % (UN, PW)) - where UN and PW are your user name and password.
	Make sure the file is only readable by you.
    '''
    creds = open(USER_CREDENTIALS_FILE,'r').read()
    return creds

def fetchHeaders(creds):
    '''
	Return the Headers
    '''
    HEADERS = { 'Accept': 'application/json',
            'Connection': 'Keep-Alive',
            'Accept-Encoding' : 'gzip',
            'Authorization' : 'Basic %s' % creds  }
    return HEADERS

def getStream():
    req = urllib2.Request(URL, headers=fetchHeaders(fetchUserCredentials()))

    response = urllib2.urlopen(req, timeout=(1+GNIPKEEPALIVE))
    # header -  print response.info()
    decompressor = zlib.decompressobj(16+zlib.MAX_WBITS)
    remainder = ''
    while True:
        tmp = ''
        try:
            tmp = decompressor.decompress(response.read(CHUNKSIZE))
        except httplib.IncompleteRead, e:
            with err_lock:
               sys.stderr.write("Incomplete Read: %s"%(str(e))) 
               tmp=''              
        if tmp == '':
            return
        [records, remainder] = ''.join([remainder, tmp]).rsplit(NEWLINE,1)
        procEntry(records).start()

def testSetup(output_folder):
    '''
       Check if the user credentials file is present at  $HOMEDIR/.gnip_credentials.secret 
    '''
    if not os.path.exists(USER_CREDENTIALS_FILE):
       print 'Please enter your GNIP credentials encoded in base64 in :{0}'.format(USER_CREDENTIALS_FILE)
       print 'It should be of the form base64.encodestring("%s:%s" % (UN, PW)) - where UN and PW are your user name and password.'
       return False
    if not os.path.exists(output_folder):
       print 'Output folder: {0} does not exists'.format(output_folder)
       return False
    return True
    
def main(output_folder):
    '''
       Entry point
    '''
    if(testSetup(output_folder)):
        #Start the stdout redirector
        print 'output_folder',output_folder
        t = threading.Thread(target=stdoutRedirector, args=(output_folder,))
        t.daemon = True
        t.start()
        
        # Note: this automatically reconnects to the stream upon being disconnected
        while True:
            try:
                getStream()
                with err_lock:
                    sys.stderr.write("Forced disconnect: %s\n"%(str(e)))
            except ssl.SSLError, e:
                with err_lock:
                    sys.stderr.write("Connection failed: %s\n"%(str(e)))
            except urllib2.HTTPError, e:
                with err_lock:
                    sys.stderr.write("HTTP Error: %s\n"%(str(e)))
            except urllib2.URLError, e:
                with err_lock:
                    sys.stderr.write("URL Error: %s\n"%(str(e)))
            except socket.error, e:
                with err_lock:
                    sys.stderr.write("Socket Error: %s\n"%(str(e)))
            except IOError, e:
	        with err_lock:
            	    sys.stderr.write("IOError: %s\n"%(str(e)))
            except SystemExit, e:
                with err_lock:
                    sys.stderr.write("It hit the fan now: %s\n"%(str(e)))
                sys.exit(e)

if __name__ == "__main__":
    from sys import argv
    if(len(argv) !=2):
        print 'Usage: python StreamingConnection.py <output folder>'
    else:
        main(argv[1])

