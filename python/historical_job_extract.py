'''
   Extract results from a historical job for Twitter, on GNIP
   Srivatsan Ramanujam <vatsan.cs@utexas.edu>
   6 June 2013
'''

import json
import urllib2
import os, threading,sys
import zlib
from threading import Lock

#number of urls to extract in a single thread
url_chunk_size=1000

#URL of the historical job: Example https://historical.gnip.com:443/accounts/ABCD/publishers/twitter/historical/track/jobs/IDENTIFIER/results.json'
results_url = '<HISTORICAL JOB URL>'
keys = [u'urlCount', u'urlList', u'expiresAt', u'suspectMinutesUrl', u'totalFileSizeBytes']

print_lock = Lock()
err_lock = Lock()
io_lock = Lock()

#Record errors in this file
sys.stderr = open('/tmp/gnip_errors','w')

def fetchChunk(chunk_urllist,output_folder,indx):
    '''	   
       This function should be invoked in its own thread.
       Fetch the data from the url and save it in a file locally.
       Inputs:
       =======
          chunk_urllist : a list of urls to be fetched (from AWS). The size of this list is defined by the url_chunk_size variable
          output_folder : The folder where the .gz files extracted from the urls  will be written to.
          indx : an integer representing the thread number (we spin one thread for every url_chunk_size urls, to fetch data simultaneously

       Outputs:
       ========
          The results are a bunch of .gz files written to the output_folder.
    '''
    offset=0
    resp=None
    for chunk_url in chunk_urllist:
        fname = os.path.join(output_folder,str(indx+offset)+'.json')
        #fetch the url only if the file doesn't already exist in the output folder
        if(not os.path.exists(fname)):
	    try:
                resp = urllib2.urlopen(chunk_url)
            except urllib2.HTTPError, e:
                with err_lock:
                    resp_msg = resp.msg if resp else None
                    sys.stderr.write('Error fetching url : {0}   resp.msg: {1} Error: {2} \n'.format(indx,resp_msg,str(e)))
            if(resp.msg=='OK'):
                with io_lock:
                     open(fname,'wb').write(zlib.decompress(resp.read(),16+zlib.MAX_WBITS))
        offset+=1
    return
	    
def readHistoricalJobResults(job_results_file):
    '''
       Read the JSON string from a local file, fetch data from the urls
       Inputs:
       =======
          job_results_file : The file containing the result of the historical job (this file contains 5 key-value pairs 
                             Refer to GNIP's documentation: http://support.gnip.com/customer/portal/articles/745678-retrieving-data-for-a-delivered-job
       Outputs:
       ========
          urlList: a list of URLs which point to AWS from where the individual JSON files (tweet chunks) can be downloaded.
    '''
    jobj=None
    try:
        jobj = json.loads(open(job_results_file,'r').read())
    except JSONDecodeError, e:
        jobj = None
        with err_lock:
             sys.stderr.write('Error reading job results file :{0}  Error: {1} \n'.format(job_results_file, str(e)))
    urlList =  jobj[u'urlList'] if(jobj and jobj.has_key(u'urlList')) else None
    return urlList

def main(job_results_file,output_folder):
    '''
       Fetch individual chunks and save in the output folder
       We'll spawn one thread for every url_chunk_size urls, each of these threads will send http requests to AWS to fetch the data and write it to file.
    '''
    urlList = readHistoricalJobResults(job_results_file)
    indx=0
    if(urlList):
        for u in range(0,len(urlList),url_chunk_size):
            with print_lock:
                print 'fetching chunk {0}/{1} '.format(indx/url_chunk_size,len(urlList)/url_chunk_size)
            t = threading.Thread(target=fetchChunk, args=(urlList[u:u+url_chunk_size],output_folder,indx))
            t.start()
            indx+=url_chunk_size
    
if(__name__=='__main__'):
    from sys import argv
    if(len(argv) !=3 ):
        print 'Usage: python historical_job_extract.py  <job_results_file>  <output_folder>'
    else:
        main(argv[1],argv[2])
    
