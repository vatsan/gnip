"""
	cjson/json parser for GNIP tweets
	Srivatsan Ramanujam<vatsan.cs@utexas.edu>
	2 May 2013
"""
import sys
import codecs, csv, cStringIO

separator = '\t'
newline = '\n'

output_fields = [u'id',u'body', u'retweetCount', u'generator', u'gnip', u'object', u'actor', u'twitter_entities', u'verb', u'link', u'provider', u'postedTime', u'objectType']

class JSONDecoder(object):
    """
       Wrapper for JSON decoder or if cjson exists, cjson decoder
    """
    def __init__(self):
        try:
           import cjson
           self.decoder = cjson
           self.decoder_type = 'cjson'
        except ImportError:
           import json
           self.decoder = json
           self.decoder_type = 'json'

    def decode(self,json_string):
        """
           Call the appropriate decode method
        """
        return self.decoder.decoder(json_string) if self.decoder_type=='cjson' else self.decoder.loads(json_string)
        

## Begin Unicode Readers & Writers ##
#Copied from http://docs.python.org/2/library/csv.html
class UTF8Recoder:
    """
    Iterator that reads an encoded stream and reencodes the input to UTF-8
    """
    def __init__(self, f, encoding):
        self.reader = codecs.getreader(encoding)(f)

    def __iter__(self):
        return self

    def next(self):
        return self.reader.next().encode("utf-8")

class UnicodeReader:
    """
    A CSV reader which will iterate over lines in the CSV file "f",
    which is encoded in the given encoding.
    """

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        f = UTF8Recoder(f, encoding)
        self.reader = csv.reader(f, dialect=dialect, **kwds)

    def next(self):
        row = self.reader.next()
        return [unicode(s, "utf-8") for s in row]

    def __iter__(self):
        return self

class UnicodeWriter:
    """
    A CSV writer which will write rows to CSV file "f",
    which is encoded in the given encoding.
    """

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        # Redirect output to a queue
        self.queue = cStringIO.StringIO()
        self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
        self.stream = f
        self.encoder = codecs.getincrementalencoder(encoding)()

    def writerow(self, row):
        self.writer.writerow([s.encode("utf-8") if s else s for s in row])
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        data = data.decode("utf-8")
        # ... and reencode it into the target encoding
        data = self.encoder.encode(data)
        # write to the target stream
        self.stream.write(data)
        # empty queue
        self.queue.truncate(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)


## End Unicode Readers & Writers ##

#Initialize global decoder variable
jdecoder = JSONDecoder()

def getUTF8Writer(handle):
    '''
       Return a UTF-8 writer to the specified handle
    '''
    return codecs.getwriter('utf-8')(handle)

def printRec(handle,fields,escape_unicode=False):
    '''
       Converts a unicode string 'line' to a byte stream 
    '''
    if(escape_unicode):
        handle.write(separator.join(map(lambda x: unicode(x).encode('unicode-escape'),fields))+newline)
    else:
        handle.write(separator.join(map(lambda x: unicode(x),fields))+newline)

def parseTweet(tweet):
    '''
    	Parse the objects from the input tweet and them as key value pairs
    	Inputs: tweet as a json string (string)
    	Output: A list contaning the values of the fields of interest
    '''
    decoded_tweet = None
    try:
        decoded_tweet = cjson.decode(tweet,all_unicode=True)
    except cjson.DecodeError:
        return None
    #Note that language and kloutScore are GNIP fields and will be embedded inside the gnip key
    output = []
    for key in output_fields:
        if(decoded_tweet.has_key(key)):
            output.append(decoded_tweet[key])
        elif(decoded_tweet.has_key('gnip') and decoded_tweet['gnip'].has_key(key)):
            val = decoded_tweet['gnip'][key]['value'] if(key=='language') else decoded_tweet['gnip'][key]
            output.append(val)
        else:
		      output.append(None)
    return output

def vanillaParse(json_string):
    """
       Parse a json string into a json object and return the keys in output_field
    """
    output = []
    jobj = None
    try:
       jobj = jdecoder.decode(json_string)
    except JSONDecodeError, e:
       jobj = None
       sys.stderr.write("\nJSON Decode error : {0}".format(str(e)))

    #Only return a record when the json string represents the object of interest (should contain tweet object, not information messages)
    if(jobj and jobj.has_key(u'id')):
        for key in output_fields:
            output.append(jobj[key] if jobj.has_key(key) else '')
        return output
    else:
        return None

def mapFields(fields):
    '''
       Convert fields to unicode
    '''
    return map(lambda x: unicode(x) if x else x, fields)
    
def main(tweet_file,out_file):
    '''
       Parse the tweets in the input file and print out a tab separated list of
       fields of interest to stdout
    '''
    uWriter = UnicodeWriter(open(out_file,'w'),quoting=csv.QUOTE_ALL)
    fl = codecs.open(tweet_file,'r','utf-8')
    fl_out=None

    header=True
    for line in fl:
        line = line.replace('\r\n','').replace('\n','').strip()
        if(line and line.startswith('{') and line.endswith('}')):
            #values = parseTweet(line)
            values = vanillaParse(line)
            if values:
                if(header):
                    uWriter.writerow(mapFields(output_fields))
                    header=False
                uWriter.writerow(mapFields(values))
    fl.close()
    
if(__name__=='__main__'):
    from sys import argv
    if(len(argv) != 3):
        print 'Usage: python gnip_parser.py <input file> <output file>'
    else:
        if(len(argv)==2):
             main(argv[1],None)
        else:
             main(argv[1],argv[2])
