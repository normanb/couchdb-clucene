from xml.dom import minidom
from urllib2 import Request, urlopen, URLError

import simplejson as json

FLICKR_FEED = "http://api.flickr.com/services/feeds/photos_public.gne?format=json"

def checkDB(couchUrl, dbName):
    url = '%s/%s' % (couchUrl, dbName)
    req = Request(url)
    req.get_method = lambda: 'HEAD'
    print 'Checking DB %s' % url

    try: 
        urlopen(req).close()
    except URLError, e:
        # create the new database
        print 'Creating DB %s' % dbName
        r = Request(url)
        r.get_method = lambda: 'PUT'
        try:
            urlopen(r).close()
        except URLError, e:
            if (e.code != 201):
                print 'Unabled to create database %s' % dbName
                return False
                
    return True
    
def checkDesignDoc(couchUrl, dbName, designName, designDoc):
    url = '%s/%s/%s' % (couchUrl, dbName, designName)
    req = Request(url)
    req.get_method = lambda: 'HEAD'
    
    print 'Checking Design Doc %s' % url
    
    try: 
        urlopen(req).close()
    except URLError, e:
        # create the new design document
        print 'Creating design doc %s' % designName
        r = Request(url, designDoc, {'Content-Type':'application/json'})
        r.get_method = lambda: 'PUT'

        try:
            urlopen(r).close()
        except URLError, e:
            if (e.code != 201):
                print 'Unabled to create design doc %s' % designName
                return False
        
    return True    


def load(couchUrl, dbName, ftiDesignName, ftiDesignDoc):
    # check the couchdb database exists
    if not checkDB(couchUrl, dbName):
        return
        
    # check the design document exists
    if not checkDesignDoc(couchUrl, dbName, ftiDesignName, ftiDesignDoc):
        return 
        
    # load the flickr feed into couch
    data = None
    
    try:
        req = Request(FLICKR_FEED)
        resp = urlopen(req)
        data = resp.read()
        resp.close()
    except URLError, e:
        if (e.code != 201):
            print 'unabled to retrieve flickr feed %s' % FLICKR_FEED
            return
    
    if data is not None:
        # parse json data remove jsonFlickrFeed(...) wrapper
        requestUrl = '%s/%s' % (couchUrl, dbName)
       
        if (len(data) > 16):   
            jsonData = json.loads(data[15:-1])
            cntr = 0
            for item in jsonData['items']:
               id = item['author_id']
               
               # make an http put to the couchdb database to put the entry
               r = Request('%s/%s' % (requestUrl, id))
               r.get_method = lambda: 'PUT'
               try:
                 urlopen(r, json.dumps(item)).close()
                 print 'created doc entry %s' % id
               except URLError, e:
                 if (e.code != 201):
                    print 'Unabled to create entry %s' % id
        else:
           print 'cannot parse %s' % data
                

           
        

if __name__ == '__main__':
    couchUrl = 'http://localhost:5984'
    dbName = 'flickr'
    ftiDesignName = '_design/fti'
    f = open('_design/fti.json')
    ftiDesignDoc = f.read()
    f.close()
    
    
    load(couchUrl, dbName, ftiDesignName, ftiDesignDoc)
    
    # search by tag
    #
    print 'query data, e.g. curl http://localhost:5984/flickr/_fti/by_tag?q=jer* to find all tags that start with jer* '
    
  
