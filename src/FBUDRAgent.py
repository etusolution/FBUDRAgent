'''
Created on 2015/3/30

@author: indichen
'''
#from __builtin__ import None

import sys
import time
from FBUDRCacheDB import UDRCacheDB
from FBUDRAgentUtil import Logger
from FBUDRFetcher import UDRFetcher
from FBUDRFeeder import UDRFeeder

g_secsADay = 86400
g_IIIAPIServer = 'http://www.google.com.tw/'
g_ERHostname = 'erwin.etusolution.com'
g_ERAuthId = 'ETUDEV'
g_ERAuthKey = 'u1mAxJQIbZqdEeYOaJVxfSWsq/Q='
g_ERGroup = 'ER'
g_ERCid = 'etusolution'

g_ERFBUDRId1 = 'etu_fb01'
g_ERFBUDRId2 = 'etu_fb02'
g_ERFBUDRId3 = 'etu_fb03'

def test(db):
    udrdata1 = db.createCacheData('FBUDR1')
    udrdata1.setLastFetchTS(time.time())
    Logger.DbgLog('Fetched TS: {0}'.format(udrdata1.getLastFetchTS()) )
    
    ret = udrdata1.clearData()
    assert(ret)
    ret = udrdata1.setData( [('u001', '"pid1", "pid2"'), ('u002', '"pid3", "pid4"')])
    assert(ret)
    ret = udrdata1.getData()
    assert(len(ret)>0)
    print ret

def fetch(cache, api):   
    assert(cache and isinstance(cache, object))
    assert(len(api)>0)
     
    # if not over 1 day since the last fetch time, skip the fetch
    ts = cache.getLastFetchTS()
    if (time.time()-ts < g_secsADay): return

    # Clear cached data before fetching
    cache.clearData()
    
    # Create the fetch instance and make fetch
    fetcher = UDRFetcher(api, cache, TestParser)
    fetcher.fetch()

def feed(cache):
    assert(cache and isinstance(cache, object))
    
    feeder = UDRFeeder(g_ERHostname, g_ERAuthId, g_ERAuthKey, g_ERGroup, g_ERCid, cache)
    feeder.feed()

def TestParser(response):
    return [('u001', '"pid1", "pid2"'),
            ('u002', '"pid3", "pid4"')
           ]
    
def main():
    db = UDRCacheDB()
    ret = db.InitDB()
    assert(ret)

    # Create the cache data db, fetch and then feed
    udrdata1 = db.createCacheData(g_ERFBUDRId1)    
#    fetch(udrdata1, g_IIIAPIServer)
    feed(udrdata1)


if __name__ == '__main__':
    sys.exit( main() )
    