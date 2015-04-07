'''
Created on 2015/4/5

@author: indichen
'''

import json
import httplib
import urlparse
import inspect
import time
from FBUDRAgentUtil import Logger

class UDRFetcher(object):
    '''
    UDRFetcher
    '''

    def __init__(self, api, cache, parser=None):
        '''
        Constructure
        [in]
        api: The URL of API to fetch data
        cache: A UDRCacheDB.UDRCacheData object
        parser: (optional) A function objection to provide the parser implementation
        '''
        assert(len(api)>0)
        assert(cache and isinstance(cache, object))

        self.__api = api
        self.__cache = cache
        if (parser and inspect.isfunction(parser)):
            self.__parser = parser
        else:
            self.__parser= UDRFetcher.defParser

    @classmethod
    def defParser(cls, response):
        '''
        Default parser
        - It 
        [in]
        response: A HTTP response 
        '''
        resultJSON = json.load(response)
        Logger.DbgLog('Parse fetcher response status: %s' % resultJSON['status'])
        return resultJSON['result']

    def setAPI(self, api):
        assert(len(api)>0)
        self.__api = api
    def getAPI(self):
        return self.__api
    
    def setCache(self, cache):
        assert(cache and isinstance(cache, object))
        self.__cache = cache
    def getCache(self):
        return self.__cache
    
    def setParser(self, parser):
        assert(parser)
        assert(inspect.isfunction(parser))
        self.__parser = parser
    def getParser(self):
        return self.__parser
        
    def fetch(self, requestData='[]', requestDataType='application/json'):
        urlParseResult = urlparse.urlparse(self.__api)
    
        conn = None
        if (urlParseResult.scheme=='https'):
            conn = httplib.HTTPSConnection(urlParseResult.netloc)
        else:
            conn = httplib.HTTPConnection(urlParseResult.netloc)
        conn.request('POST', urlParseResult.path, requestData, {'Content-Type': requestDataType})
#        conn.request('GET', urlParseResult.path)
    
        res = conn.getresponse()
    
        if (res.status==200):
            data = self.__parser(res)
            self.__cache.setData(data)
            self.__cache.setLastFetchTS(time.time())
        else:
            Logger.ErrorLog('Response status: %d' % res.status)
            return False
        
        return True