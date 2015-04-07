'''
Created on 2015/4/5

@author: indichen
'''

import inspect
import time
import base64
import urlparse
import httplib
import json
from FBUDRAgentUtil import Logger

class UDRFeeder(object):
    '''
    UDRFeeder
    '''


    def __init__(self, ERHostname, authId, authKey, group, cid, cache):
        assert(len(ERHostname)>0)
        assert(len(authId)>0)
        assert(len(authKey)>0)
        assert(len(group)>0)
        assert(len(cid)>0)
        assert(cache)
        assert(isinstance(cache, object))
        
        self.__ERHostname = ERHostname
        self.__ERUDRFeedAPI = UDRFeeder.__makeFeedAPIURL(self.__ERHostname)
        self.__authId = authId
        self.__authKey = authKey
        self.__group = group
        self.__cid = cid
        self.__group_cid = UDRFeeder.__makeGroupCid(self.__group, self.__cid)
        self.__cache = cache

    @classmethod
    def __makeFeedAPIURL(cls, ERHostname):
        assert(len(ERHostname)>0)
        return 'http://{0}/service/udr/feed/json'.format(ERHostname)
    
    @classmethod
    def __makeGroupCid(cls, group, cid):
        assert(len(group)>0)
        assert(len(cid)>0)
        return group + '_' + cid

    def setERHostname(self, ERHostname):
        assert(len(ERHostname)>0)
        self.__ERHostname = ERHostname
        self.__ERUDRFeedAPI = UDRFeeder.__makeFeedAPIURL(self.__ERHostname)
    def getERHostname(self):
        return self.__ERHostname
    
    def setAuthId(self, authId):
        assert(len(authId)>0)
        self.__authId = authId
    def getAuthId(self):
        return self.__authId
    
    def setAuthKey(self, authKey):
        assert(len(authKey)>0)
        self.__authKey = authKey
    def getAuthKey(self):
        return self.__authKey
    
    def setGroup(self, group):
        assert(len(group)>0)
        self.__group = group
        self.__group_cid = UDRFeeder.__makeGroupCid(self.__group, self.__cid)
    def getGroup(self):
        return self.__group
            
    def setCid(self, cid):
        assert(len(cid)>0)
        self.__cid = cid
        self.__group_cid = UDRFeeder.__makeGroupCid(self.__group, self.__cid)
    def getCid(self):
        return self.__cid
        
    def setCache(self, cache):
        assert(cache and isinstance(cache, object))
        self.__cache = cache
    def getCache(self):
        return self.__cache

    @classmethod
    def __createToken(cls, authKey, group_cid):
        '''
        createToken()
        Description:
            This function create a token with HMAC-SHA1 algorithm. It's
            not the generic funciton as hmac-sha1.py since it refers to the
            global vars g_authKey and g_group_cid.
        Param: None
        Result: A tuple contains (token, timestamp)
        '''
        from hashlib import sha1
        import hmac

        assert(len(authKey)>0)
        assert(len(group_cid)>0)
        
        timestamp = time.strftime('%Y%m%d%H%M%S', time.localtime(time.time()-1))
    
        # HMAC SHA1 hash with g_authKey, group_cid, timestamp
        hashed = hmac.new(base64.b64decode(authKey), digestmod=sha1)
        hashed.update(group_cid)
        hashed.update(timestamp)
    
        # The token
        token = hashed.digest().encode("base64").rstrip('\n')
        return (token, timestamp)
    # End of _createToken

    def feed(self):
        data = self.__cache.getData()
        if (len(data)==0): return True

        token = UDRFeeder.__createToken(self.__authKey, self.__group_cid)

        feedData = {
            'udrId' : self.__cache.getName(),
            'group' : self.__group,
            'cid' : self.__cid,
            'authId' : self.__authId,
            'authToken' : token[0],
            'timestamp' : token[1],
            'list' : [],
        }
        
        for d in data:
            feedData['list'].append({'key': d[0], 'value': d[1]})
            
        urlParseResult = urlparse.urlparse(self.__ERUDRFeedAPI)

        conn = None
        if (urlParseResult.scheme=='https'):
            conn = httplib.HTTPSConnection(urlParseResult.netloc)
        else:
            conn = httplib.HTTPConnection(urlParseResult.netloc)

        feedDataJSON = json.dumps(feedData)

        conn.request('POST', urlParseResult.path, feedDataJSON, {'Content-Type': 'application/json'})
    
        res = conn.getresponse()
    
        ret = False
        if (res.status==200):
            result = res.read()
            if (result=='success'):
                Logger.DbgLog('API server processed completely.')
                self.__cache.clearData()
                ret = True
            else:
                Logger.ErrorLog('Response from ER API server: %s' % result)
        else:
            Logger.ErrorLog('Response status: %d' % res.status)

        return ret
    