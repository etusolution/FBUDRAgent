'''
Created on 2015/3/31


@author: indichen
'''

import sys
import os
import sqlite3
from FBUDRAgentUtil import Singleton
from FBUDRAgentUtil import Logger
from __builtin__ import True

class UDRCacheDB(object):
    '''
    Cache DB
    '''
    __metaclass__ = Singleton    

    ''' Constants '''
    __defDBPath =  os.path.join(os.path.dirname(__file__), 'FBUDRAgent.db')
    __keyDBSchemaVer = 'DBSchemaVer'
    __DBSchemaVer = 1

    def __init__(self):
        self.__conn = None
        
    def InitDB(self):
        needInitTables = True
        if (os.path.exists(self.__defDBPath)):
            needInitTables = False

        self.__conn = sqlite3.connect(self.__defDBPath)
        
        if (needInitTables):
            try:
                cursor = self.__conn.cursor()
                cursor.execute('''
                    create table Info
                        (key char[255],
                         strvalue char[255],
                         numvalue long)
                ''')

                # Init default values                
                cursor.execute('''
                    insert into Info (key, numvalue) values('{0}', '{1}')
                '''.format(self.__keyDBSchemaVer, self.__DBSchemaVer)
                )
                self.__conn.commit()
            except:
                Logger.ErrorLog('Failed to init database')
                self.__conn.rollback()
                os.remove(self.__defDBPath)
                return False
        
        return True

    def getDBSchemaVer(self):
        assert(self.__conn)
        cursor = self.__conn.execute('''
            select numValue from Info where key='{0}'
        '''.format(self.__keyDBSchemaVer)
        )
        
        ver = cursor.fetchone()[0]
        return ver
        
        
    ''' Internal Class: UDRCatchData '''
    class UDRCacheData:
        ''' Cache Data '''
        
        ''' Constants '''
        __tblprefixData = 'CacheData_'
        __keyprefixLastFetchTS = 'LastFetchTS_'
        
        def __init__(self, conn, name):
            assert(conn)
            assert(len(name)>0)

            
            self.__conn = conn
            self.__name = name
            self.__dbname = self.__tblprefixData + self.__name
            self.__keyLastFetchTS = self.__keyprefixLastFetchTS + name
            if (self.__isTblExist()): return

            try:
                cursor = self.__conn.cursor()
                cursor.execute('''
                    create table {0}
                        (fbid char[255],
                         data text)
                '''.format(self.__dbname)
                )
                
                cursor.execute('''
                    insert into Info (key, numvalue) values ('{0}', 0) 
                '''.format(self.__keyLastFetchTS)
                )                
                self.__conn.commit()

            except:
                Logger.ErrorLog('Failed to init cache data db {0}'.format(name))
                self.__conn.rollback()
                print 'Exception catched:\nType: %s\nDesc:%s' % (sys.exc_info()[0], sys.exc_info()[1])

        def __isTblExist(self):
            assert(self.__conn)
            
            cursor = self.__conn.cursor()
            ret = False
            try:
                cursor.execute('''
                    select * from sqlite_master where name='{0}' and type='table'
                '''.format(self.__dbname)
                )
                if (cursor.fetchone()):
                    ret = True
            except:
                print 'Exception catched:\nType: %s\nDesc:%s' % (sys.exc_info()[0], sys.exc_info()[1])
            
            return ret
        
        def setLastFetchTS(self, ts):
            assert(self.__conn)
            self.__conn.execute('''
                update Info set numValue='{1}' where key='{0}'
            '''.format(self.__keyLastFetchTS, long(ts))
            )
            
        def getLastFetchTS(self):
            assert(self.__conn)
            cursor = self.__conn.execute('''
                select numValue from Info where key='{0}'
            '''.format(self.__keyLastFetchTS)
            )
            
            ts = cursor.fetchone()[0]
            return ts

        def setData(self, data):
            assert(data)
            if len(data)<=0: return True
            
            cursor = self.__conn.cursor()
            try:
                cursor.executemany('''
                        insert into {0} values (?, ?)
                    '''.format(self.__dbname), data
                )
                self.__conn.commit()
            except:
                Logger.ErrorLog('Failed to insert cache data {0}'.format(self.__dbname))
                self.__conn.rollback()
                print 'Exception catched:\nType: %s\nDesc:%s' % (sys.exc_info()[0], sys.exc_info()[1])
                return False
            
            return True
        
        def getData(self):
            ret = []
            cursor = self.__conn.cursor()
            try:
                cursor.execute('''
                    select fbid, data from {0}
                '''.format(self.__dbname)
                )
                ret = cursor.fetchall()
            except:
                Logger.ErrorLog('Failed ot get cache data {0}'.format(self.__dbname))
                print 'Exception catched:\nType: %s\nDesc:%s' % (sys.exc_info()[0], sys.exc_info()[1])
                return []
            
            return ret
        
        def clearData(self):
            self.__conn.execute('''
                delete from {0}
            '''.format(self.__dbname)
            )
            return True
        
        def getName(self):
            return self.__name
    # class UDRCacheData

    def createCacheData(self, name):
        ret = self.UDRCacheData(self.__conn, name)
        return ret
# End of class UDRCacheDB
