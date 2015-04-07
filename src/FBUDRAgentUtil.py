'''
Created on 2015/3/30

@author: indichen
'''
from _pyio import __metaclass__

import time

'''
Singleton class
- It is used as the singleton metaclass
'''
class Singleton(type):
    
    def __init__(cls, name, bases, diction):
        super(Singleton, cls).__init__(name, bases, diction)
        cls.instance = None 

    def __call__(cls,*args,**kw):
        if cls.instance is None:
            cls.instance = super(Singleton, cls).__call__(*args, **kw)
        return cls.instance

class Logger(object):
    @classmethod
    def ErrorLog(cls, log):
        ts = time.strftime('%Y%m%d %H:%M:%S')
        print "[Error] %s %s" % (ts, log)
    
    @classmethod
    def DbgLog(cls, log):
        ts = time.strftime('%y%m%d-%H:%M:%S')
        print "[Debug] %s %s" % (ts, log)
