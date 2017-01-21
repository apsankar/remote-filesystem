#!/usr/bin/env python

import logging
from pymongo import MongoClient
import pymongo, memcache, inspect, Queue, threading

from errno import ENOENT

from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

if not hasattr(__builtins__, 'bytes'):
    bytes = str

class CacheSync(threading.Thread):

    def __init__(self,que,e):
        threading.Thread.__init__(self)
        self.que = que
        self.e = e
        for i in threading.enumerate():
            if i.name == "MainThread":
                self.mt = i

    def run(self):
        while self.mt.isAlive() or (not self.que.empty()):
            while not self.que.empty():
                x = self.que.get()
                x[0](x[1],x[2])
                self.que.task_done()
            else:
                if self.e.wait(1):
                    self.e.clear()
        else:
            if self.e.wait(1):
                self.e.clear()


class CacheDB():
    """ Proposed method to handle cache
    insert_one - insert in memcache and queue write through

    find - if direct match -> find from the memcache, if doesnt exist find and return from mongodb and insert into memcache
           if regex -> find and return from mongodb and insert into memcache

    update_one - update in cache and queue write through

    find_one_and_update - direct match -> find from memcache and update, else find and update from mongodb and insert into memcache

    delete_one - delete in memcache if exists, queue write through

    delete_many - delete in memcache if exists, queue write through
    """
    def __init__(self,numFilesCache):
        self.client = MongoClient()
        self.db = self.client.db
        self.files = self.db.files
        self.numFilesCache = numFilesCache
        self.c = memcache.Client(['127.0.0.1:11211'], debug=False)
        self.q = Queue.Queue()
        self.e = threading.Event()
        self.cac = CacheSync(self.q, self.e)
        self.cac.start()
        self.cachefiles = {}
        self.files.drop()

    def create_index(self,*args,**kwargs):
        return self.files.create_index(*args,**kwargs)

    def insert_one(self,*args,**kwargs):
        return self.files.insert_one(*args,**kwargs)

    def find(self,*args,**kwargs):
        if str(args[0]['path']) in self.cachefiles.keys():
            self.cachefiles[args[0]['path']] = time()
            return [ self.c.get(args[0]['path']) ]
        else:
            if inspect.stack()[1][3] in ['write','truncate','read','readlink']:
                x = self.files.find({'path':args[0]['path']})[0]
                if self.cachefiles.__len__() >= self.numFilesCache:
                    de = min(self.cachefiles, key=self.cachefiles.get)
                    del( self.cachefiles[de] )
                    self.c.delete(de)
                self.c.set(args[0]['path'],x)
                self.cachefiles[args[0]['path']] = time()
                return [x]
            else:
                return self.files.find(*args,**kwargs)

    def update_one(self,*args,**kwargs):
        if str(args[0]['path']) in self.cachefiles.keys():
            x = self.c.get(args[0]['path'])
            op = ( args[1].keys()[0] )
            if op == '$set':
                y = args[1]['$set'].keys()[0].split('.')
                if len(y) == 1:
                    x[ y[0] ] = args[1]['$set'][ y[0] ]
                else:
                    x[ y[0] ][ y[1] ] = args[1]['$set'][ args[1]['$set'].keys()[0] ]
            elif op == '$push':
                if '$slice' in args[1]['$push']['data'].keys():
                    x[ 'data' ] = x['data'][ :args[1]['$push']['data']['$slice'] ]
                else:
                    x[ 'data' ] = x['data'] + args[1]['$push']['data']['$each']

            elif op == '$bit':
                if args[1]['$bit']['st_mode'].keys()[0] == 'and' :
                    x['st_mode'] &= args[1]['$bit']['st_mode']['and']
                if args[1]['$bit']['st_mode'].keys()[0] == 'or' :
                    x['st_mode'] |= args[1]['$bit']['st_mode']['or']
            elif op == '$unset':
                del( x['attrs'][ args[1]['$unset']['attrs'].keys()[0] ])
            self.c.set(args[0]['path'],x)
            self.cachefiles[args[0]['path']] = time()
            self.q.put([self.files.update_one,args[0],args[1]])
            self.e.set()
        else:
            if inspect.stack()[1][3] in ['write','truncate']:
                print 'writing into cache'
                x = self.files.find({'path':args[0]['path']})[0]
                op = ( args[1].keys()[0] )
                if op == '$set':
                    y = args[1]['$set'].keys()[0].split('.')
                    if len(y) == 1:
                        x[ y[0] ] = args[1]['$set'][ y[0] ]
                    else:
                        x[ y[0] ][ y[1] ] = args[1]['$set'][ args[1]['$set'].keys()[0] ]
                elif op == '$push':
                    print 'pushing'
                    if '$slice' in args[1]['$push']['data'].keys():
                        x[ 'data' ] = x['data'][ :args[1]['$push']['data']['$slice'] ]
                    else:
                        x[ 'data' ] = x['data'] + args[1]['$push']['data']['$each']
                        print x['data']

                elif op == '$bit':
                    if args[1]['$bit']['st_mode'].keys()[0] == 'and' :
                        x['st_mode'] &= args[1]['$bit']['st_mode']['and']
                    if args[1]['$bit']['st_mode'].keys()[0] == 'or' :
                        x['st_mode'] |= args[1]['$bit']['st_mode']['or']
                elif op == '$unset':
                    del( x['attrs'][ args[1]['$unset']['attrs'].keys()[0] ])

                if self.cachefiles.__len__() >= self.numFilesCache:
                    de = min(self.cachefiles, key=self.cachefiles.get)
                    del( self.cachefiles[de] )
                    self.c.delete(de)
                self.c.set(args[0]['path'],x)
                self.cachefiles[args[0]['path']] = time()

                self.q.put([self.files.update_one,args[0],args[1]])
                self.e.set()
            else:
                return self.files.update_one(*args,**kwargs)

    def find_one_and_update(self,*args,**kwargs):
        if args[0]['path'] in self.cachefiles.keys():
            x = self.c.get(args[0]['path'])
            x['path'] = args[1]['$set']['path']
            self.c.delete(args[0]['path'])
            self.c.set(args[1]['$set']['path'], x)
        return self.files.find_one_and_update(*args,**kwargs)

    def delete_one(self,*args,**kwargs):
        if args[0]['path'] in self.cachefiles.keys():
            self.c.delete(args[0]['path'])
            del(self.cachefiles[args[0]['path']])
        return self.files.delete_one(*args,**kwargs)

    def delete_many(self,*args,**kwargs):
        for i in self.cachefiles.keys():
            if i.startswith(args[0]['path']['$regex'][1:]):
                self.c.delete(i)
                del(self.cachefiles[i])
            # if re.search(args[0]['path']['$regex'],i) != None :
            #     self.c.delete(i)
        return self.files.delete_many(*args,**kwargs)

class Memory(LoggingMixIn, Operations):
    """Implements a hierarchical file system by using FUSE virtual filesystem.
       All the data is stored on a remote server.  
       The data is not lost even when the client disconnects from the server."""

    def __init__(self):
        print '__init__ called'
        """Connect to xmlrpc server hosted by simpleht.py at localhost:51234.
           TTL is set to 3000 ms.
           Tries to retrieve the file structure from server. If not found, mounts an empty directory."""
        self.files = CacheDB(5)
        self.fd = 0
        now = time()
        res = self.files.insert_one({ "path":"/", "st_mode":(S_IFDIR | 0755), 'st_ctime':now, 'st_mtime':now, 'st_atime':now, 'st_nlink':2 })
        self.files.create_index([('path',pymongo.ASCENDING)])

    def chmod(self, path, mode):
        print 'chmod called'
        res = self.files.update_one({'path':path},{'$bit':{'st_mode':{'and':0770000}}})
        res = self.files.update_one({'path':path},{'$bit':{'st_mode':{'or':mode}}})
        return 0

    def chown(self, path, uid, gid):
        print 'chown called'
        res = self.files.update_one({'path':path},{'$set':{'st_uid':uid}})
        res = self.files.update_one({'path':path},{'$set':{'st_gid':gid}})


    def create(self, path, mode):
        print 'create called'
        res = self.files.insert_one({ "path":path, "st_mode":(S_IFREG | mode), 'st_ctime':time(), 'st_mtime':time(), 'st_atime':time(), 'st_nlink':1, 'st_size':0, 'data':[] })
        self.fd += 1
        return self.fd

    def getattr(self, path, fh=None):
        print 'getattr called'
        res = self.files.find({'path':path},{'data':0})
        try:
            p = res[0]
        except:
            raise FuseOSError(ENOENT)
        else:
            del(p['_id'])
            del(p['path'])
            return p

    def getxattr(self, path, name, position=0):
        print 'getxattr called', path,name
        res = self.files.find({'path':path},{'data':0})
        p = res[0]
        attrs = p.get('attrs', {})
        try:
            return attrs[name]
        except KeyError:
            return ''       # Should return ENOATTR

    def listxattr(self, path):
        print 'listxattr called'
        res = self.files.find({'path':path},{'data':0})
        p = res[0]
        attrs = p.get('attrs', {})
        return attrs.keys()

    def mkdir(self, path, mode):
        print 'mkdir called'
        res = self.files.insert_one({"path":path, "st_mode":(S_IFDIR | mode),'st_size':0, 'st_ctime':time(), 'st_mtime':time(), 'st_atime':time(), 'st_nlink':2 })
        pp = path[:path.rfind('/')]
        pp = '/' if pp == '' else pp
        res = self.files.update_one({'path':pp},{'$inc':{'st_nlink':1}})

    def open(self, path, flags):
        print 'open called'
        self.fd += 1
        return self.fd

    def read(self, path, size, offset, fh):
        print 'read called'
        ff = self.files.find({'path':path})[0]['data']
        da = str(''.join(ff[offset:offset + size]))
        print "read - ", da
        return da

    def readdir(self, path, fh):
        print 'readdir called'
        path = '' if len(path) == 1 else path
        res = self.files.find({'path':{'$regex':'^' + path + '/[^/]+$'}},{'data':0})
        return ['.', '..'] + [x['path'][x['path'].rfind('/')+1:] for x in res ]

    def readlink(self, path):
        print 'readlink called'
        da = ''.join(self.files.find({'path':path})[0]['data'])
        return da

    def removexattr(self, path, name):
        print 'removexattr called'
        try:
            res = self.files.update_one({'path':path},{'$unset':{'attrs':{name:''}}})
        except KeyError:    ###
            pass        # Should return ENOATTR

    def rename(self, old, new):
        print 'rename called'
        f = self.files.find_one_and_update({'path':old},{'$set':{'path':new}},projection={'data':0},return_document=pymongo.ReturnDocument.AFTER)
        if f['st_mode'] & 0770000 == S_IFDIR:
            op = old[:old.rfind('/')]
            np = new[:new.rfind('/')]
            res = self.files.update_one({'path':op if len(op) > 0 else '/'},{'$inc':{'st_nlink':-1}})
            res = self.files.update_one({'path':np if len(np) > 0 else '/'},{'$inc':{'st_nlink':1}})
            res = self.files.find({'path':{'$regex':'^'+old+'/'}},{'data':0})
            for i in res:
                op = i['path']
                np = op.replace(old,new,1)
                res = self.files.find_one_and_update({'path':op},{'$set':{'path':np}})

    def rmdir(self, path):
        print 'rmdir called'
        res = self.files.delete_one({'path':path})
        res = self.files.delete_many({'path':{'$regex':'^' + path + '/'}})
        pp = path[:path.rfind('/')]
        res = self.files.update_one({'path':pp if len(pp) > 0 else '/'},{'$inc':{'st_nlink':-1}})

    def setxattr(self, path, name, value, options, position=0):
        print 'setxattr called'
        # Ignore options
        res = self.files.update_one({'path':path},{'$set':{'attrs.'+name:value}})

    def statfs(self, path):
        print 'statfs called'
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)

    def symlink(self, target, source):
        print 'symlink called'
        res = self.files.insert_one({ "path":target, "st_mode":(S_IFLNK | 0777), 'st_nlink':1, 'st_size':len(source),'data':list(source) })

    def truncate(self, path, length, fh=None):
        print 'truncate called'
        res = self.files.update_one({'path':path},{'$push':{'data':{'$each':[],'$slice':length}}})
        res = self.files.update_one({'path':path},{'$set':{'st_size':length}})

    def unlink(self, path):
        print 'unlink called'
        res = self.files.delete_one({'path':path})

    def utimens(self, path, times=None):
        print 'utimens called'
        now = time()
        atime, mtime = times if times else (now, now)
        res = self.files.update_one({'path':path},{'$set':{'st_atime': atime, 'st_mtime': mtime}})


    def write(self, path, data, offset, fh):
        print 'write called'
        res = self.files.update_one({'path':path},{'$push':{'data':{'$each':[],'$slice':offset}}})
        res = self.files.update_one({'path':path},{'$push':{'data':{'$each':list(data)}}}) # ,'$set':{'st_size':offset+len(data)}})
        res = self.files.update_one({'path':path},{'$set':{'st_size':offset+len(data)}})
        return len(data)


if __name__ == '__main__':
    if len(argv) != 2:
        print('usage: %s <mountpoint>' % argv[0])
        exit(1)

    logging.getLogger().setLevel(logging.DEBUG)
    fuse = FUSE(Memory(), argv[1], foreground=True)
