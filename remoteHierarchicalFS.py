#!/usr/bin/env python

import logging, xmlrpclib, pickle, hashlib, random

from xmlrpclib import Binary
from collections import defaultdict, Counter
from errno import ENOENT
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

if not hasattr(__builtins__, 'bytes'):
	bytes = str

class Memory(LoggingMixIn, Operations):
	"""Implements a hierarchical file system by using FUSE virtual filesystem.
	   All the data is stored on a remote server.  
	   The data is not lost even when the client disconnects from the server."""

	def __init__(self, qr, qw, mport, dports):
		"""Connect to xmlrpc server hosted by simpleht.py at localhost:51234.
		   TTL is set to 3000 seconds.
		   Tries to retrieve the file structure from server. If not found, mounts an empty directory."""
		self.qr = int(qr)
		self.qw = int(qw)
		if self.qr > len(dports):
			print "Qr greater than Nreplicas, Exiting..."
			exit(1)
		if self.qr < 1:
			print "Qr should be between 1 and Nreplicas"
			exit(1)
		if self.qw != len(dports):
			print "Qw not equal to the Nreplicas, Exiting..."
			exit(1) # exit fatal
		self.mrpc = xmlrpclib.ServerProxy("http://localhost:" + str(int(mport)) + '/')
		self.drpc= [ xmlrpclib.ServerProxy("http://localhost:" + str(int(dport)) + '/') for dport in dports ]
		self.ttl = 3000
		self.fd = 0
		self.mrpc.delete_all()
		for d in self.drpc:
			d.delete_all()
		now = time()
		self.putmeta('/', dict(st_mode=(S_IFDIR | 0755), st_ctime=now, st_mtime=now, st_atime=now, st_nlink=2, files=[]) )
		# try:
		#     self.getmeta('/')
		# except KeyError:
		#     # The key files holds a dict of filenames(and their attributes and 'files' if it is a directory) under each level
		#     self.putmeta('/', dict(st_mode=(S_IFDIR | 0755), st_ctime=now, st_mtime=now, st_atime=now, st_nlink=2, files=[]) )
		
	def md5sum(self,path):
		"""Returns the md5 hash of path, which is used as the key for storing data on the server"""
		return hashlib.md5(path).hexdigest()

	def putmeta(self, path, meta):
		"""Sends the dictionary self.files to the server"""
		try:
			if self.mrpc.put(Binary(self.md5sum(path)),Binary(pickle.dumps(meta)),self.ttl):
				return True
		except Exception, e:
			print "ERROR in putmeta", e
			return False

	def getmeta(self, path):
		"""Retrieves the dictionary self.files from server"""
		return pickle.loads(self.mrpc.get(Binary(self.md5sum(path)))['value'].data)

	def getdata(self, path):
		"""Gets the data stored in particular file from the server by using md5 hash of the path as the key"""
		l = range(len(self.drpc))
		random.shuffle(l)
		da = []
		for i in l:
			try:
				da.append(pickle.loads(self.drpc[i].get(Binary(self.md5sum(path)))['value'].data))
				if len(da) >= self.qr:
					break
			except Exception, e:
				print "ERROR in getdata", e
				print "Server", i, "didn't reply!"
		if len(da) < self.qr:
			return ""
		chs = [ self.md5sum(d) for d in da ]
		cc = Counter(chs)
		mc = cc.most_common(2)
		print len(da),mc
		if len(mc) == 1:
			return da[chs.index(mc[0][0])]
		elif mc[0][1] > mc[1][1]:
			return da[chs.index(mc[0][0])]
		else:
			return ""

		
	def putdata(self, path, data):
		"""Sends the data of particular file to the server and stores it in a dict with a key equal to the md5 hash of path of the file"""
		for i in range(len(self.drpc)):
			try:
				self.drpc[i].put(Binary(self.md5sum(path)),Binary(pickle.dumps(data)),self.ttl)
			except Exception, e:
				print "ERROR in putdata", e
				return False
		return True

	def deletemeta(self,path):
		try:
			if self.mrpc.delete(Binary(self.md5sum(path))):
				return True
		except Exception, e:
			print "ERROR in deletemeta", e
			return False

	def deletedata(self,path):
		for i in range(len(self.drpc)):
			try:
				self.drpc[i].delete(Binary(self.md5sum(path)))
			except Exception, e:
				print "ERROR in deletedata", e
				return False
		return True

	def splitpath(self, path):
		childpath = path[path.rfind('/')+1:]
		parentpath = path[:path.rfind('/')]
		if parentpath == '':
			parentpath = '/'
		return parentpath, childpath

	def chmod(self, path, mode):
		p = self.getmeta(path)
		p['st_mode'] &= 0770000
		p['st_mode'] |= mode
		self.putmeta(path,p)
		return 0

	def chown(self, path, uid, gid):
		p = self.getmeta(path)
		p['st_uid'] = uid
		p['st_gid'] = gid
		self.putmeta(path,p)

	def create(self, path, mode):
		if self.putdata(path,""):
			self.putmeta(path,dict(st_mode=(S_IFREG | mode), st_nlink=1,
						 st_size=0, st_ctime=time(), st_mtime=time(),
						 st_atime=time()))
			ppath,cpath = self.splitpath(path)
			pp = self.getmeta(ppath)
			pp['files'].append(cpath)
			self.putmeta(ppath,pp)
			self.fd += 1
			return self.fd
		else:
			return False

	def getattr(self, path, fh=None):
		try:
			p = self.getmeta(path)
		except KeyError:
			raise FuseOSError(ENOENT)
		return p

	def getxattr(self, path, name, position=0):
		p = self.getmeta(path)
		attrs = p.get('attrs', {})
		try:
			return attrs[name]
		except KeyError:
			return ''       # Should return ENOATTR

	def listxattr(self, path):
		p = self.getmeta(path)
		attrs = p.get('attrs', {})
		return attrs.keys()

	def mkdir(self, path, mode):
		ppath, cpath = self.splitpath(path)
		self.putmeta(path, dict(st_mode=(S_IFDIR | mode), st_nlink=2,
								st_size=0, st_ctime=time(), st_mtime=time(),
								st_atime=time(),files=[]))
		pp = self.getmeta(ppath)
		pp['st_nlink'] += 1
		pp['files'].append(cpath)
		self.putmeta(ppath,pp)

	def open(self, path, flags):
		self.fd += 1
		return self.fd

	def read(self, path, size, offset, fh):
		ff = self.getdata(path)
		return ff[offset:offset + size]

	def readdir(self, path, fh):
		p = self.getmeta(path)['files']
		return ['.', '..'] + [x for x in p ]

	def readlink(self, path):
		return self.getdata(path)

	def removexattr(self, path, name):
		p = self.getmeta(path)
		attrs = p.get('attrs', {})
		try:
			del attrs[name]
			self.putmeta(path,p)
		except KeyError:
			pass        # Should return ENOATTR

	def rename(self, old, new):
		oppath,ocpath = self.splitpath(old)
		nppath,ncpath = self.splitpath(new)
		o = self.getmeta(old)
		op = self.getmeta(oppath)
		if oppath != nppath:
			np = self.getmeta(nppath)
		if o['st_mode'] & 0770000 == S_IFDIR:
			for f in o['files']:
				oc = o.copy()
				oc['files'] = []
				self.putmeta(new,oc)
				self.rename(old + '/' + f, new + '/' + f)
			if oppath != nppath:
				op['st_nlink'] -= 1
				np['st_nlink'] += 1
		else:
			self.putdata(new,self.getdata(old))
			self.deletedata(old)
		self.deletemeta(old)
		self.putmeta(new,o)
		op['files'].remove(ocpath)
		if oppath != nppath:
			np['files'].append(ncpath)
			self.putmeta(nppath,np)
		else:
			op['files'].append(ncpath)
		self.putmeta(oppath,op)

	def rmdir(self, path):
		ppath, cpath = self.splitpath(path)
		p = self.getmeta(path)
		if p['st_mode'] & 0770000 == S_IFDIR:
			for f in p['files']:
				c = self.getmeta(path + '/' + f)
				if c['st_mode'] & 0770000 == S_IFDIR:
					self.rmdir(path + '/' + f)
				else:
					if not self.deletedata(path + '/' + f):
						raise FuseOSError(ENOENT)
		self.deletemeta(path)
		pp = self.getmeta(ppath)
		pp['files'].remove(cpath)
		pp['st_nlink'] -= 1
		self.putmeta(ppath,pp)

	def setxattr(self, path, name, value, options, position=0):
		# Ignore options
		p = self.getmeta(path)
		attrs = p.setdefault('attrs', {})
		attrs[name] = value
		self.putmeta(path,p)

	def statfs(self, path):
		return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)

	def symlink(self, target, source):
		self.putmeta(target, dict(st_mode=(S_IFLNK | 0777), st_nlink=1,
								  st_size=len(source)))
		ppath, cpath = self.splitpath(path)
		p = self.getmeta(ppath)
		p['files'].append(cpath) 
		self.putmeta(ppath,p)
		self.putdata(target,source)

	def truncate(self, path, length, fh=None):
		self.putdata(path,self.getdata(path)[:length])
		p = self.getmeta(path)
		p['st_size'] = length
		self.putmeta(path,p)

	def unlink(self, path):
		if self.deletedata(path):
			self.deletemeta(path)
			ppath, cpath = self.splitpath(path)
			p = self.getmeta(ppath)
			p['files'].remove(cpath)
			self.putmeta(ppath,p)
		else:
			raise FuseOSError(ENOENT)

	def utimens(self, path, times=None):
		now = time()
		atime, mtime = times if times else (now, now)
		p = self.getmeta(path)
		p['st_atime'] = atime
		p['st_mtime'] = mtime
		self.putmeta(path,p)

	def write(self, path, data, offset, fh):
		p = self.getmeta(path)
		if self.putdata(path,self.getdata(path)[:offset] + data):
			p['st_size'] = len(self.getdata(path))
			self.putmeta(path,p)
			return len(data)
		else:
			return 0


if __name__ == '__main__':
	if len(argv) < 6:
		print('usage: %s <mountpoint> <qr> <qw> <meta server port> <data server ports>' % argv[0])
		exit(1)
	dports = argv[5:]
	logging.getLogger().setLevel(logging.DEBUG)
	fuse = FUSE(Memory(argv[2],argv[3],argv[4], dports), argv[1], foreground=True)
