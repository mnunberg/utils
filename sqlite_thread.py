#!/usr/bin/env python

from threading import Thread, Lock, Event
from Queue import Queue
from sqlite3 import dbapi2
import string
import time
import random

class DBProxyCursor(object):
    def __init__(self, dbthread):
        self.lock = Lock()
        self.dbthread = dbthread
        
    def _communicate(self, fn, *args):
        self.lock.acquire()
        rq = Queue()
        msg = (fn, args, rq, self.lock)
        self.dbthread.q.put(msg)
        self.lock.acquire()
        assert not rq.empty()
        self.lock.release()
        ret = rq.get()
        if isinstance(ret, Exception):
            raise ret
        return ret
    
    def execute(self, *args):
        "Executes a command"
        ret = self._communicate(self.dbthread.cursor.execute, *args)
        self.cursor = ret
        return self
    def executemany(self, *args):
        ret = self._communicate(self.dbthread.cursor.executemany, *args)
        self.cursor = ret
        return self
    
    def _fetch(self, all=False):
        "Fetches a single row  as a list"
        if not self.cursor:
            return []
        if all:
            fn = self.cursor.fetchall
        else:
            fn = self.cursor.fetchone
        return self._communicate(fn)
    def fetchone(self):
        return self._fetch()
    def fetchall(self):
        return self._fetch(all=True)
    def commit(self):
        return self._communicate(self.dbthread.conn.commit)


class DBThread(Thread):
    "Blocking, synchronous thread"
    DBSCHEMA = "CREATE TABLE foo (bar TEXT PRIMARY KEY);"
    def __init__(self, dbfile=":memory:"):
        Thread.__init__(self)
        self.q = Queue(1)
        #put something on the queue, so nothing else can use it
        self.do_close = False
        self.setDaemon(True)
        self.cursor = None
        self.dbfile = dbfile
        self.done = False
        self.start()
        while not self.done:
            time.sleep(0.0001)
        #wait for cursor to become available...
    def run(self):
        #put something on the queue
        self.conn = dbapi2.connect(self.dbfile)
        self.cursor = self.conn.cursor()
        self.cursor.executescript(self.DBSCHEMA)
        self.conn.commit()
        self.done = True
        while True:
            msg = self.q.get()
            fn, args, consumer_q, rq_lock = msg
            try:
                ret = fn(*args)
            except dbapi2.Error, e:
                ret = e
            except Exception, e:
                print "OOPS! in %r(%r)" % (fn, args)
                raise
            consumer_q.put(ret)
            rq_lock.release()


if __name__ == "__main__":
    class Accessor(Thread):
        "a thread that wants to store/retrieve information from the DB"
        def run(self):
            def randyielder():
                for i in xrange(1000):
                    yield ("".join(random.sample(string.ascii_letters, 10)),)
                    
            randiter = randyielder()
            c = DBProxyCursor(dbthread)
            
            try:
                c.executemany("INSERT INTO foo(bar) VALUES (?)",randiter)
                c.commit()
            except dbapi2.IntegrityError, e:
                print "Hrrm.. already exists:", e
            ret = c.execute("SELECT * FROM foo")
            rows = ret.fetchall()
            print "Got %d rows" % (len(rows),)


    dbthread = DBThread()
    print "initialized"
    
    threadlist = []
    for i in xrange(10):
        t = Accessor()
        threadlist.append(t)
        t.start()
    
    for t in threadlist[:]:
        t.join()
        threadlist.remove(t)
        #print "%d threads remaining" % (len(threadlist))