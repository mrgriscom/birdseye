import httplib
from urlparse import urlparse
from Queue import *
import threading
import socket
import logging
import settings

REQUESTS_PER_CONN = 50

class DownloadManager(object):
    """a frontend for many downloading worker threads. this class is not actually
    a thread! it just mimics one (to manage the worker threads)
    """

    def __init__(self, terminal_statuses, num_workers=10, num_retries=5, limit=1):
        """
        limit -- maximum buffer for processing; this prevents worker threads downloading
          items faster than they can be processed and filling up all memory
        """
        self.qin = Queue(limit)
        self.qout = Queue(limit)
        self.workers = [DownloadWorker(self.qin, self.qout, terminal_statuses, num_retries) for i in range(num_workers)]
 
    def start(self):
        """start all workers"""
        for w in self.workers:
            w.start()

    def terminate(self):
        """kill all workers"""
        if not self.qin.empty():
            logging.warning('shutting down downloaders before queue empty')

        for w in self.workers:
            w.terminate()

    def join(self):
        """block until all workers have terminated"""
        for w in self.workers:
            w.join()

    def enqueue(self, item):
        """add a item to download"""
        self.qin.put(item)

    def fetch(self):
        """retrieve a downloaded item for processing"""
        try:
            return self.qout.get(True, 0.05)
        except Empty:
            return None

class DownloadWorker(threading.Thread):
    """a downloading worker thread"""

    def __init__(self, qin, qout, terminal_statuses, num_retries):
        """
        terminal_statuses -- consider the download 'complete' if any of these statuses
          received, else, do a retry
        num_retries -- number of retries before giving up
        """
        threading.Thread.__init__(self)
        self.up = True

        self.connection_request_limit = REQUESTS_PER_CONN
        self.useragent = settings.TILE_DL_UA
 
        self.qin = qin
        self.qout = qout
        self.terminal_statuses = terminal_statuses
        self.num_retries = num_retries

        # mapping of open connection to each host
        self.connections = {}

    def terminate(self):
        self.up = False

    def run(self):
        try:
            while self.up:
                try:
                    item = self.qin.get(True, 0.05)
                    self.download(item)
                except Empty:
                    pass
        except:
            logging.exception('unexpected exception in download worker thread')

    def download(self, (key, url)):
        """download a single item and place in 'out' queue for processing"""
        host = urlparse(url).netloc
        headers = {'User-Agent': self.useragent}

        for t in range(self.num_retries):
            try:
                status, data = self.get_connection(host).download(url, headers)
            except Exception, e:
                status, data = None, '%s: %s' % (type(e), e)

            if status in self.terminal_statuses:
                break

        self.qout.put((key, status, data))

    def get_connection(self, host):
        """get persistent connection to host, or (re-)initialize if necessary"""
        conn = self.connections.get(host)
        if not conn or not conn.good():
            conn = Connection.make(host, self.connection_request_limit)
            self.connections[host] = conn
        return conn

class Connection(object):
    """a persistent, keep-alive http connection"""

    def __init__(self, host, limit):
        """
        host -- e.g., c.mapserver.org:8080
        limit -- maximum requests on this connection before discarding
        """
        self.conn = httplib.HTTPConnection(host, strict=True)
        self.limit = limit

        self.count = 0
        self.error = False

    @staticmethod
    def make(host, limit):
        try:
            return Connection(host, limit)
        except (httplib.HTTPException, socket.error):
            logging.exception('http connection error during init')
            raise

    def good(self):
        """return whether connection is still usable; clean up if not"""
        g = self.count < self.limit and not self.error
        if not g:
            self.conn.close()
        return g

    def download(self, url, headers={}):
        """execute a download request on this connection"""
        up = urlparse(url)
        get = '%s?%s' % (up.path, up.query)
        headers.update({
            'Accept': '*/*',
            'Connection': 'Keep-Alive'
        })

        try:
            self.conn.request('GET', get, headers=headers)
            self.count += 1
            result = self.conn.getresponse()
            # could read mime type here, but it's better to define it globally for the layer
            return (result.status, result.read())
        except (httplib.HTTPException, socket.error), e:
            if not isinstance(e, httplib.BadStatusLine):
                logging.exception('http connection error during request')
            self.error = True
            raise
