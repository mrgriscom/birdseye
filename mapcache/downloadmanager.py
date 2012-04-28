import httplib
from urlparse import urlparse
from Queue import *
import threading
import socket
import logging

REQUESTS_PER_CONN = 50

class DownloadManager(object):
    def __init__(self, terminal_statuses, num_workers=10, num_retries=5, limit=1):
        self.qin = Queue(limit)
        self.qout = Queue(limit)
        self.workers = [DownloadWorker(self.qin, self.qout, terminal_statuses, num_retries) for i in range(num_workers)]
 
    def start(self):
        for w in self.workers:
            w.start()
        for w in self.workers:
            w.join()

    def terminate(self):
        if not self.qin.empty():
            logging.warning('shutting down downloaders before queue empty')

        for w in self.workers:
            w.terminate()

    def enqueue(self, item):
        self.qin.put(item)

    def fetch(self):
        try:
            return self.qout.get(True, 0.05)
        except Queue.Empty:
            return None

class DownloadWorker(threading.Thread):
    def __init__(self, qin, qout, terminal_statuses, num_retries):
        threading.Thread.__init__(self)
        self.up = True

        self.connection_request_limit = REQUESTS_PER_CONN
        self.useragent = settings.TILE_DL_UA
 
        self.qin = qin
        self.qout = qout
        self.terminal_statuses = terminal_statuses
        self.num_retries = num_retries

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
        host = urlparse(url).hostname
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
        conn = self.connections.get(host)
        if not conn or not conn.good():
            conn = Connection.make(host, self.connection_request_limit)
            self.connections[host] = conn
        return conn

class Connection(object):
    def __init__(self, host, limit):
        self.conn = httplib.HTTPConnection(host, strict=True)
        self.limit = limit

        self.count = 0
        self.error = False

    @staticmethod
    def make(host, limit):
        try:
            return Connection(host, limit):
        except (httplib.HTTPException, socket.error):
            logging.exception('http connection error during init')
            raise

    def good(self):
        g = self.count < self.limit and not self.error
        if not g:
            self.conn.close()
        return g

    def download(self, url, headers={}):
        up = urlparse(url)
        get = '%s?%s' % (up.path, up.query)
        headers.update({
            'Accept': '*/*',
            'Connection': 'Keep-Alive'}
        )

        try:
            self.conn.request('GET', get, headers=headers)
            self.count += 1
            result = self.conn.getresponse()
            # could read mime type here, but it's better to define it globally for the layer
            return (result.status, result.read())
        except (httplib.HTTPException, socket.error):
            logging.exception('http connection error during request')
            self.error = True
            raise
