import httplib
from urlparse import urlparse
from Queue import *
import threading
import socket

class DownloadManager ():
  def __init__ (self, terminal_statuses, num_workers=10, num_retries=5, limit=-1, errs=None):
    self.in_queue = Queue(limit)
    self.out_queue = Queue(limit)
    self.count = 0

    self.workers = [DownloadWorker(self.in_queue, self.out_queue, terminal_statuses, num_retries, errs) for i in range(0, num_workers)]
 
  def start (self):
    for w in self.workers:
      w.start()

  def terminate (self):
    if not self.done():
      print "Warning: queue not empty"

    for w in self.workers:
      w.terminate()

  def enqueue (self, item):
    self.in_queue.put(item)
    self.count += 1

  def done (self):
    return self.in_queue.empty()

  def status (self):
    num_done = self.count - self.in_queue.qsize()
    return (num_done, self.count)

class DownloadWorker (threading.Thread):
  def __init__ (self, in_queue, out_queue, terminal_statuses, num_retries, errs=None):
    threading.Thread.__init__(self)
    self.up = True
    self.errors = errs

    self.connection_request_limit = 50
    self.useragent = "Mozilla/5.0 (X11; U; Linux i686; en-US) Gecko/20080208 Firefox/2.0.0.13"
 
    self.in_queue = in_queue
    self.out_queue = out_queue
    self.terminal_statuses = terminal_statuses
    self.num_retries = num_retries

    self.connections = {}

  def terminate (self):
    self.up = False

  def run (self):
    while self.up:
      try:
        item = self.in_queue.get(True, 0.05)
        self.download(item)
      except Empty:
        pass
      except Exception, e:
        if self.errors != None:
          self.errors.put('Unexpected exception in download worker thread: ' + str(e))
        #todo: work within logging framework

  def download (self, (val, url)):
    host = urlparse(url)[1] #.hostname
    headers = {'User-Agent': self.useragent}

    for t in range(0, self.num_retries):
      (status, data) = download(url, self.get_connection(host), headers)
      if status in self.terminal_statuses:
        break

    #todo: log download
    self.out_queue.put((val, status, data))

  def get_connection (self, host):
    conn = None
    if host in self.connections:
      conn = self.connections[host]
      if conn['error'] or conn['count'] == self.connection_request_limit:
        conn['conn'].close()
        del self.connections[host]
        conn = None

    return conn if conn else self.new_connection(host)

  def new_connection (self, host):
    try:
      conn = dict(conn=httplib.HTTPConnection(host, strict=True), count=0, error=False)
      self.connections[host] = conn
      return conn
    except (httplib.HTTPException, socket.error):
      return None #log?

def download (url, conn, headers={}):
  if conn:
    up = urlparse(url)
    get = "%s?%s" % (up[2], up[4]) #(up.path, up.query)
    headers.update({'Accept': '*/*', 'Connection': 'Keep-Alive'})

    try:
      conn['conn'].request("GET", get, headers=headers)
      conn['count'] += 1
      result = conn['conn'].getresponse()
      return (result.status, result.read())
    except (httplib.HTTPException, socket.error):
      conn['error'] = True #log?
 
  return (None, None)
