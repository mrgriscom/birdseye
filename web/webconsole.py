import sys
import os.path

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from tornado.ioloop import IOLoop
import tornado.web as web
import tornado.gen as gen
from tornado.httpclient import HTTPError
from tornado.template import Template
import logging
import os
import settings
import json
from datetime import datetime
import time
import email

from mapcache import maptile as mt
from mapcache import mapdownload as md
import nav.texture

from sqlalchemy import func

class LayersHandler(web.RequestHandler):
    """information about available layers"""

    def initialize(self, dbsess=None):
        self.sess = dbsess

    def get(self):
        deflayer = None
        if self.sess:
            # set the layer with the most tiles at the initial zoom level as the default layer
            defzoom = int(self.get_argument('default_zoom', '0'))
            tallies = list(self.sess.query(func.count('*'), mt.Tile.layer).filter(mt.Tile.z == defzoom).group_by(mt.Tile.layer))
            if tallies:
                deflayer = max(tallies)[1]

        def mk_layer(key):
            L = settings.LAYERS[key]
            info = {
                'id': key,
                'name': L.get('name', key),
                'overlay': L.get('overlay', False),
            }

            url_spec = L['tile_url']
            if hasattr(url_spec, '__call__'):
                url_spec = url_spec()
            if hasattr(url_spec, '__call__'):
                url_spec = '{custom:%s}' % key
            url_spec = L.get('file_type', '').join(url_spec.split('{type}'))
            info['url'] = url_spec

            if key == deflayer:
                info['default'] = True

            return info

        payload = sorted((mk_layer(k) for k in settings.LAYERS.keys()), key=lambda l: l['name'])
        self.set_header('Content-Type', 'text/json')
        self.write(json.dumps(payload))

class TileRequestHandler(web.RequestHandler):

    def get(self, layer, z, x, y):
        self._get(mt.Tile(layer=layer, z=int(z), x=int(x), y=int(y)))

    def return_static(self, layer, content, digest, modtime):
        self.set_header('Cache-Control', 'public')
        if modtime:
            self.set_header('Last-Modified', modtime)
        self.set_header('Etag', '"%s"' % digest)

        req_etag = self.request.headers.get('If-None-Match')
        if req_etag and digest in req_etag:
            self.set_status(304)
            return
        req_ims = self.request.headers.get('If-Modified-Since')
        if req_ims is not None and modtime:
            if_since = datetime.fromtimestamp(time.mktime(email.utils.parsedate(req_ims)))
            if if_since >= modtime:
                self.set_status(304)
                return
        
        if content:
            file_type = settings.LAYERS[layer]['file_type']
            self.set_header('Content-Type', 'image/' + file_type)
            self.set_header('Content-Length', len(content))
            self.write(content)
        else:
            self.set_status(404)

class TileHandler(TileRequestHandler):
    """return tile images"""

    def initialize(self, dbsess):
        self.sess = dbsess

    def _get(self, tile):
        t = sess.query(mt.Tile).get(tile.pk())
        if not t:
            self.set_status(404)
            return

        self.return_static(
            t.layer,
            t.load(self.sess) if not t.is_null() else None,
            t.uuid,
            t.fetched_on
        )

class TileURLHandler(TileRequestHandler):
    """return mapserver tile url for layer"""

    def _get(self, tile):
        self.set_header('Content-Type', 'text/plain')
        self.write(tile.url())

class TileProxyHandler(TileRequestHandler):

    def initialize(self, tiledl):
        self.tiledl = tiledl
    
    @web.asynchronous
    @gen.engine
    def _get(self, tile):
        cache = (self.get_argument('cache', None) == 'true')
        overwrite = (self.get_argument('overwrite', None) == 'true')

        def async(callback):
            self.tiledl.add({
                    'tile': tile,
                    'callback': callback,
                    'cache': cache,
                    'overwrite': overwrite,
                }, tile.url())
        data = yield gen.Task(async)

        self.return_static(
            tile.layer,
            data,
            md.digest(data),
            datetime.utcnow()
        )
        self.finish()

class TileCoverHandler(TileRequestHandler):
    """return metadata describing the coverage over this tile at other zoom levels"""

    def initialize(self, dbsess):
        self.sess = dbsess

    def get(self, tile):
        desc = tile.get_descendants(self.sess, 8)
        def rel_tile(t):
            zdiff = t.z - tile.z
            return {'z': zdiff, 'x': t.x - tile.x * 2**zdiff, 'y': t.y - tile.y * 2**zdiff}
        payload = [rel_tile(t) for t in desc]

        self.set_header('Content-Type', 'text/json')
        self.write(json.dumps(payload))

class RootContentHandler(web.StaticFileHandler):
    def get(self):
        super(RootContentHandler, self).get('map.html')


def tile_fetch_callback(meta, status, data):
    IOLoop.instance().add_callback(lambda: meta['callback'](md.normdata(status, data)))


sess = mt.dbsess()
tiledl = md.DownloadService(tile_fetch_callback, sess)
application = web.Application([
    (r'/layers', LayersHandler, {'dbsess': sess}),
    (r'/tile/([A-Za-z0-9_-]+)/([0-9]+)/([0-9]+),([0-9]+)', TileHandler, {'dbsess': sess}),
    (r'/tileproxy/([A-Za-z0-9_-]+)/([0-9]+)/([0-9]+),([0-9]+)', TileProxyHandler, {'tiledl': tiledl}),
    (r'/tileurl/([A-Za-z0-9_-]+)/([0-9]+)/([0-9]+),([0-9]+)', TileURLHandler),
    (r'/tilecover/([A-Za-z0-9_-]+)/([0-9]+)/([0-9]+),([0-9]+)', TileCoverHandler, {'dbsess': sess}),
    (r'/', RootContentHandler, {'path': os.path.join(project_root, 'web/static')}),
    (r'/(.*)', web.StaticFileHandler, {'path': os.path.join(project_root, 'web/static')}),
])

if __name__ == "__main__":

    try:
        port = int(sys.argv[1])
    except IndexError:
        port = 8000

    application.listen(port)

    try:
        IOLoop.instance().start()
        print 'hereeee'
    except KeyboardInterrupt:
        pass
    except Exception, e:
        print e
        raise

    logging.info('shutting down...')

    tiledl.terminate()
