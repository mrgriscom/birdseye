import sys
import os.path

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from tornado.ioloop import IOLoop
import tornado.web as web
from tornado.httpclient import HTTPError
from tornado.template import Template
import logging
import os
import settings
import json
from datetime import datetime
import time
import email

from mapcache import maptile
import nav.texture

class LayersHandler(web.RequestHandler):
    """information about available layers"""

    def get(self):
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

            return info

        payload = sorted((mk_layer(k) for k in settings.LAYERS.keys()), key=lambda l: l['name'])
        self.set_header('Content-Type', 'text/json')
        self.write(json.dumps(payload))

class TileHandler(web.RequestHandler):
    """return tile images"""

    def initialize(self, dbsess):
        self.sess = dbsess

    def get(self, layer, z, x, y):
        z = int(z)
        x = int(x)
        y = int(y)

        t = sess.query(maptile.Tile).get((layer, z, x, y))
        if not t:
            self.set_status(404)
            return

        self.return_static(
            t.load(self.sess) if not t.is_null() else None,
            t.uuid,
            t.fetched_on,
            settings.LAYERS[layer]['file_type']
        )

    def return_static(self, content, digest, modtime, file_type):
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
            self.set_header('Content-Type', 'image/' + file_type)
            self.set_header('Content-Length', len(content))
            self.write(content)
        else:
            self.set_status(404)

class TileURLHandler(web.RequestHandler):
    """return mapserver tile url for layer"""

    def get(self, layer, z, x, y):
        z = int(z)
        x = int(x)
        y = int(y)

        self.set_header('Content-Type', 'text/plain')
        self.write(maptile.Tile(layer=layer, z=z, x=x, y=y).url())

class TileCoverHandler(web.RequestHandler):
    """return metadata describing the coverage over this tile at other zoom levels"""

    def initialize(self, dbsess):
        self.sess = dbsess

    def get(self, layer, z, x, y):
        z = int(z)
        x = int(x)
        y = int(y)

        desc = maptile.Tile(layer=layer, z=z, x=x, y=y).get_descendants(self.sess, 8)
        def rel_tile(t):
            zdiff = t.z - z
            return {'z': zdiff, 'x': t.x - x * 2**zdiff, 'y': t.y - y * 2**zdiff}
        payload = [rel_tile(t) for t in desc]

        self.set_header('Content-Type', 'text/json')
        self.write(json.dumps(payload))

class RootContentHandler(web.StaticFileHandler):
    def get(self):
        super(RootContentHandler, self).get('map.html')

sess = maptile.dbsess()
application = web.Application([
    (r'/layers', LayersHandler),
    (r'/tile/([A-Za-z0-9_-]+)/([0-9]+)/([0-9]+),([0-9]+)', TileHandler, {'dbsess': sess}),
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
    IOLoop.instance().start()
