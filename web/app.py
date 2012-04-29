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

from mapcache import maptile
import nav.texture

class LayersHandler(web.RequestHandler):
    def get(self):
        payload = [{'id': k, 'name': settings.LAYERS[k].get('name', k)} for k in settings.LAYERS.keys()]
        self.set_header('Content-Type', 'text/json')
        self.write(json.dumps(payload))

class TileHandler(web.RequestHandler):
    def initialize(self, dbsess):
        self.sess = dbsess

    def get(self, layer, z, x, y):
        z = int(z)
        x = int(x)
        y = int(y)

        content = nav.texture.get_tile(self.sess, z, x, y, layer)
        if content:
            self.set_header('Content-Type', 'image/' + settings.LAYERS[layer]['file_type'])
            self.write(content)
        else:
#            self.set_header('Content-Type', 'image/png')
#            with open('/home/drew/tmp/overlay.png') as f:
#                self.write(f.read())
            self.set_status(404)

class TileCoverHandler(web.RequestHandler):
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

if __name__ == "__main__":

    sess = maptile.dbsess()

    application = web.Application([
        (r'/layers', LayersHandler),
        (r'/tile/([A-Za-z0-9_-]+)/([0-9]+)/([0-9]+),([0-9]+)', TileHandler, {'dbsess': sess}),
        (r'/tilecover/([A-Za-z0-9_-]+)/([0-9]+)/([0-9]+),([0-9]+)', TileCoverHandler, {'dbsess': sess}),
        (r'/static/(.*)', web.StaticFileHandler, {'path': '/home/drew/dev/birdseye/web/static/'}),
    ])
    application.listen(10101)

    IOLoop.instance().start()
