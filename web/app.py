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

import nav.texture as tiles

class TileHandler(web.RequestHandler):
    def initialize(self, dbsess):
        self.sess = dbsess

    def get(self):
        z = self.get_argument('z')
        x = self.get_argument('x')
        y = self.get_argument('y')

        content = tiles.get_tile(self.sess, z, x, y, 1)
        if content:
            self.set_header("Content-Type", "image/png")
            self.write(content)
        else:
            self.set_status(404)

if __name__ == "__main__":

    sess = tiles.dbsess()

    application = web.Application([
        (r'/tile', TileHandler, {'dbsess': sess}),
        (r"/static/(.*)", web.StaticFileHandler, {"path": "/home/drew/dev/birdseye/web/static/"}),
    ])
    application.listen(10101)

    IOLoop.instance().start()
