from Image import *
import os
import ImageEnhance
from mapcache import mapdownload
from mapcache import maptile
import settings
import sys

blocksize = 256
fallback = 2

def get_texture_image (mode, zoom, xmin, ymin, width, height):
  tx = new("RGB", (blocksize * width, blocksize * height))

  for bx in range(0, width):
    for by in range(0, height):
      tile = get_img_chunk(mode, zoom, bx + xmin, by + ymin)
      tx.paste(tile, [blocksize * p for p in [bx, by, bx + 1, by + 1]])

  return tx

def get_img_chunk (mode, zoom, x, y):
  tile = get_zoom_tile(mode, zoom, x, y)
  if tile == None:
    tile = get_fallback_tile(mode, zoom, x, y)
  if tile == None:
    missing = '%s/pixmap/missing.jpg' % sys.path[0]
    tile = open(missing)
  return tile

def get_zoom_tile (mode, zoom, x, y):
  file = tile_file(mode, zoom, x, y)
  if file != None:
    img = open(file)
    return img.convert("RGB")
  else:
    return None

def get_fallback_tile (mode, zoom, x, y):
  for zdiff in range(1, fallback + 1):
    z = zoom - zdiff
    if z < 0:
      break

    zx = int(x / 2**zdiff)
    zy = int(y / 2**zdiff)

    tile = get_zoom_tile(mode, z, zx, zy)
    if tile == None:
      continue

    diffx = x - zx * 2**zdiff
    diffy = y - zy * 2**zdiff
    cropbounds = [int(256. * b / 2.**zdiff) for b in [diffx, diffy, diffx + 1, diffy + 1]]

    tile = tile.crop(cropbounds)
    tile = tile.resize((256, 256), BICUBIC)
    tile = ImageEnhance.Brightness(tile).enhance(.9**zdiff)
    return tile

  return None


conn = None
def tile_file (mode, zoom, x, y):
  global conn
  if conn == None:
    conn = maptile.dbsess()

  layer = {
    'map': 'gmap-map',
  }[mode]
  t = conn.query(maptile.Tile).get((layer, zoom, x, y))
  return t.open(conn) if t else None

def get_tile(sess, z, x, y, layer):
    t = sess.query(maptile.Tile).get((layer, z, x, y))
    return t.load(sess) if t else None


