from Image import *
import os
import ImageEnhance
from mapcache import mapdownload
from mapcache import maptile
import settings
import sys
import util.util as u

blocksize = 256
fallback = settings.LOOKBACK

def get_texture_image (mode, zoom, xmin, ymin, width, height):
  tx = new("RGB", (blocksize * width, blocksize * height))

  for bx in range(0, width):
    for by in range(0, height):
      cx = (xmin + bx) % 2**zoom
      cy = ymin + by
      
      tile = get_img_chunk(mode, zoom, cx, cy)
      tx.paste(tile, [blocksize * p for p in [bx, by, bx + 1, by + 1]])

  return tx

def get_img_chunk (mode, zoom, x, y):
  if x < 0 or y < 0 or x >= 2**zoom or y >= 2**zoom:
    return open(u.pixmap_path('space.jpg'))


  tile = get_zoom_tile(mode, zoom, x, y)
  if tile == None:
    tile = get_fallback_tile(mode, zoom, x, y)
  if tile == None:
    missing = u.pixmap_path('missing.jpg')
    tile = open(missing)
  return tile

def get_zoom_tile (mode, zoom, x, y):
  file = tile_file(mode, zoom, x, y)
  if file != None:
    with file as _f:
      img = open(_f)
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

  t = conn.query(maptile.Tile).get((mode, zoom, x, y))
  return t.open(conn) if t else None

def get_tile(sess, z, x, y, layer):
    t = sess.query(maptile.Tile).get((layer, z, x, y))
    return t.load(sess) if t else None


