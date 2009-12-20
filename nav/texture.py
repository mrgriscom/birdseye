from Image import *
import os
import ImageEnhance
from mapcache import mapdownloader
import config

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
    missing = '/home/drew/nav/missing.jpg'
    tile = open(missing)
  return tile

def get_zoom_tile (mode, zoom, x, y):
  file = tile_file(mode, zoom, x, y)
  if file != None and os.path.exists(file):
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
    conn = mapdownloader.dbconn()

  curs = conn.cursor()
  vals = dict(z=zoom, x=x, y=y, type=1 if mode == 'map' else 2)
  curs.execute('select uuid from tiles where (z, x, y, type) = (%(z)s, %(x)s, %(y)s, %(type)s);', vals) 
  if curs.rowcount > 0:
    uuid = curs.fetchall()[0][0]
  else:
    return None

  path = config.tile_dir
  if path[-1] != '/':
    path += '/'

  buckets = [3]
  for i in buckets:
    path += uuid[:i] + '/'

  path += uuid + ('.png' if mode == 'map' else '.jpg')

  print path
  return path

