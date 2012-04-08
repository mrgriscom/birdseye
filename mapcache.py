import sys
import yaml
import re
from Polygon import *
import psycopg2
from datetime import datetime
from mapcache import mapdownloader
import settings

def error_out (msg):
  print msg
  sys.exit()

def args_from_yaml (file):
  try:
    data = yaml.safe_load(file)
  except yaml.YAMLError:
    error_out('Cannot parse directives; exiting...')

  args = {}

  if 'name' in data:
    args['name'] = str(data['name'])
    if not validate_name(args['name']):
      error_out('Region name not valid; exiting...')
  else:
    error_out('Region name is required; exiting...')

  if 'delete' in data:
    args['delete'] = True
    return args

  if 'region' in data:
    args['region'] = validate_region(str(data['region']))
    if not args['region']:
      sys.exit()

  dlayers = dict(data['layers']) if 'layers' in data else None
  if dlayers:
    args['layers'] = {}
    for (lname, ldata) in dlayers.iteritems():
      layername = str(lname)
      if not validate_name(layername):
        error_out('Layer name not valid; exiting...')
      args['layers'][layername] = parse_yaml_layer(ldata)
  else:
    if not ('delete' in args or 'region' in args):
      error_out('No action (delete, define region, layer action) defined; exiting...')

  return args

def parse_yaml_layer (layerdata):
  largs = {}

  if 'delete' in layerdata:
    largs['delete'] = True
    return largs

  if 'zoom' in layerdata:
    largs['zoom'] = int(layerdata['zoom'])
    if largs['zoom'] < 0 or largs['zoom'] > 30:
      error_out('Zoom level outside allowed range')
  else:
    error_out('Zoom level or \'delete\' required')

  if 'trim' in layerdata:
    largs['trim'] = True
    return largs

  if 'refresh-mode' in layerdata:
    largs['refr'] = str(layerdata['refresh-mode'])
    if largs['refr'] not in ['always', 'missing', 'never']:
      error_out('Refresh mode not recognized')
  else:
    largs['refr'] = 'never'

  return largs

def validate_name (str):
  return re.match('^[A-Za-z0-9 !"#$%&\'()*+,\\-./:;<=>?@[\\\\\\]^_`{|}~]+$', str)

def validate_region (region_str):
  pcs = [pc for pc in re.split('(\s+|,)', region_str) if pc.strip()]

  if len(pcs) % 3 != 0:
    print 'Invalid format for region boundary; exiting...'
    return None

  strips = [[pcs[i] for i in range(k, len(pcs), 3)] for k in range(0, 3)]

  if set(strips[1]) != set(','):
    print 'Invalid format for region boundary; exiting...'
    return None

  try:
    (lats, lons) = [[float(f) for f in strips[k]] for k in (0, 2)]
  except ValueError:
    print 'Invalid format for region boundary; exiting...'
    return None

  coords = zip(lats, lons)
  if len(coords) < 3:
    print 'Too few region coordinates; exiting...'
    return None

  for (lat, lon) in coords:
    if abs(lat) > 90. or abs(lon) > 180.:
      print 'Region coordinates out of range; exiting...'
      return None
    
  return Polygon(coords)

def check_unsupported (args):
  if 'delete' in args:
    error_out('Delete not supported')
  if 'layers' in args:
    if len(args['layers']) > 1:
      error_out('Only one layer supported')
    (layertype, layer) = args['layers'].items()[0]
    if layertype != 'gmap-map':
      error_out('Only gmap-map overlay type supported')
    if 'delete' in layer or 'trim' in layer:
      error_out('Delete/trim not supported')

def db_validate (args):
  try:
    conn = psycopg2.connect(database=settings.TILE_DB.split('/')[-1])
    curs = conn.cursor()
  except:
    error_out('can\'t connect to database')

  #fetch/store region for name
  curs.execute("select id from regions where name = '%s';" % args['name'])
  if 'region' in args:
    if curs.rowcount > 0:
      error_out('region name already taken')

    curs.execute('insert into regions (name, boundary, created_on) values (%(name)s, %(poly)s, %(now)s);',
                 dict(name=args['name'], poly=str(args['region'].contour(0)), now=datetime.utcnow()))
  else:
    if curs.rowcount == 0:
      error_out('no region defined by that name')

    curs.execute("select boundary from regions where name = '%s';" % args['name'])
    args['region'] = Polygon(eval(curs.fetchall()[0][0]))
  conn.commit()

  curs.execute("select id from regions where name = '%s';" % args['name'])
  region_id = curs.fetchall()[0][0]

  #per layer, update overlay info
  for (layername, layerinfo) in args['layers'].iteritems():
    curs.execute("select id from overlays where name = '%s';" % layername)
    if curs.rowcount == 0:
      print 'unrecognized overlay [%s]' % layername
    else:
      ovl_id = curs.fetchall()[0][0]

      curs.execute('select depth from region_overlays where (region_id, overlay_id) = (%(reg)s, %(ovl)s);',
                   dict(reg=region_id, ovl=ovl_id))
      if curs.rowcount == 0:
        curs.execute('insert into region_overlays (region_id, overlay_id, depth) values (%(reg)s, %(ovl)s, %(dep)s);',
                     dict(reg=region_id, ovl=ovl_id, dep=layerinfo['zoom']))
      else:
        depth = curs.fetchall()[0][0]
        if depth < layerinfo['zoom']:        
          curs.execute('update region_overlays set depth = %(dep)s where (region_id, overlay_id) = (%(reg)s, %(ovl)s);',
                       dict(reg=region_id, ovl=ovl_id, dep=layerinfo['zoom']))
      conn.commit()

  conn.close()




if __name__ == "__main__":

  args = args_from_yaml(sys.stdin)
  check_unsupported(args)
  db_validate(args)

  (layername, layerinfo) = args['layers'].items()[0]
  mapdownloader.download(args['region'], layername, layerinfo['zoom'], layerinfo['refr'])




