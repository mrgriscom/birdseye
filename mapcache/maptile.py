import math
from Polygon import *
import bisect

def ll_to_mercator ((lat, lon)):
  """Project latitude/longitude position (in degrees) to mercator
  longitude/latitude in radians"""
  return (math.radians(lon), math.log(math.tan(math.pi / 4. + math.radians(lat) / 2.)))

def mercator_to_ll ((x, y)):
  """Inverse of ll_to_mercator"""
  return (math.degrees(2. * (math.atan(math.exp(y)) - math.pi / 4.)), math.degrees(x))

def mercator_to_xy ((x, y)):
  """Transform mercator longitude/latitude to quadtree plane coordinates
  (top-left = (0, 0); bottom-right = (1, 1))"""
  return (x / (2. * math.pi) + 0.5, -y / (2. * math.pi) + 0.5)

def xy_to_mercator ((x, y)):
  """Inverse of mercator_to_xy"""
  return (2. * math.pi * (x - 0.5), 2. * math.pi * (0.5 - y))

def xy_to_tile (p, zoom):
  """Map quadtree plane x/y coordinates to tile coordinates at given zoom level"""
  return tuple([int(c) for c in xy_to_tilef(p, zoom)])

def xy_to_tilef (p, zoom):
  """Same as xy_to_tile, but include fractional part"""
  return tuple([2.**zoom * c for c in p])

def tilef_to_xy (p, zoom):
  """Inverse of xy_to_tilef"""
  return tuple([c / 2.**zoom for c in p])

def calc_scale_brackets (limit=math.pi):
  """Generate the list of mercator y-coordiantes at which linear distortion
  reaches successive powers of 2. y[i] is point at which scale is 2^(i+1)*equator,
  List is theoretically infinite, but stop at last value less than limit
  (default: edge of quadtree plane (~85.05 degrees latitude))"""
  disconts = []

  disc_merc = 0
  while disc_merc < limit:
    disc_lat = math.degrees(math.acos(1. / 2.**(len(disconts) + 1)))
    disc_merc = ll_to_mercator((disc_lat, 0))[1]
    disconts.append(disc_merc)

  return disconts[0:-1]

scale_brackets = None
def init_scale_brackets ():
  """Initialize scale brackets global variable"""
  global scale_brackets
  if scale_brackets == None:
    scale_brackets = calc_scale_brackets()

def zoom_adjust (zoom, y):
  """Calculate the zoom level difference, for the given y-tile and zoom level,
  that gives the same effective scale as at the equator"""
  init_scale_brackets()

  #consider closest point on tile to equator (least distortion -- err on
  #side of higher resolution)
  if zoom == 0:
    yr = 0.5
  else:
    yr = y
    if y < 2**(zoom - 1):
      yr += 1

  merc_y = abs(xy_to_mercator(tilef_to_xy((0., yr), zoom))[1])
  return bisect.bisect_right(scale_brackets, merc_y)

def bracket (x, min=None, max=None):
  """Limit x at min and max, if defined"""
  if min != None and x < min:
    return min
  elif max != None and x > max:
    return max
  else:
    return x

def max_y_for_zoom (zoom, max_zoom):
  """Return the minimum and maximum y-tiles at the given zoom level for which the
  effective scale will not exceed the maximum zoom level"""
  init_scale_brackets()

  zdiff = max_zoom - zoom
  if zdiff < 0:
    mid = 2**(zoom - 1)
    return (mid, mid - 1)
 
  max_merc_y = scale_brackets[zdiff] if zdiff < len(scale_brackets) else math.pi
  ybounds = [xy_to_tile(mercator_to_xy((0, s*max_merc_y)), zoom)[1] for s in (1, -1)]
  return tuple([bracket(y, 0, 2**zoom - 1) for y in ybounds]) #needed to fix y=-pi,
      #but also a sanity check








def tile (polygon, scale_extents, zoom, (x, y)):
  """Recursively enumerate tiles overlapping the polygon"""
  if not within_extent(scale_extents, zoom, y):
    return

  (xmin, ymin) = tilef_to_xy((x, y), zoom)
  (xmax, ymax) = tilef_to_xy((x + 1, y + 1), zoom)
 
  q = quadrant(xmin, xmax, ymin, ymax)
  if polygon.overlaps(q):
    yield (zoom, x, y)

    if polygon.covers(q):
      for t in fill_in(scale_extents, zoom, (x, y)):
        yield t
    else:
      for child in quad_children(x, y):
        for t in tile(polygon, scale_extents, zoom + 1, child):
          yield t

def fill_in (scale_extents, root_zoom, (x, y)):
  """For a tile completely within the polygon, recursively add all child
  tiles up to the terminating zoom level"""
  z = root_zoom + 1

  empty = False
  while not empty:
    zdiff = z - root_zoom
    (xmin, xmax) = [(x + xo) * 2**zdiff for xo in [0, 1]]
    (ymin, ymax) = [(y + yo) * 2**zdiff for yo in [0, 1]]

    (ymin2, ymax2) = scale_extents[z]
    ymin = max(ymin, ymin2)
    ymax = min(ymax, ymax2 + 1)
    empty = (ymin >= ymax)

    for ty in range(ymin, ymax):
      for tx in range(xmin, xmax):
        yield (z, tx, ty)

    z += 1

def within_extent (scale_extents, z, y):
  """Return whether the y-tile falls within the desired range at this zoom level"""
  (ymin, ymax) = scale_extents[z]
  return y >= ymin and y <= ymax

def calc_scale_extents (max_zoom):
  """Return the minimum and maximum y-tiles that should be fetched at each zoom
  level, so as to not exceed the effective scale of the max zoom level. Return
  list such that list[zoom_level] = (min_y, max_y). List will have entries for
  all zoom levels from 0 to max_zoom + 1 (the range for max_zoom + 1 will be
  empty)"""
  return [max_y_for_zoom(z, max_zoom) for z in range(0, max_zoom + 2)] 

def quadrant (xmin, xmax, ymin, ymax):
  return Polygon([(xmin, ymin), (xmax, ymin), (xmax, ymax), (xmin, ymax)])

def quad_children (x, y):
  """For a given tile, return its 4 constituent children at the next zoom
  level"""
  return [(2 * x + xo, 2 * y + yo) for xo in [0, 1] for yo in [0, 1]]




class region_tessellation:
  def __init__ (self, polygon, max_zoom):
    self.polygon = polygon
    self.max_zoom = max_zoom

  def __iter__ (self):
    return self.next()

  def next (self):
    for t in tile(self.polygon, calc_scale_extents(self.max_zoom), 0, (0, 0)):
      yield t

  #replace with an alternate method that generates new polygons with a 'fuzz' threshold
  #(.5 * tile size) and computes area exactly (no fudge)
  def size_estimate (self, compensate=True):
    init_scale_brackets()

    ymins = [max(mercator_to_xy((0, y))[1], 0.) for y in scale_brackets]
    base_area = self.polygon.area()

    z_areas = []
    for z in range(0, self.max_zoom + 1):
      if z <= self.max_zoom - len(ymins):
        area = base_area
      else:
        ymin = ymins[self.max_zoom - z]
        sub_poly = self.polygon & quadrant(0., 1., ymin, 1. - ymin)
        area = sub_poly.area()
      z_areas.append(area)

    z_tiles = [area * 4**z for (z, area) in enumerate(z_areas)]
    total = sum([math.ceil(t) for t in z_tiles])

    #compensate for underestimation
    fudge = min(5. / math.sqrt(total), 0.75) if compensate else 0.
    fudged_total = math.ceil(total * (1. + fudge))
    max_possible =  math.floor(4./3. * 4**self.max_zoom)
    return int(min(fudged_total, max_possible))
