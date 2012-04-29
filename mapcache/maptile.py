import math
from Polygon import *
import bisect
import util.util as u
from util import geodesy
import settings
import os.path
from glob import glob
import mapdownload # argh circular import

from sqlalchemy import create_engine, Column, DateTime, Integer, String, ForeignKey, CheckConstraint, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.expression import func

Base = declarative_base()
class Tile(Base):
    __tablename__ = 'tiles'

    layer = Column(String, primary_key=True)
    z = Column(Integer, CheckConstraint('z >= 0'), primary_key=True)
    x = Column(Integer, primary_key=True)
    y = Column(Integer, primary_key=True)

    qt = Column(String, nullable=False)
    uuid = Column(String, nullable=False, index=True)

    fetched_on = Column(DateTime, default=func.now(), onupdate=func.now())

    __table_args__ = (
        CheckConstraint('x >= 0 and x < 2^z'),
        CheckConstraint('y >= 0 and y < 2^z'),
        Index('qt', layer, qt),
    )

    def __init__(self, **kw):
        try:
            kw['qt'] = u.to_quadindex(kw['z'], kw['x'], kw['y'])
        except KeyError:
            pass
        super(Tile, self).__init__(**kw)

    def pk(self):
        """get primary key"""
        return (self.layer, self.z, self.x, self.y)

    def path(self, suffix=None):
        """compute tile file path

        suffix -- if absent, pull from layer definition, or directory search
        """
        bucket = list(self.path_intermediary())[-1]
        def mkpath(suffix):
            return os.path.join(bucket, '%s.%s' % (self.uuid, suffix))

        if not suffix:
            layer = settings.LAYERS.get(self.layer)
            if layer:
                suffix = layer.get('file_type')
        if not suffix:
            try:
                return glob(mkpath('*'))[0]
            except IndexError:
                suffix = ''
        return mkpath(suffix)

    def path_intermediary(self):
        """compute all intermediary paths ('buckets')"""
        for i in range(len(settings.TILE_BUCKETS)):
            yield os.path.join(settings.TILE_ROOT, *(self.uuid[:k] for k in settings.TILE_BUCKETS[:i+1]))

    def save(self, data, hashfunc, file_type=None):
        """save tile data to file and compute uuid

        data -- raw image data
        hashfunc -- hash function to compute uuid
        """
        self.uuid = hashfunc(data)
        if data is None:
            return

        for ipath in self.path_intermediary():
            if not os.path.exists(ipath):
                os.mkdir(ipath)

        path = self.path(file_type)
        if not os.path.exists(path):
            with open(path, 'w') as f:
                f.write(data)

    def url(self):
        """compute the original mapserver url for this tile"""
        return mapdownload.tile_url((self.z, self.x, self.y), self.layer)

    def get_descendants(self, sess, max_depth=None, min_depth=None):
        """query all descendant tiles from this tile (i.e., tiles at deeper zooms
        that overlap this tile's area"""
        q = sess.query(Tile).filter(Tile.layer == self.layer).filter(Tile.qt > self.qt).filter(Tile.qt < (self.qt + '4'))
        if min_depth:
            q = q.filter(Tile.z >= self.z + min_depth)
        if max_depth:
            q = q.filter(Tile.z <= self.z + max_depth)
        return q

class Region(Base):
    """named regions / tile download areas"""

    __tablename__ = 'regions'

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)
    boundary = Column(String, nullable=False)

    def __init__(self, name, coords):
        """
        coords -- a list of lat/lon coordinates ([(lat0, lon0), (lat1, lon1), ...]),
          or a string like 'lat0,lon0 lat1,lon1 ...'. lon must be bounded [-180,180]
        """
        if hasattr(coords, '__iter__'):
            coords = ' '.join('%s,%s' % c for c in coords)

        super(Region, self).__init__(**{
            'name': name,
            'boundary': coords,
        })

        self.validate()

    def coords(self):
        """coordinate list from db string representation"""
        return [tuple(float(k) for k in c.split(',')) for c in self.boundary.split()]

    def validate(self):
        coords = self.coords()
        if len(coords) < 3:
            raise Exception('not enough points')
        for lat, lon in coords:
            if lat > 90. or lat < -90. or lon < -180. or lon > 180.:
                raise Exception('coordinates out of range')

    def poly(self):
        """generate a polygon for the region, in lat/lon coordinates, taking
        care of wrapping around the IDL"""
        # correct coords so each lon is within 180 deg (in absolute
        # terms) of the previous. this essentially 'unrolls' any overlap
        # with the IDL by letting lon go beyond [-180,180]
        def relative_adjust(coords):
            ref_lon = 0.
            for lat, lon in coords:
                adj_lon = geodesy.anglenorm(lon, 180. - ref_lon)
                yield (lat, adj_lon)
                ref_lon = adj_lon
        coords = list(relative_adjust(self.coords()))
        min_lon = min(c[1] for c in coords)
        max_lon = max(c[1] for c in coords)

        # for each wraparound (360-degree width) segment of unrolled
        # poly, cut out that segment and shift back to normal lon range
        unrolled = Polygon(coords)
        def poly_segment(edge):
            # clip lat to prevent discontinuity when converting to mercator
            world = quadrant(-89.999, 89.999, edge, edge + 360.)
            overlap = world & unrolled
            overlap.shift(0, -180. - edge)
            return overlap
        edge0 = ((min_lon + 180.) // 360. - .5) * 360.
        return reduce(lambda a, b: a | b, (poly_segment(edge) for edge in range(edge0, max_lon, 360.)))

    def merc_poly(self):
        """like poly(), but transformed to mercator coordinates"""
        p = self.poly()
        mp = Polygon()
        for i, c in enumerate(p):
            mp.addContour(ll_to_xy(c), p.isHole(i))
        return mp

    @staticmethod
    def world():
        """region covering entire world"""
        return Region('world', [
                # delta-lons must be < 180
                ( 90, -180), ( 90, -60), ( 90,  60), ( 90,  180),
                (-90,  180), (-90,  60), (-90, -60), (-90, -180), 
            ])
    
def ll_to_xy(coords):
    return [mercator_to_xy(ll_to_mercator(c)) for c in coords]

class RegionOverlay(Base):
    """record which regions have been downloaded -- layer and depth"""

    __tablename__ = 'region_overlays'

    region = Column(Integer, ForeignKey('regions.id'), primary_key=True) # todo: cascade?
    layer = Column(String, primary_key=True)
    depth = Column(Integer, nullable=False)

    created_on = Column(DateTime, default=func.now())

def dbsess(connector=settings.TILE_DB, echo=False):
    """create a connector to the tile database"""
    engine = create_engine(connector, echo=echo)
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine)()





def ll_to_mercator((lat, lon)):
    """project lat/lon position (in degrees) to mercator lon/lat
    in radians"""
    return (math.radians(lon), math.log(math.tan(math.pi / 4. + math.radians(lat) / 2.)))

def mercator_to_ll((x, y)):
    """inverse ll_to_mercator"""
    return (math.degrees(2. * (math.atan(math.exp(y)) - math.pi / 4.)), math.degrees(x))

def mercator_to_xy((x, y)):
    """transform mercator lon/lat to quadtree plane coordinates
    (top-left = (0, 0); bottom-right = (1, 1))"""
    return (x / (2. * math.pi) + 0.5, -y / (2. * math.pi) + 0.5)

def xy_to_mercator((x, y)):
    """inverse mercator_to_xy"""
    return (2. * math.pi * (x - 0.5), 2. * math.pi * (0.5 - y))

def xy_to_tile(p, zoom):
    """map quadtree plane x/y coordinates to tile coordinates at given zoom level"""
    return tuple([int(c) for c in xy_to_tilef(p, zoom)])

def xy_to_tilef(p, zoom):
    """same as xy_to_tile, but include fractional part"""
    return tuple([2.**zoom * c for c in p])

def tilef_to_xy(p, zoom):
    """inverse of xy_to_tilef"""
    return tuple([c / 2.**zoom for c in p])

def calc_scale_brackets(offset=1., limit=math.pi):
    """generate the list of mercator y-coordiantes at which linear distortion
    reaches successive powers of 2. y[i] is point at which scale is 2^(i+1)*equator,
    list is theoretically infinite, but stop at last value less than limit
    (default: edge of quadtree plane (~85.05 degrees latitude))"""
    # TODO document 'offset'
    if offset <= 0. or offset > 1.:
        raise ValueError('offset must be in (0, 1]')

    i = 0
    while True:
        disc_lat = math.degrees(math.acos(1. / 2.**(i + offset)))
        disc_merc = ll_to_mercator((disc_lat, 0))[1]
        if disc_merc >= limit:
            break
        yield disc_merc
        i += 1

def zoom_adjust(scale_brackets, zoom, y):
    """calculate the zoom level difference, for the given y-tile and zoom level,
    that gives the same effective scale as at the equator"""
    # consider closest point on tile to equator (least distortion -- err on
    # side of higher resolution)
    if zoom == 0:
        yr = 0.5
    else:
        yr = y
        if y < 2**(zoom - 1):
            yr += 1

    merc_y = abs(xy_to_mercator(tilef_to_xy((0., yr), zoom))[1])
    return bisect.bisect_right(scale_brackets, merc_y)

def max_y_for_zoom(scale_brackets, zoom, max_zoom):
    """return the minimum and maximum y-tiles at the given zoom level for which the
    effective scale will not exceed the maximum zoom level"""
    zdiff = max_zoom - zoom
    if zdiff < 0:
        mid = 2**(zoom - 1)
        return (mid, mid - 1)
 
    max_merc_y = scale_brackets[zdiff] if zdiff < len(scale_brackets) else math.pi
    ybounds = [xy_to_tile(mercator_to_xy((0, s * max_merc_y)), zoom)[1] for s in (1, -1)]
    return tuple(u.clip(y, 0, 2**zoom - 1) for y in ybounds) #needed to fix y=-pi, but also a sanity check

class MercZoom(object):
    def __init__(self, offset=1.):
        self.scale_brackets = list(calc_scale_brackets(offset))

    def adjust(self, zoom, y):
        return zoom_adjust(self.scale_brackets, zoom, y)

    def max_y(self, zoom, max_zoom):
        return max_y_for_zoom(self.scale_brackets, zoom, max_zoom)

    def extents(self, max_zoom):
        """return the minimum and maximum y-tiles that should be fetched at each zoom
        level, so as to not exceed the effective scale of the max zoom level. return
        list such that list[zoom_level] = (min_y, max_y). List will have entries for
        all zoom levels from 0 to max_zoom + 1 (the range for max_zoom + 1 will be
        empty)"""
        return [self.max_y(z, max_zoom) for z in range(0, max_zoom + 2)] 





def tile(polygon, scale_extents, zoom, (x, y)):
    """recursively enumerate tiles overlapping the polygon"""
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

def fill_in(scale_extents, root_zoom, (x, y)):
    """For a tile completely within the polygon, recursively add all child
    tiles up to the terminating zoom level"""
    z = root_zoom + 1

    while True:
        zdiff = z - root_zoom
        (xmin, xmax) = [(x + xo) * 2**zdiff for xo in [0, 1]]
        (ymin, ymax) = [(y + yo) * 2**zdiff for yo in [0, 1]]

        (ext_ymin, ext_ymax) = scale_extents[z]
        ymin = max(ymin, ext_ymin)
        ymax = min(ymax, ext_ymax + 1)
        if ymin >= ymax:
            break

        for ty in range(ymin, ymax):
            for tx in range(xmin, xmax):
                yield (z, tx, ty)

        z += 1

def within_extent(scale_extents, z, y):
    """return whether the y-tile falls within the desired range at this zoom level"""
    (ymin, ymax) = scale_extents[z]
    return y >= ymin and y <= ymax

def quadrant(xmin, xmax, ymin, ymax):
    """generate a polygon for the rectangle with the given bounds"""
    return Polygon([(xmin, ymin), (xmax, ymin), (xmax, ymax), (xmin, ymax)])

def quad_children(x, y):
    """for a given tile, generate the 4 constituent children at the next zoom
    level"""
    for xo in range(2):
        for yo in range(2):
            yield (2 * x + xo, 2 * y + yo)

class RegionTessellation(object):
    """an enumerator of all the tiles within a region"""
    def __init__(self, polygon, max_zoom, offset=1.):
        self.polygon = polygon
        self.max_zoom = max_zoom
        self._z = MercZoom(offset)

    def __iter__(self):
        return self.next()

    def next(self):
        for t in tile(self.polygon, self._z.extents(self.max_zoom), 0, (0, 0)):
            yield t

    def size_estimate(self, compensate=True):
        """estimate the number of tiles contained within"""
        # this method is pretty kludgey. better method: generate new polygons with
        # .5*tile radius 'fuzz' (at each zoom level), and compute/sum exact areas

        ymins = [max(mercator_to_xy((0, y))[1], 0.) for y in self._z.scale_brackets]
        base_area = self.polygon.area()

        def z_areas():
            for z in range(0, self.max_zoom + 1):
                if z <= self.max_zoom - len(ymins):
                    yield base_area
                else:
                    ymin = ymins[self.max_zoom - z]
                    sub_poly = self.polygon & quadrant(0., 1., ymin, 1. - ymin)
                    yield sub_poly.area()
        z_tiles = (area * 4**z for (z, area) in enumerate(z_areas()))
        total = sum(math.ceil(t) for t in z_tiles)

        #compensate for underestimation
        fudge = min(5. / math.sqrt(total), 0.75) if compensate else 0.
        fudged_total = math.ceil(total * (1. + fudge))
        max_possible = math.floor(4./3. * 4**self.max_zoom)
        return int(min(fudged_total, max_possible))
