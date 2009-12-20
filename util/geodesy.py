import math

#geodesic computations, such as distances and bearings between points
#all calculations assume a spherical earth for now

EARTH_EQ_RAD = 6378137.0
EARTH_POL_RAD = 6356752.3
EARTH_MEAN_RAD = 6371009.0

EPSILON = 1.0e-9

#return i dot j
#i and j are any vector of the same dimension
def dotp (i, j):
  return sum([x * y for (x, y) in zip(i, j)])

#return i cross j
#i and j are any 3-vector
def crossp (i, j):
  indexes = [(k, (k + 1) % 3) for k in (1, 2, 0)]
  return tuple([i[t] * j[u] - i[u] * j[t] for (t, u) in indexes])

#return norm/length of v
#v is any vector in any dimension
def vlen (v):
  return math.sqrt(sum([k**2. for k in v]))

#return k * v
#v is any vector in any dimension, k is a scalar factor
def vscale (v, k):
  return tuple([x * k for x in v])

#return i + j
#i and j are any vector of the same dimension
def vadd (i, j):
  return tuple([x + y for (x, y) in zip(i, j)])

#normalize v; exception if norm(v) == 0
#v is any vector in any dimension
def vnorm (v):
  norm = vlen(v)
  if (norm < EPSILON):
    raise ZeroDivisionError()
  return vscale(v, 1. / norm)

#return component of j orthogonal to i, and cosine of angle between i and j
#i and j are unit 3-vectors
def vortho (i, j):
  kcos = dotp(i, j)
  return (vadd(j, vscale(i, -kcos)), kcos)

#rotate v by angle theta around axis, clockwise for positive theta when looking
#from 'axis' toward origin. 
#v and axis are unit 3-vectors, v and axis need not be orthogonal, theta in radians
def vrot (v, axis, theta):
  return vrotv(v, axis, [theta])[0]

#vrot, but for many thetas at once
def vrotv (v, axis, thetas):
  (vo, kcos) = vortho(axis, v)
  vaxial = vscale(axis, kcos)
  vd = crossp(vo, axis)
  return [vadd(vaxial, vangle(vo, vd, theta)) for theta in thetas]

#return point on unit sphere corresponding to (lat, lon)
def ll_to_ecefu ((lat, lon)):
  rlat = math.radians(lat)
  rlon = math.radians(lon)
  latcos = math.cos(rlat)
  return (math.cos(rlon) * latcos, math.sin(rlon) * latcos, math.sin(rlat))

#convert a point on the unit sphere to (lat, lon)
def ecefu_to_ll ((x, y, z)):
  rlat = math.asin(clamp(z, -1., 1.))
  if (abs(x) < EPSILON and abs(y) < EPSILON):
    rlon = 0.
  else:
    rlon = math.atan2(y, x)
  return (math.degrees(rlat), math.degrees(rlon))

#return 'north' and 'east' vectors for a given position vector
def orientate (vp):
  try:
    veast = vnorm(crossp((0., 0., 1.), vp))
  except ZeroDivisionError:
    #at a pole
    veast = (0., -vp[2], 0.)
  vnorth = crossp(vp, veast)
  return (vnorth, veast)

#create angle vector given orthogonal basis vectors 'u' and 'v', and angle 'theta' in radians
def vangle (u, v, theta):
  return vadd(vscale(u, math.cos(theta)), vscale(v, math.sin(theta)))

#return bearing vector for a given position vector and bearing angle
def vbear (vp, bearing):
  rbear = math.radians(bearing)
  (vnorth, veast) = orientate(vp)
  return vangle(vnorth, veast, rbear)

#return distance, in meters, between lat/lon coordinates p0 and p1
def distance (p0, p1):
  [v0, v1] = [ll_to_ecefu(p) for p in [p0, p1]]
  (vo, kcos) = vortho(v0, v1)
  ksin = vlen(vo)
  return EARTH_MEAN_RAD * math.atan2(ksin, kcos)

#return compass bearing from src to dst; None if src/dst are antipodal;
#if src is polar, treat direction of 0 longitude as north
def bearing (src, dst):
  [vsrc, vdst] = [ll_to_ecefu(p) for p in [src, dst]]

  (vdir, _) = vortho(vsrc, vdst)
  if vlen(vdir) < EPSILON:
    #antipodal
    return None

  (vnorth, veast) = orientate(vsrc)
  return math.degrees(math.atan2(dotp(vdir, veast), dotp(vdir, vnorth)))

#return the coordinates of the position 'distance' meters away from 'p', in direction 'bearing' 
def plot (p, bearing, distance):
  return plot_dv(p, bearing, [distance])[0]

#plot, but for many distances at once (useful for great circle arcs)
def plot_dv (p, bearing, distances):
  vp = ll_to_ecefu(p)
  vdir = vbear(vp, bearing)
  return [ecefu_to_ll(vangle(vp, vdir, d / EARTH_MEAN_RAD)) for d in distances]

#plot, but for many bearings at once (useful for distance range arcs)
def plot_bv (p, bearings, distance):
  vp = ll_to_ecefu(p)
  dst = ll_to_ecefu(plot(p, 0, distance))
  return [ecefu_to_ll(p) for p in vrotv(dst, vp, [math.radians(b) for b in bearings])]



def great_circle (p, bearing, distdelta, distmax=2.*math.pi*EARTH_MEAN_RAD, distmin=0.):
  return plot_dv(p, bearing, rangef(distmin, distmax, distdelta))

def distance_arc (p, distance, angledelta, anglemin=None, anglemax=None):
  return plot_bv(p, rangea(angledelta, anglemin, anglemax), distance)



def rangef (min, max, step):
  f = min
  vals = []
  while f < max:
    vals.append(f)
    f += step
  if min == max or (min < max and max - vals[-1] > EPSILON):
    vals.append(max)
  return vals

def rangea (step, min=None, max=None):
  if (min == None or max == None or max - min + EPSILON >= 360.):
    return rangef(-180., 180., step)
  elif abs(max - min) < EPSILON:
    vals = [min]
  elif max > min:
    vals = rangef(min, max, step)
  elif min - max - EPSILON <= 360.:
    vals = rangef(min, max + 360., step)
  else:
    vals = []
  return [anglenorm(a) for a in vals]

#offset = 0. - lowest angle
def anglenorm (a, offset=180.):
  return (a + offset) % 360. - offset

def clamp (x, min, max):
  if x > max:
    return max
  elif x < min:
    return min
  else:
    return x





if __name__ == '__main__':

  import sys

  type = sys.argv[1]
  lat = float(sys.argv[2])
  lon = float(sys.argv[3])
  z = float(sys.argv[4])
  step = float(sys.argv[5])
  min = None
  if len(sys.argv) >= 7:
    min = float(sys.argv[6])
  max = None
  if len(sys.argv) >= 8:
    max = float(sys.argv[7])

  open = """<?xml version="1.0" encoding="UTF-8"?>
  <kml xmlns="http://earth.google.com/kml/2.1">
  <Document>
  <Placemark>
  <Style><LineStyle><color>ff8040ff</color></LineStyle></Style>
  <LineString>
  <tessellate>1</tessellate>
  <altitudeMode>clampToGround</altitudeMode>
  <coordinates>"""

  close = """</coordinates>
  </LineString>
  </Placemark>
  </Document>
  </kml>"""

  if type == 'line':
    pts = great_circle((lat, lon), z, step, max, min)
  elif type == 'arc':
    pts = distance_arc((lat, lon), z, step, min, max)

  print open
  for p in pts:
    print '%s,%s,%s' % (p[1], p[0], 0)
  print close
