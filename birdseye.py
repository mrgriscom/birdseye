import sys
from OpenGL.GL import *
from OpenGL.GLUT import *
from OpenGL.GLU import *
import Image
import time
import math
from nav.tracker import tracker
from mapcache import maptile
from nav import texture
from util import geodesy
import ImageFont
import ImageDraw
from datetime import datetime
from optparse import OptionParser
import os
import logging
import config

ESCAPE = '\x1b'

window = 0

zoom = None
view = 'map'
curview = None

texwidth = 6
texheight = 6

maptexid = None
curstexid = None
markertexids = None
texttexid = None
glyphtable = None

gps = None

destpos = None

scales = None

#units = 'metric'
units = 'imperial'

def InitGL(Width, Height):
  LoadStaticTextures()

  glClearColor(0.0, 0.0, 0.0, 0.0)
  glDisable(GL_DEPTH_TEST)
  glShadeModel(GL_SMOOTH)

  glEnable(GL_BLEND)
  glBlendFunc(GL_SRC_ALPHA, GL_ONE_MINUS_SRC_ALPHA)

  glEnable(GL_POINT_SMOOTH)
  glEnable(GL_LINE_SMOOTH)
  glHint(GL_POINT_SMOOTH_HINT, GL_NICEST)
  glHint(GL_LINE_SMOOTH_HINT, GL_NICEST)

  SetProjection(Width, Height)

def ReSizeGLScene(Width, Height):
  if Height == 0:
    Height = 1

  SetProjection(Width, Height)

def SetProjection (Width, Height):
  (xmin, xmax, ymin, ymax) = [0.5 / 256. * d for d in [-1024, 1024, 600, -600]]

  glViewport(0, 0, Width, Height)
  glMatrixMode(GL_PROJECTION)
  glLoadIdentity()
  glOrtho(xmin, xmax, ymin, ymax, -1, 1)
  glMatrixMode(GL_MODELVIEW)

def LoadTexture(id, image, alpha=False):
  pixels = image.tostring("raw", "RGBA" if alpha else "RGBX", 0, -1)

  glBindTexture(GL_TEXTURE_2D, id)
  glPixelStorei(GL_UNPACK_ALIGNMENT,1)
  glTexImage2D(GL_TEXTURE_2D, 0, 4, image.size[0], image.size[1], 0, GL_RGBA, GL_UNSIGNED_BYTE, pixels)
  glTexParameterf(GL_TEXTURE_2D, GL_TEXTURE_WRAP_S, GL_CLAMP) #_TO_EDGE)
  glTexParameterf(GL_TEXTURE_2D, GL_TEXTURE_WRAP_T, GL_CLAMP) #_TO_EDGE)
  glTexParameterf(GL_TEXTURE_2D, GL_TEXTURE_MAG_FILTER, GL_LINEAR)
  glTexParameterf(GL_TEXTURE_2D, GL_TEXTURE_MIN_FILTER, GL_LINEAR)
  glTexEnvf(GL_TEXTURE_ENV, GL_TEXTURE_ENV_MODE, GL_DECAL)

def LoadStaticTextures ():
  global curstexid
  global markertexids
  curstexid = glGenTextures(1)
  markertexids = [glGenTextures(1) for i in range(0, 3)]

  image = Image.open('%s/pixmap/cursor.png' % sys.path[0])
  LoadTexture(curstexid, image, True)

  for (id, i) in zip(markertexids, range(1, 4)):
    image = Image.open('%s/pixmap/target%d.png' % (sys.path[0], i))
    LoadTexture(id, image, True)

  #text
  global texttexid
  global glyphtable
  fontdir = '/usr/share/fonts/truetype/freefont/'
  font = ImageFont.truetype(fontdir + 'FreeSansBold.ttf', 30)

  texttexid = glGenTextures(1)
  glyphtable = {}
  xc = 0
  timg = Image.new('RGBA', (1024, 1024))
  draw = ImageDraw.Draw(timg)
  for text in u'0123456789.:+-hmJanFebMrApyulgSOctNovDTWdifEk \xb0?':
    sz = font.getsize(text)
    draw.text((xc, 0), text, font=font)
    glyphtable[text] = (xc, sz[0], sz[1])
    xc += sz[0]
  LoadTexture(texttexid, timg, True)

  

def LoadMapTexture (view, zoom, tile):
  global maptexid
  if maptexid == None:
    maptexid = glGenTextures(1)
    print maptexid

  xmin = tile[0] - texwidth / 2
  ymin = tile[1] - texwidth / 2

  tex_image = texture.get_texture_image(view, zoom, xmin, ymin, texwidth, texheight)
  LoadTexture(maptexid, tex_image)

  glTexEnvi(GL_TEXTURE_ENV, GL_TEXTURE_ENV_MODE, GL_DECAL)

def DrawGLScene():
 try:
  (pos, v, age) = gps.get_loc()

  #will differ in browse mode
  pos_center = pos

  xy = maptile.mercator_to_xy(maptile.ll_to_mercator(pos_center))
  tile = maptile.xy_to_tile(xy, zoom)
  tilef = maptile.xy_to_tilef(xy, zoom)

  pf = maptile.xy_to_tilef(maptile.mercator_to_xy(maptile.ll_to_mercator(pos)), zoom)

  global curview
  if curview != (view, zoom, tile):
    curview = (view, zoom, tile)
    LoadMapTexture(view, zoom, tile)

  glClear(GL_COLOR_BUFFER_BIT)
  glMatrixMode(GL_MODELVIEW)
  glLoadIdentity()					# Reset The View 

  glPushMatrix()
#  glTranslatef(-1.333, 0, 0)
#  glRotatef(90 - v[1], 0, 0, 1)

  glPushMatrix()
  glTranslatef(-texwidth/2, -texheight/2, 0.)
  glTranslatef(tile[0] - tilef[0], tile[1] - tilef[1], 0.)

  glEnable(GL_TEXTURE_2D)
  glBindTexture(GL_TEXTURE_2D, maptexid)   # 2d texture (x and y size)
  glTexEnvf(GL_TEXTURE_ENV, GL_TEXTURE_ENV_MODE, GL_DECAL)

  glBegin(GL_QUADS)
  glColor3f(.8,.7,.4)
  glTexCoord2f(0.0, 1.0) 
  glVertex3f(0.0, 0.0, 0.0)
  glTexCoord2f(1.0, 1.0) 
  glVertex3f(texwidth, 0.0, 0.0)
  glTexCoord2f(1.0, 0.0) 
  glVertex3f(texwidth, texheight, 0.0)
  glTexCoord2f(0.0, 0.0) 
  glVertex3f(0.0, texheight, 0.0)
  glEnd()

  glPopMatrix()

  glTexEnvf(GL_TEXTURE_ENV, GL_TEXTURE_ENV_MODE, GL_MODULATE)

  #destination marker
  if destpos != None:
    dp = maptile.xy_to_tilef(maptile.mercator_to_xy(maptile.ll_to_mercator(destpos)), zoom)

    rotspeed = [100, -10, 130]

    glPushMatrix()
    glTranslatef(dp[0] - tilef[0], dp[1] - tilef[1], 0)

    for i in range(0, 3):
      glBindTexture(GL_TEXTURE_2D, markertexids[i])

      glPushMatrix()
      glRotatef(clock() * rotspeed[i], 0, 0, 1)

      glBegin(GL_QUADS)
      glColor4f(0., 0, 1, .55)
      glTexCoord2f(0.0, 1.0) 
      glVertex3f(-0.125, 0.125, 0.0)
      glTexCoord2f(1.0, 1.0) 
      glVertex3f(0.125, 0.125, 0.0)
      glTexCoord2f(1.0, 0.0) 
      glVertex3f(0.125, -0.125, 0.0)
      glTexCoord2f(0.0, 0.0) 
      glVertex3f(-0.125, -0.125, 0.0)
      glEnd()

      glPopMatrix()

    glPopMatrix()

    #line to dest
    glDisable(GL_TEXTURE_2D)

    segment_length = 2.*math.pi*geodesy.EARTH_MEAN_RAD*math.cos(math.radians(pos_center[0]))/(256*2.**zoom) * 20
    segment_length = min(200000., max(50000., segment_length))

    dist = geodesy.distance(pos, destpos)
    bear = geodesy.bearing(pos, destpos)
    vd = geodesy.rangef(0, dist, segment_length)
    vp = geodesy.plot_dv(pos, bear, vd)
    pts = [maptile.xy_to_tilef(maptile.mercator_to_xy(maptile.ll_to_mercator(wpt)), zoom) for wpt in vp]
    pts = [(pt[0] - tilef[0], pt[1] - tilef[1]) for pt in pts]

    filt = [(pt[0]**2. + pt[1]**2.)**.5 < 3 for pt in pts]
    filt2 = [False] * len(filt)
    for i in range(0, len(filt)):
      if filt[i]:
        filt2[i] = True
        if i > 0:
          filt2[i - 1] = True
        if i < len(filt) - 1:
          filt2[i + 1] = True
    pts = [pt for (i, pt) in enumerate(pts) if filt2[i]]

    glLineWidth(2.5)
    glBegin(GL_LINE_STRIP)
    glColor4f(0, 0, 1, .3)
    for pt in pts:
      glVertex3f(pt[0], pt[1], 0.0)
    glEnd()

    glEnable(GL_TEXTURE_2D)

  #position marker
  glPushMatrix()
  glTranslatef(pf[0] - tilef[0], pf[1] - tilef[1], 0)
  glRotatef(v[1], 0., 0., 1.)

  glBindTexture(GL_TEXTURE_2D, curstexid)

  glBegin(GL_QUADS)
  if age < 5.:
    glColor4f(1., 0., 0., cursalpha(clock()))
  else:
    glColor4f(.3, .3, .3, cursalpha(clock()))
  glTexCoord2f(0.0, 1.0) 
  glVertex3f(0.125, -0.125, 0.0)
  glTexCoord2f(1.0, 1.0) 
  glVertex3f(-0.125, -0.125, 0.0)
  glTexCoord2f(1.0, 0.0) 
  glVertex3f(-0.125, 0.125, 0.0)
  glTexCoord2f(0.0, 0.0) 
  glVertex3f(0.125, 0.125, 0.0)
  glEnd()

  glPopMatrix()

  glPopMatrix()

  #clock
  inst = datetime.now()
  uinst = datetime.utcnow()
  diff = inst - uinst
  offset = int(round((86400*diff.days + diff.seconds + 1.0e-6*diff.microseconds) / 60, 0))

  timestr = '%02d:%02d:%02d.%02d' % (inst.hour, inst.minute, inst.second, inst.microsecond / 10000) 
  offsetstr = '+' if offset >= 0 else '-'
  if offset < 0:
    offset = -offset
  offsetstr += '%dh' % (offset / 60)
  if offset % 60 > 0:
    offsetstr += '%02dm' % (offset % 60)

  weekday = inst.strftime('%A')
  month = inst.strftime('%B')
  datestr = '%04d-%s-%02d %s' % (inst.year, month[0:3], inst.day, weekday[0:3])

  glBindTexture(GL_TEXTURE_2D, texttexid)

  glPushMatrix()
  glTranslatef(1.35, -1.17, 0)

  glPushMatrix()
  glScalef(.5, .5, 1.)
  writeText(datestr)
  glPopMatrix()

  glPushMatrix()
  glTranslatef(.48, 0, 0)
  glScalef(.4, .5, 1.)
  writeText(offsetstr)
  glPopMatrix()

  glTranslatef(0, .05, 0)
  writeText(timestr)

  glPopMatrix()

  #distance
  if destpos != None:

    unit = 1000. if units == 'metric' else 1609.344

    diststr = '%.3f' % (dist / unit)

    glBindTexture(GL_TEXTURE_2D, texttexid)

    glPushMatrix()
    glTranslatef(-1.99, -1.17, 0)

    writeText(diststr)

    glPopMatrix()

  #scale bar
  global scales
  if scales == None:
    scales = []

    if units != 'metric':
      for i in [1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000]:
        scales.append((i*.3048, '%d ft' % i))
      for i in [1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000]:
        scales.append((i*1609.344, '%d mi' % i))
    else:
      for i in [1, 2, 5, 10, 20, 50, 100, 200, 500]:
        scales.append((i, '%d m' % i))
      for i in [1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000, 20000]:
        scales.append((i*1000., '%d km' % i))

  meters_per_pixel = 2*math.pi*geodesy.EARTH_MEAN_RAD*math.cos(math.radians(pos_center[0]))/(256*2.**zoom)
  optimum_bar_length = 100

  def optimality (x):
    px = x / meters_per_pixel
    return optimum_bar_length / px if px < optimum_bar_length else px / optimum_bar_length

  scale = min(scales, key=lambda (x, lab): optimality(x))
  length = scale[0] / meters_per_pixel
  lab = scale[1]

  glPushMatrix()
  glTranslatef(1.95, 287/256., 0)  

  glDisable(GL_TEXTURE_2D)
  glLineWidth(6)
  glBegin(GL_LINE_STRIP)
  color = (.2, .2, .2, .7) if view == 'map' else (.9, .9, .9, .7)
  glColor4f(*color)
  glVertex3f(0, 0, 0.0)
  glVertex3f(-length / 256., 0, 0.0)
  glEnd()

  glPushMatrix()
  glTranslatef(0, -.08, 0)
  glScalef(.5, .5, 1)
  glTranslatef(-textLen(lab)/256., 0, 0)

  glEnable(GL_TEXTURE_2D)
  glBindTexture(GL_TEXTURE_2D, texttexid)

  writeText(lab)

  glPopMatrix()
  glPopMatrix()

  #position
  tw = textLen(u'W999.99999\xb0')/256. + .02
  slat = u'%08.5f\xb0' % abs(pos_center[0])
  slon = u'%09.5f\xb0' % abs(pos_center[1])

  glPushMatrix()
  glTranslatef(-1.99, 1.015, 0)
  glScalef(.7, .7, 0)

  writeText('N' if pos_center[0] >= 0 else 'S')
  glPushMatrix()
  glTranslatef(tw - textLen(slat)/256., 0, 0)
  writeText(slat)
  glPopMatrix()

  glTranslatef(0, .1, 0)
  writeText('E' if pos_center[1] >= 0 else 'W')
  glTranslatef(tw - textLen(slon)/256., 0, 0)
  writeText(slon)

  glPopMatrix()

  glutSwapBuffers()

 except:
  logging.exception('')
  sys.exit()

def textLen (str):
  return sum([glyphtable[c if c in glyphtable else '?'][1] for c in str])

def writeText (str):
  xc = 0
  for c in str:
    (xo, sx, sy) = glyphtable[c if c in glyphtable else '?']

    tx0 = xo / 1024.
    tx1 = (xo + sx) / 1024.
    ty0 = 1.
    ty1 = 1 - (sy / 1024.)

    cx0 = xc / 256.
    cx1 = (xc + sx) / 256.
    cy0 = 0.
    cy1 = sy / 256.

    glBegin(GL_QUADS)
    color = (.3, .3, .3, 1.) if view == 'map' else (.8, .8, .8, 1.)
    glColor4f(*color)
    glTexCoord2f(tx0, ty0) 
    glVertex3f(cx0, cy0, 0.0)
    glTexCoord2f(tx1, ty0) 
    glVertex3f(cx1, cy0, 0.0)
    glTexCoord2f(tx1, ty1) 
    glVertex3f(cx1, cy1, 0.0)
    glTexCoord2f(tx0, ty1) 
    glVertex3f(cx0, cy1, 0.0)
    glEnd()

    xc += sx


def cursalpha (phase):
  period = 1.2
  min = .2
  max = .75
  pow = 1.6

  y = (0.5 * (math.sin(phase / period * 2.*math.pi) + 1)) ** pow
  return min + y * (max - min)

start = None
def clock():
  global start
  if start == None:
    start = time.time()

  return time.time() - start

def keyPressed(*args):
  global zoom
  global view

  if args[0] == 'z':
    zoom += 1
  elif args[0] == 'x':
    if zoom > 0:
      zoom -= 1
  elif args[0] == 'v':
    view = 'sat' if view == 'map' else 'map'
  elif args[0] == ESCAPE:
    sys.exit()

def main():
  global window
  windowname = 'map'

  glutInit([''])
  glutInitDisplayMode(GLUT_RGBA | GLUT_DOUBLE)
  glutInitWindowSize(1024, 600)
  glutInitWindowPosition(0, 0)
  window = glutCreateWindow(windowname)

  glutFullScreen()
  os.popen('wmctrl -r %s -b toggle,fullscreen' % windowname)

  glutDisplayFunc(DrawGLScene)
  glutIdleFunc(DrawGLScene)
  glutReshapeFunc(ReSizeGLScene)
  glutKeyboardFunc(keyPressed)

  InitGL(1024, 600)

  glutMainLoop()



waypoints = None

def load_waypoints ():
  global waypoints
  if waypoints == None:
    waypoints = {}
    lines = [l.strip() for l in open('%s/data/waypoints' % sys.path[0]).readlines() if l.strip()]    

    for l in lines:
      k = l.find('#')
      if k >= 0:
        l = l[:k].strip()
      if not l:
        continue

      k = l.find(':')
      if k == -1:
        print "can't parse %s" % l
        continue
      name = l[:k].strip()
      pcs = l[k+1:].split()
      if len(pcs) < 2:
        print "can't parse %s" % l
        continue
      pos = parse_ll(pcs[0] + ',' + pcs[1])
      if pos == None:
        print "can't parse %s" % l
        continue

      waypoints[name] = pos

def parse_ll (arg):
  load_waypoints()
  arg = arg.strip()
  if arg in waypoints:
    return waypoints[arg]

  pcs = arg.split(',')
  try:
    lat = float(pcs[0].strip())
    lon = float(pcs[1].strip())
  except (IndexError, ValueError):
    return None

  if lat < -90. or lat > 90. or lon < -180. or lon > 180.:
    print 'lat/lon out of range'
    return None

  return (lat, lon)

def parse_v (arg):
  pcs = arg.split(',')
  speed = float(pcs[0].strip())
  heading = float(pcs[1].strip())

  if speed < 0:
    speed = -speed
    heading += 180.

  return (speed, geodesy.anglenorm(heading))

def parse_args (args):
  global zoom, view, destpos, gps

  parser = OptionParser()
  parser.add_option('-z', '--zoom', dest='zoom', default='7')
  parser.add_option('-v', '--view', dest='view', default='map')
  parser.add_option('--dp', dest='demopos')
  parser.add_option('--dv', dest='demovel', default='0,0')

  (options, args) = parser.parse_args()

  zoom = int(options.zoom)
  if options.view in ['map', 'sat']:
    view = options.view
  else:
    print 'unrecognized view type'
    sys.exit()

  demo = False
  if options.demopos != None:
    demo = True
    demo_p = parse_ll(options.demopos)
    if demo_p == None:
      print 'invalid position'
      sys.exit()
    demo_v = parse_v(options.demovel)

  if len(args) > 0:
    destpos = parse_ll(args[0])
    if destpos == None:
      print 'invalid position'
      sys.exit()

  if not demo:
    gps = tracker()
  else:
    gps = tracker((demo_p, demo_v))
  gps.start()

  print zoom, view, destpos, ('demo', demo_p, demo_v) if demo else 'gps'







if __name__ == "__main__":

  logging.basicConfig(level=logging.INFO, stream=sys.stderr, format='')
  parse_args(sys.argv)

  print 'waiting for gps lock...'
  while gps.get_loc()[0] == None:
    time.sleep(0.1)
  print 'lock acquired'

  main() 	

  #gps.terminate()






