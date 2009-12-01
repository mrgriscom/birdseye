import gpslistener
import psycopg2
import sys

def insert_query (table, column_mapping):
  columns = ', '.join(column_mapping.keys())
  fields = ', '.join(map(lambda f: '%%(%s)s' % f, column_mapping.values()))
  return 'insert into %s (%s) values (%s);' % (table, columns, fields)

if __name__ == '__main__':
  insert_gps_fix = insert_query('gps_log', {
    'gps_time': 'time',
    'system_time': 'systime',
    'latitude': 'lat',
    'longitude': 'lon',
    'altitude': 'alt',
    'speed': 'speed',
    'heading': 'heading',
    'climb': 'climb',
    'err_horiz': 'h_error',
    'err_vert': 'v_error',
    'type_of_fix': 'fix_type',
    'comment': 'comment'
  })

  try:
    conn = psycopg2.connect(database='geoloc')
    curs = conn.cursor()
  except:
    print 'cannot connect to gps database'
    sys.exit()

  try:
    gps = gpslistener.gps_subscription()
  except gpslistener.linesocket.CantConnect:
    print 'cannot connect to gps dispatcher'
    sys.exit()

  COMMIT_INTERVAL = 60

  def flushbuffer(conn, curs, buffer):
    for p in buffer:
      try:
        curs.execute(insert_gps_fix, p)
      except:
        print 'error committing fix: ' + p
        #raise
    conn.commit()
    return []

  try:
    c = 0
    buffer = []
    while True:
      c += 1
      data = gps.get_fix()
      if data != None:
        buffer.append(data)

      if c % COMMIT_INTERVAL == 0:
        buffer = flushbuffer(conn, curs, buffer)
        print 'flushed %d' % c

  #bug if interrupt happens during flushbuffer
  except KeyboardInterrupt:
    flushbuffer(conn, curs, buffer)

    print 'shutting down...'
    gps.unsubscribe()
    conn.close()
    print 'shut down complete'
