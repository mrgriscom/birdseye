


class texcache:
  def __init__(self, max_entries=20000, max_size=2**27):
    self.max_entries = max_entries
    self.max_size = max_size

    self.entries = {}
    self.tiles = {}




class mru_cache:
  def __init__(self, fetch, max, metric=lambda: 1):
    self.fetch = fetch
    self.max = max
    self.metric = metric
    self.cache = {}
    self.access_count = 0
    self.size = 0

  def get (self, key):
    if key in self.cache:
      (val, _) = self.cache[key]
    else:
      val = self.fetch(key)
      if val == None:
        raise ValueError('no value fetchable for key [%s]' % str(key))

      self.size += self.metric(val)
      while self.size > self.max:
        oldest_key = min(self.cache, key=lambda k: self.cache[k][1])
        oldest_val = self.cache[oldest_key]
        self.size -= self.metric(oldest_val)
        del self.cache[oldest_key]

    self.cache[key] = (val, self.access_count)
    self.access_count += 1
    return val


