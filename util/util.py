import time

EPSILON = 1.0e-9

def f_eq(a, b):
    try:
        return abs(a - b) < EPSILON
    except TypeError:
        return a == b

def wait(delay, abortfunc=lambda: False, increment=0.01):
    end_at = time.time() + delay
    while not abortfunc() and time.time() < end_at:
        time.sleep(increment)


