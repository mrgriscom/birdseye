db = 'navdata'
tile_dir = '/home/drew/tiles/'
gps_device = '/dev/ttyUSB0'
baud_rate = 57600
app_name = 'birdseye'

sample_window = 0.1 #s, how far apart the GPS timestamps can be before being considered separate samples
buffer_window = 0.3 #s, how long we wait for all messages to come in before dispatching the sample
