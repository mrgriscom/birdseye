import socket

class MessageSocket(object):
    """a socket wrapper that tries to deal in whole messages, instead of low-level
    byte buffers. ideally would use zero-mq, but this class is for talking to
    outside services that don't support it.

    supports timeouts, but main use case is for timeouts when no data has been
    sent/received. mid-transmission timeouts will probably hose the socket; best
    used for local socket communication only.
    """

    class MsgSocketException(Exception):
        """exception to wrap low-level socket errors"""
        def __init__(self, arg=None):
            if isinstance(arg, Exception):
                self.ex = arg
                message = '%s: %s' % (type(self.ex), str(self.ex))
            else:
                self.ex = None
                message = arg
            Exception.__init__(self, message)
            
    class ConnectionBroken(MsgSocketException):
        """established connection has been broken"""
        pass

    class ConnectionFailed(MsgSocketException):
        """unable to establish connection"""
        pass

    class ChannelCorrupted(MsgSocketException):
        """data transmission failed in a way we can't be bothered to recover from.
        should be very rare with small message payloads and localhost communication"""
        pass

    #socket.Timeout can also be thrown

    BUFSIZE = 2**12

    def __init__ (self, sock=None, timeout=1.):
        self.recvbuf = ''
        self.timeout = timeout

        self.socket = sock or socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._timeout(timeout)

    def connect (self, port, host='localhost', conn_timeout=15.):
        try:
            self._timeout(conn_timeout)
            self.socket.connect((host, port))
            self._timeout(self.timeout)
        except socket.error, e:
            raise self.ConnectionFailed(e)

    def _timeout (self, s):
        self.socket.settimeout(s)

    def close (self):
        self.socket.close()

    def readn (self, n):
        return self.readuntil(lambda data: len(data) >= n, lambda data: (n, 0))

    def readline (self, newline = '\r\n'):
        return self.readuntil(lambda data: newline in data, lambda data: (data.find(newline), len(newline)))

    def readuntil (self, satisfaction, length):
        data = self.recvbuf
        try:
            while not satisfaction(data):
                data += self.readbuf()
        except socket.timeout:
            self.recvbuf = data
            raise

        (datalen, skip) = length(data)
        self.recvbuf = data[datalen+skip:]
        return data[0:datalen]

    def readbuf (self):
        try:
            fragment = self.socket.recv(self.BUFSIZE)
            if len(fragment) == 0:
                raise self.ConnectionBroken('remote end closed connection gracefully')
            return fragment
        except socket.timeout:
            raise
        except socket.error, e:
            raise self.ConnectionBroken(e)

    def send (self, data):
        """send data, but don't try very hard"""
        try:
            sent = self.socket.send(data)
            if sent == 0:
                raise self.ConnectionBroken('remote end closed connection gracefully')
            elif sent != len(data):
                raise self.ChannelCorrupted('not all data sent')
        except socket.timeout:
            raise self.ChannelCorrupted('timeout during send')
        except socket.error, e:
            raise self.ConnectionBroken(e)

