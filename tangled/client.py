import asyncore
import socket
import urlparse
import mimetools
import cStringIO
import core

class Response(object):
    def __init__(self):
        self.data = ""
        self.header = ""
        self.finished = False

    def close(self):
        self.finished = True

    def feed(self, data):
        self.data += data

    def http_header(self, header):
        self.header = header


class AsyncHTTPClient(asyncore.dispatcher_with_send):
    """Asynchronous HTTP client, based on 
    http://effbot.org/librarybook/SimpleAsyncHTTP.py
    """
    def __init__(self, url, command="GET", data="", consumer=None):
        asyncore.dispatcher_with_send.__init__(self)
        parsed = urlparse.urlparse(url)
        self._request = '%s %s HTTP/1.1\r\n' % (command, parsed.path)
        self._request = self._request + 'Host: %s\r\n' % parsed.netloc
        if len(data) > 0:
            self._request = self._request + 'Content-Length: %d\r\n\r\n%s' % (len(data), data)
        else:
            self._request = self._request + "\r\n"
        self.consumer = consumer
        if self.consumer is None:
            self.consumer = Response()
            self.response = self.consumer
        self.status = None
        self.header = None
        self.data = ""
        self._result = core.Deferred()
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        addr = parsed.netloc.split(":")
        host = addr[0]
        try:
            port = int(addr[1])
        except IndexError:
            port = 80        
        self.connect((host, port))

    def request(self):
        return self._result

    def handle_connect(self):
        print "sending request:", self._request
        self.send(self._request)

    def notify_header(self):
        self.consumer.http_header(self.header)

    def handle_expt(self):
        # connection failed; notify consumer (status is None)
        self.close()
        self.notify_header()
        print "connection failed"
        self._result.callback(self.response)

    def handle_read(self):
        data = self.recv(2048)        
        print "received:\n", data 
        if not self.header:
            self.data = self.data + data
            i = self.data.find("\r\n\r\n")
            if i != -1:
                # parse header
                fp = cStringIO.StringIO(self.data[:i+4])
                # status line is "HTTP/version status message"
                status = fp.readline()
                self.status = status.split(" ", 2)
                # followed by a rfc822-style message header
                self.header = mimetools.Message(fp)
                # followed by a newline, and the payload (if any)
                data = self.data[i+4:]
                self.data = ""
                # notify consumer (status is non-zero)
                self.notify_header()
                if not self.connected:
                    return # channel was closed by consumer

        if self.header:
            self.consumer.feed(data)

    def handle_close(self):
        self.consumer.close()
        self.close()
        print "connection closed"
        self._result.callback(self.response)

def request(url, command="GET", data=""):
    c = AsyncHTTPClient(url, command, data)
    return c.request()

if __name__=="__main__":
    import optparse

    parser = optparse.OptionParser()

    parser.add_option("-n", "--number", dest="number", type="int",
                      help="Number of connections")
    parser.add_option("-c", "--concurrent", dest="concurrent", type="int",
                      help="Number of concurrent connections")
    parser.add_option("-p", "--printresponse", dest="printresponse", action="store_true", default=False, 
                      help="Number of concurrent connections")

    (options, args) = parser.parse_args()

    num = 0
    for n in range(0, options.number, options.concurrent):
        clients = [AsyncHTTPClient(args[0]) for i in range(options.concurrent)]
        asyncore.loop()
        if options.printresponse:
            for c in clients:
                print c.response.header
                print c.response.data
        num += len(clients)
        print "Completed", num, "requests"
