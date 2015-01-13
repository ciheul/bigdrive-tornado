# urls:
# http://reminiscential.wordpress.com/2012/04/07/realtime-notification-delivery-using-rabbitmq-tornado-and-websocket/

# TODO
# - refactor out PikaConnection
# - subclass two new classes: Publisher and Consumer

import tornado.ioloop
import tornado.web
from tornado import gen
from tornado.concurrent import run_on_executor

from futures import ThreadPoolExecutor

import simplejson as json


from rabbitmq_manager import PikaConnection

from time import sleep


PORT = 22000
AMQP_URL = 'amqp://guest:guest@localhost:5672/%2F'

# sample polyline for leaflet
cs = [
    { 'lat': -6.890848, 'lon': 107.610982 },
    { 'lat': -6.891040, 'lon': 107.610392 },
    { 'lat': -6.893223, 'lon': 107.610456 },
    { 'lat': -6.893777, 'lon': 107.613020 },
    { 'lat': -6.898975, 'lon': 107.612784 },
    { 'lat': -6.899028, 'lon': 107.616035 },
    { 'lat': -6.899337, 'lon': 107.618052 },
    { 'lat': -6.899252, 'lon': 107.626066 },
    { 'lat': -6.899870, 'lon': 107.629285 },
    { 'lat': -6.897846, 'lon': 107.634242 },
    { 'lat': -6.900275, 'lon': 107.644992 },
    { 'lat': -6.901574, 'lon': 107.647760 },
    { 'lat': -6.902320, 'lon': 107.660399 },
    { 'lat': -6.903534, 'lon': 107.661622 },
    { 'lat': -6.904528, 'lon': 107.671535 },
    { 'lat': -6.904876, 'lon': 107.673080 },
    { 'lat': -6.904620, 'lon': 107.675634 },
]


class SSEHandler(tornado.web.RequestHandler):
    def initialize(self):
        self.set_header('Content-Type', 'text/event-stream')
        self.set_header('Cache-Control', 'no-cache')

    @tornado.web.asynchronous
    def emit(self, data, event=None):
        response = u''
        encoded_data = data
        #encoded_data = json.dumps(data)
        if event != None:
            response += u'event: ' + unicode(event).strip() + u'\n'

        response += u'data: ' + encoded_data.strip() + u'\n\n'

        print response.strip()

        self.write(response)
        self.flush()


class EventHandler(SSEHandler):
    def initialize(self):
        self.application.pc.add_event_listener(self)
        super(EventHandler, self).initialize()

    def get(self):
        self.emit("testing")

    def on_connection_closed(self):
        print "on_connection_closed"


class PlayApi(tornado.web.RequestHandler):
    executor = ThreadPoolExecutor(max_workers=4)

    def initialize(self):
        self.set_header('Content-Type', 'application/json')

    @gen.coroutine
    def get(self):
        self.play()
        self.write(json.dumps({ 'success': 0 }))
        self.flush()

    @run_on_executor
    def play(self):
        """Publish line in a background task."""
        for i, c in enumerate(cs):
            if i == 0:
                line = {
                    'type': 'path',
                    'lat1': c['lat'], 'lon1': c['lon'],
                    'lat2': c['lat'], 'lon2': c['lon'],
                }
            else:
                line = {
                    'type': 'path',
                    'lat1': cs[i-1]['lat'], 'lon1': cs[i-1]['lon'],
                    'lat2': c['lat'], 'lon2': c['lon'],
                }
        
            kwargs = { 'message': json.dumps(line) }
            self.application.pc.publish_message(**kwargs)
            print " [x] Sent:", kwargs['message']
            sleep(1)


class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.render('views/index.html')


application = tornado.web.Application([
    (r'/', MainHandler),
    (r'/sse/', EventHandler),
    (r'/api/play/', PlayApi),
], debug=True)


if __name__ == '__main__':
    print "Push server is running on port", PORT

    #logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

    io_loop = tornado.ioloop.IOLoop.instance()

    pc = PikaConnection(io_loop, AMQP_URL)
    application.pc = pc
    application.pc.connect()
    application.listen(PORT)

    try:
        io_loop.start()
    except KeyboardInterrupt:
        print "finish."
