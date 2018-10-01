import sys
import time
import socket
import json

from tornado import gen
import tornado.ioloop
import tornado.web
import requests
from nsq.reader import Reader
from contextlib import closing
from basescript import BaseScript
import Queue
from deeputil import generate_random_string, AttrDict, keeprunning 
from logagg_utils import NSQSender
from logagg_utils import InvalidArgument, AuthenticationFailure
from logagg_utils import start_daemon_thread, log_exception

class NsqAPI(tornado.web.RequestHandler):

    NSQ_MAX_IN_FLIGHT = 200
    CHANNEL_NAME_LENGTH = 6
    DELETE_CHANNEL_URL = 'http://{}/channel/delete?topic={}&channel={}'

    def initialize(self, log):

        self.log = log

    def _parse_query(self, query_arguments):
        '''
        query_arguments: {'topic': ['Heartbeat'], 'nsqd_tcp_address': ['195.201.98.142:4150']}
        '''

        nsqd_tcp_addresses=query_arguments['nsqd_tcp_address']
        topic = query_arguments['topic'][0]

        return nsqd_tcp_addresses, topic

    def _remove_channel(self, nsqd_host,
            topic, channel):
        '''
        Delete a Nsq channel
        '''

        nsqd_http_port = '4151'
        nsqd_http_address = nsqd_host + ':' + nsqd_http_port

        requests.post(self.DELETE_CHANNEL_URL.format(nsqd_http_address,
            topic, channel))

    @gen.coroutine
    def get(self):
        '''
        Sample uri: /tail?nsqd_tcp_address=195.201.98.142:4150&topic=Heartbeat
        '''
        loop = tornado.ioloop.IOLoop.current()

        nsqd_tcp_addresses, topic = self._parse_query(self.request.query_arguments)
        channel = generate_random_string(self.CHANNEL_NAME_LENGTH)
        nsqd_host = nsqd_tcp_addresses[0].split(':')[0]


        nsq_reader = Reader(nsqd_tcp_addresses=nsqd_tcp_addresses,
                topic=topic,
                channel=channel,
                max_in_flight=self.NSQ_MAX_IN_FLIGHT)

        def cleanup():
            nsq_reader.close()
            self._remove_channel(nsqd_host=nsqd_host,
                    topic=topic, channel=channel)
            self.log.info('channel_deleted', channel=channel)
            self.finish()

        try:
            for msg in nsq_reader:
                if self.request.connection.stream.closed():
                    self.log.info('stream_closed')
                    cleanup()
                    break
                else:
                    try:
                        self.log.info('writing_to_socket')
                        self.write(msg.body)
                        msg.fin()
                        yield self.flush()
                    except Exception as e:
                        self.log.warn('error', exp=e)
                        break
        except KeyboardInterrupt:
            cleanup()
            sys.exit(0)


class NsqServer(BaseScript):
    NAME = 'NsqServer'
    DESC = 'Reads nsq topics'
    NAMESPACE = 'nsq_api'
    REGISTER_URL = 'http://{master_address}/logagg/v1/register_component?namespace={namespace}&cluster_name={cluster_name}&cluster_passwd={cluster_passwd}&host={host}&port={port}'
    GET_NSQ_URL = 'http://{master_address}/logagg/v1/get_nsq?cluster_name={cluster_name}' 
    HEARTBEAT_RESTART_INTERVAL = 5 

    def _init_nsq_sender(self):
        '''
        Initialize nsq sender to send heartbeats
        '''
        url = self.GET_NSQ_URL.format(master_address=self.master.host+':'+self.master.port,
                cluster_name=self.master.cluster_name)

        resp = json.loads(requests.get(url).content)
        nsqd_http_address = resp['result']['nsqd_http_address']
        heartbeat_topic = resp['result']['heartbeat_topic']
        nsq_depth_limit = resp['result']['nsq_depth_limit']

        nsq_sender_heartbeat = NSQSender(nsqd_http_address,
                heartbeat_topic,
                self.log)
        
        return nsq_sender_heartbeat

    def _parse_master_args(self):
        master = AttrDict()
        try:
            m = self.args.master.split(':')
            for a in m:
                a = a.split('=')
                if a[0] == 'host': master.host = a[-1]
                elif a[0] == 'port': master.port = a[-1]
                elif a[0] == 'cluster_name': master.cluster_name = a[-1]
                elif a[0] == 'cluster_passwd': master.cluster_passwd = a[-1]
                else: raise ValueError
        except ValueError:
            raise InvalidArgument(self.args.master)

        return master
 

    def register_to_master(self):
        '''
        'http://localhost:1088/logagg/v1/register_component?namespace=master&cluster_name=logagg&passwd=ad9379b4&host=78.47.113.210&port=1088'
        '''
        master = self.master

        url = self.REGISTER_URL.format(master_address=master.host+':'+master.port,
                                namespace=self.NAMESPACE,
                                cluster_name=master.cluster_name,
                                cluster_passwd=master.cluster_passwd,
                                host=self.host,
                                port=self.port)

        return json.loads(requests.get(url).content)


    @keeprunning(HEARTBEAT_RESTART_INTERVAL, on_error=log_exception)
    def send_heartbeat(self, state):
        '''
        Sends continuous heartbeats to a seperate topic in nsq
        '''
        heartbeat_payload = {'host': self.host,
                'port': self.port,
                'namespace': self.NAMESPACE,
                'cluster_name': self.master.cluster_name,
                'timestamp': time.time(),
                'heartbeat_number': state.heartbeat_number,
                }
        self.nsq_sender_heartbeat.handle_heartbeat(heartbeat_payload)
        state.heartbeat_number += 1
        time.sleep(self.HEARTBEAT_RESTART_INTERVAL)

    def start_heartbeat(self):
        state = AttrDict(heartbeat_number=0)
        th_heartbeat = start_daemon_thread(self.send_heartbeat, (state,))

        return th_heartbeat


    def prepare_api(self):
        return tornado.web.Application([
        (r'/tail', NsqAPI,
        dict(log=self.log)),
        ])

    def start(self):
        self.master = self._parse_master_args()
        self.host = self.args.host
        self.port = self.args.port
        self.nsq_sender_heartbeat = self._init_nsq_sender()

        register_response = self.register_to_master()
        if register_response['result']['authentication'] == 'passed':
            self.log.info('authentication_passed')
            app = self.prepare_api()
            app.listen(self.args.port)
            self.th_heartbeat = self.start_heartbeat()
        else:
            raise AuthenticationFailure(register_response)

        try:
            tornado.ioloop.IOLoop.current().start()
        except KeyboardInterrupt:
            # FIXME: cleanup if exited
            self.log.info('exiting')


    def define_subcommands(self, subcommands):
        super(NsqServer, self).define_subcommands(subcommands)

        runserver_cmd = subcommands.add_parser('runserver',
                help='NsqServer Service')

        runserver_cmd.set_defaults(func=self.start)

        runserver_cmd.add_argument('-p', '--port',
                default=1077,
                help='NsqServer port, default: %(default)s')
        runserver_cmd.add_argument('-i', '--host',
                default=socket.gethostname(),
                help='Hostname of this service for other components to contact to, default: %(default)s')
        runserver_cmd.add_argument('-m', '--master',
                required=True,
                help='Master address, in host=<hostname>:port=<port>:cluster_name=<name>:cluster_passwd=<cluster_passwd> format')

def main():
    NsqServer().start()

if __name__ == '__main__':
    main()
