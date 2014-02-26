# Copyright 2014 Intel Corporation.
# Copyright 2014 Isaku Yamahata <isaku.yamahata at intel com>
#                               <isaku.yamahata at gmail com>
# All Rights Reserved.
#
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.
#
# @author: Isaku Yamahata, Intel Corporation.

import greenlet
import logging
import os
import os.path
import pickle
import socket
import threading
import time
import uuid

import eventlet
from oslo.config import cfg
from six import moves

from oslo import messaging
from oslo.messaging._drivers import base
from oslo.messaging._drivers import common


_ = lambda s: s
LOG = logging.getLogger(__name__)
unix_opts = [
    cfg.StrOpt('rpc_unix_ipc_dir',
               default='/var/run/opentsack/rpc_unix_ipc_dir',
               help=_('Directory for holding UNIX domain socket for RPC')),
]


class RpcContext(common.CommonRpcContext):
    _TARGET_KEYS = [
        'exchange', 'topic', 'namespace', 'version', 'server', 'fanout']
    _MESSAGE_KEYS = ['msg_id', 'reply_q', 'conf']
    _ADDITIONAL_KEYS = _TARGET_KEYS + _MESSAGE_KEYS

    def __init__(self, **kwargs):
        for key in self._ADDITIONAL_KEYS:
            setattr(self, key, kwargs.pop(key, None))
        super(RpcContext, self).__init__(**kwargs)

    # for debug
    def __str__(self):
        values = self.to_dict()
        values.update((attr, getattr(self, attr))
                      for attr in self._ADDITIONAL_KEYS)
        return str(values)

    def deepcopy(self):
        values = self.to_dict()
        values.update((key, getattr(self, key))
                      for key in self._ADDITIONAL_KEYS)
        return self.__class__(**values)

    @classmethod
    def unpack(cls, conf, msg):
        """Unpack context from msg."""
        ctxt_dict = {}
        for key in list(msg.keys()):
            # NOTE(vish): Some versions of Python don't like unicode keys
            #             in kwargs.
            key = str(key)
            if key.startswith('_context_'):
                value = msg.pop(key)
                ctxt_dict[key[9:]] = value

        for key in cls._MESSAGE_KEYS:
            ctxt_dict[key] = msg.pop('_%s' % key, None)
        ctxt_dict['conf'] = conf
        ctxt = cls.from_dict(ctxt_dict)
        common._safe_log(LOG.debug, _('unpacked context: %s'), ctxt.to_dict())
        return ctxt

    @staticmethod
    def _msg_update(msg, dict_value):
        msg.update(('_context_%s' % key, value) for (key, value) in dict_value)

    @classmethod
    def pack(cls, msg, target, context):
        if not isinstance(context, dict):
            context = context.to_dict()
        cls._msg_update(msg, context.items())
        cls._msg_update(msg, ((key, getattr(target, key))
                              for key in cls._TARGET_KEYS))


class UnixIncomingMessage(base.IncomingMessage):
    def __init__(self, listener, ctxt, msg, requeue):
        super(UnixIncomingMessage, self).__init__(listener, ctxt.to_dict(),
                                                  dict(msg))
        self._msg_id = getattr(ctxt, 'msg_id')
        self._reply_q = getattr(ctxt, 'reply_q')
        self._requeue_callback = requeue

    def reply(self, reply=None, failure=None, log_failure=True):
        if failure:
            failure = common.serialize_remote_exception(failure, log_failure)
        msg = {
            'result': reply, 'failure': failure,
            '_context_topic': self._reply_q, '_msg_id': self._msg_id,
        }
        self.listener.driver.reply(msg)

    def requeue(self):
        self._requeue_callback()


class UnixListener(base.Listener):
    _QUEUE_MAXSIZE = 256

    def __init__(self, driver):
        super(UnixListener, self).__init__(driver)
        self._incoming = moves.queue.Queue(self._QUEUE_MAXSIZE)

    def __call__(self, ctxt, msg):
        common._safe_log(LOG.debug, 'ctxt %(ctxt)s, received %(msg)s',
                         {'ctxt': ctxt, 'msg': msg})

        def requeue():
            self._incoming.put(unix_incoming_message)
        unix_incoming_message = UnixIncomingMessage(self, ctxt, msg, requeue)
        self._incoming.put(unix_incoming_message)

    def poll(self):
        msg = self._incoming.get()
        return msg


class UnixSender(object):
    _QUEUE_MAXSIZE = 256

    def __init__(self, get_socket):
        self._q = moves.queue.Queue(self._QUEUE_MAXSIZE)
        self._thread = eventlet.spawn(self._loop, get_socket)

    def _loop(self, get_socket):
        f = get_socket().makefile()
        while True:
            msg = self._q.get()
            pickle.dump(msg, f)

            for i in xrange(self._QUEUE_MAXSIZE):
                try:
                    msg = self._q.get_nowait()
                except moves.queue.Empty:
                    break
                pickle.dump(msg, f)
            f.flush()
            time.sleep(0)

    def kill(self):
        self._thread.kill()

    def wait(self):
        try:
            self._thread.wait()
        except greenlet.GreenletExit:
            pass

    def send(self, message):
        self._q.put(message)


class UnixExchange(object):
    def __init__(self, name):
        self.name = name
        self._queues_lock = threading.Lock()
        self._topic_queues = {}
        self._server_queues = {}

    def _get_topic_queue(self, topic):
        return self._topic_queues.setdefault(topic, [])

    def _get_server_queue(self, server, topic):
        return self._server_queues.setdefault((topic, server), [])

    def subscribe_topic(self, topic, callback):
        assert callable(callback)
        self._get_topic_queue(topic).append(callback)

    def subscribe_server(self, topic, server, callback):
        assert callable(callback)
        self._get_server_queue(topic, server).append(callback)

    def deliver_message(self, ctxt, msg):
        topic = ctxt.topic
        with self._queues_lock:
            if ctxt.fanout:
                queues = [q for t, q in self._server_queues.items()
                          if t[0] == topic]
            elif ctxt.server is not None:
                queues = self._get_server_queue(topic, ctxt.server)
            else:
                queues = self._get_topic_queue(topic)
            for queue in queues:
                queue(ctxt, msg)


class UnixReciever(object):
    def __init__(self, conf, get_socket, default_exchange):
        self.conf = conf
        self._default_exchange = default_exchange

        self._exchanges_lock = threading.Lock()
        self._exchanges = {}

        self._thread = eventlet.spawn(self._loop, get_socket)

    def _loop(self, get_socket):
        f = get_socket().makefile()
        while True:
            msg = pickle.load(f)
            ctxt = RpcContext.unpack(self.conf, msg)
            exchange = self._get_exchange(ctxt.exchange)
            exchange.deliver_message(ctxt, msg)
            time.sleep(0)

    def kill(self):
        self._thread.kill()

    def wait(self):
        try:
            self._thread.wait()
        except greenlet.GreenletExit:
            pass

    def _get_exchange(self, name=None):
        if name is None:
            name = self._default_exchange
        with self._exchanges_lock:
            return self._exchanges.setdefault(name, UnixExchange(name))

    def subscribe_topic(self, topic, callback=None, exchange_name=None):
        exchange = self._get_exchange(exchange_name)
        exchange.subscribe_topic(topic, callback)

    def subscribe_server(self, topic, server, callback=None,
                         exchange_name=None):
        exchange = self._get_exchange(exchange_name)
        exchange.subscribe_server(topic, server, callback)


class ReplyWaiter(object):
    def __init__(self, allowed_remote_exmods):
        super(ReplyWaiter, self).__init__()
        self._allowed_remote_exmods = allowed_remote_exmods

        self._waiters_lock = threading.Lock()
        self._waiters = {}
        self._warn_threshold = 10

    def __call__(self, ctxt, message):
        msg_id = ctxt.msg_id
        result = message['result']
        if message['failure']:
            failure = message['failure']
            result = common.deserialize_remote_exception(
                failure, self._allowed_remote_exmods)
        with self._waiters_lock:
            q = self._waiters[msg_id]
            if q:
                q.put(result)
            else:
                LOG.warn(_('No calling threads waiting for '
                           'msg_id: %(msg_id)s, message %(data)s'),
                         {'msg_id': msg_id, 'data': message})

    def listen(self, msg_id):
        with self._waiters_lock:
            assert msg_id not in self._waiters
            self._waiters[msg_id] = moves.queue.Queue()
            if len(self._waiters) > self._warn_threshold:
                LOG.warn(_('Number of call queues is greater than %d. '
                           'There could be a leak'), self._warn_threshold)
                self._warn_threshold *= 2

    def unlisten(self, msg_id):
        with self._waiters_lock:
            del self._waiters[msg_id]

    def wait(self, msg_id, timeout):
        with self._waiters_lock:
            q = self._waiters[msg_id]
        try:
            return q.get(timeout=timeout)
        except moves.queue.Empty:
            raise messaging.MessagingTimeout(
                _('timed out waiting for a replay: message ID %(msg_id)s '
                  'timeout %(timeout)d'), {'msg_id': msg_id,
                                           'timoeout': timeout})


class UnixDriver(base.BaseDriver):
    @staticmethod
    def _unix_ensure_dir(conf, path):
        unix_ipc_dir = conf.rpc_unix_ipc_dir
        if not os.path.isdir(unix_ipc_dir):
            os.makedirs(unix_ipc_dir)
        return os.path.join(unix_ipc_dir, path)

    @staticmethod
    def _punix_listen(conf, path):
        path = UnixDriver._unix_ensure_dir(conf, path)

        try:
            os.unlink(path)
        except OSError:
            pass
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.bind(path)
        sock.listen(5)
        return sock

    @staticmethod
    def _punix_accept(listen_socket):
        sock, _addr = listen_socket.accept()
        return sock

    @staticmethod
    def _punix_cleanup(conf, path):
        path = UnixDriver._unix_ensure_dir(conf, path)
        try:
            os.unlink(path)
        except OSError:
            pass

    @staticmethod
    def _unix_connect(conf, path):
        path = UnixDriver._unix_ensure_dir(conf, path)
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.connect(path)
        return sock

    def __init__(self, conf, url,
                 default_exchange=None, allowed_remote_exmods=None):
        conf.register_opts(unix_opts)

        allowed_remote_exmods = allowed_remote_exmods or []
        super(UnixDriver, self).__init__(conf, url, default_exchange,
                                         allowed_remote_exmods)

        self._is_passive = url.transport.startswith('p')
        self._reply_q = 'reply_' + uuid.uuid4().hex
        if url.virtual_host is None:
            url.virtual_host = uuid.uuid4().hex
        self._path = url.virtual_host

        self._waiter = ReplyWaiter(allowed_remote_exmods)
        self._socket_lock = threading.Lock()
        if self._is_passive:
            self._listen_socket = self._punix_listen(self.conf, self._path)
        else:
            self._listen_socket = None

        self._socket = None
        self._sender = UnixSender(self._get_socket)
        self._receiver = UnixReciever(self.conf, self._get_socket,
                                      default_exchange)

    def require_features(self, requeue=True):
        pass

    def _get_socket(self):
        with self._socket_lock:
            if self._socket is not None:
                return self._socket
            if self._is_passive:
                sock = self._punix_accept(self._listen_socket)
                self._listen_socket.close()
                self._listen_socket = None
            else:
                sock = self._unix_connect(self.conf, self._path)

            self._socket = sock
            self._receiver.subscribe_topic(self._reply_q, self._waiter)
            return self._socket

    def reply(self, msg):
        assert self._socket is not None
        self._sender.send(msg)

    def _send(self, target, ctxt, message, wait_for_reply=None, timeout=None,
              envelope=False):
        assert not envelope

        msg = message
        RpcContext.pack(msg, target, ctxt)
        if wait_for_reply:
            msg_id = uuid.uuid4().hex
            msg['_msg_id'] = msg_id
            msg['_reply_q'] = self._reply_q
            self._waiter.listen(msg_id)
        try:
            self._sender.send(msg)
            if wait_for_reply:
                result = self._waiter.wait(msg_id, timeout)
                if isinstance(result, Exception):
                    raise result
                return result
        finally:
            if wait_for_reply:
                self._waiter.unlisten(msg_id)

    def send(self, target, ctxt, message,
             wait_for_reply=None, timeout=None, envelope=False):
        return self._send(target, ctxt, message, wait_for_reply, timeout,
                          envelope)

    def send_notification(self, target, ctxt, message, version):
        return self._send(target, ctxt, message, envelope=(version == 2.0))

    def listen(self, target):
        listener = UnixListener(self)
        self._receiver.subscribe_topic(target.topic, listener)
        self._receiver.subscribe_server(target.topic, target.server, listener)
        return listener

    def listen_for_notifications(self, targets_and_priorities):
        listener = UnixListener(self)
        for target, priority in targets_and_priorities:
            topic = '%s.%s' % (target.topic, priority)
            self._receiver.subscribe_topic(topic, listener)
        return listener

    def cleanup(self):
        if self._sender:
            self._sender.kill()
            self._sender.wait()
        if self._receiver:
            self._receiver.kill()
            self._receiver.wait()
        if self._socket:
            self._socket.close()
            self._socket = None
        if self._is_passive:
            self._punix_cleanup(self.conf, self._path)
