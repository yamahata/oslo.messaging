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

import contextlib
import threading

import mock

from oslo import messaging
from oslo.messaging import proxy
from tests import utils


class TestProxyDispatcher(utils.BaseTestCase):
    def _setup_server(self, src_transport, src_target,
                      dst_transport, dst_target):
        proxy_server = proxy.get_proxy_server(
            src_transport, src_target, None, dst_transport, dst_target, None)
        self._proxy_server = proxy_server

        thread = threading.Thread(target=proxy_server.start)
        thread.daemon = True
        thread.start()
        self._thread = thread

    def _stop_server(self, src_transport):
        self._proxy_server.stop()
        dummy_target = messaging.Target(topic='_stop_server',
                                        server='_stop_server')
        src_transport._send(dummy_target, {}, {})
        self._proxy_server.wait()
        self._proxy_server = None

        self._thread.join(1)
        self._thread = None

    def _test_proxy(self, wait_for_reply, timeout):
        src_transport = messaging.get_transport(self.conf, url='fake:')
        src_target = messaging.Target(topic='foo_src', server='bar_src')
        dst_transport = messaging.get_transport(self.conf, url='fake:')
        dst_target = messaging.Target(topic='foo_dst', server='bar_dst')

        self._setup_server(src_transport, src_target,
                           dst_transport, dst_target)

        print('dst_transport', dst_transport)
        with contextlib.nested(
            mock.patch.object(dst_transport, '_send', return_value=None),
            mock.patch('oslo.messaging.proxy.dispatcher._wait_for_reply',
                       return_value=wait_for_reply)) as (mock_send,
                                                         mock_wait_for_reply):
            ctxt = {'ctxt_key': 'ctxt_value'}
            args = {'msg_key': 'msg_value'}
            message = {'args': args}
            src_transport._send(src_target, ctxt, message,
                                wait_for_reply=wait_for_reply, timeout=timeout)

            if not wait_for_reply:
                self._stop_server(src_transport)
            self.assertEqual(mock_send.call_count, 1)
            mock_send.assert_called_with(dst_target, ctxt, message,
                                         wait_for_reply, None)
        if wait_for_reply:
            self._stop_server(src_transport)

    def test_proxy_nowait(self):
        self._test_proxy(False, None)

    def test_proxy_wait_notimeout(self):
        self._test_proxy(True, None)

    def test_proxy_wait_timeout(self):
        self._test_proxy(True, 1)


class TestProxyServer(utils.BaseTestCase):
    def _setup_server(self, src_transport, src_target,
                      dst_transport, dst_target, endpoint):
        self._server = messaging.get_rpc_server(dst_transport, dst_target,
                                                [endpoint, self])
        self._server_thread = threading.Thread(target=self._server.start)
        self._server_thread.daemon = True
        self._server_thread.start()

        proxy_server = proxy.get_proxy_server(
            src_transport, src_target, None, dst_transport, dst_target, None)
        self._proxy_server = proxy_server

        proxy_thread = threading.Thread(target=proxy_server.start)
        proxy_thread.daemon = True
        proxy_thread.start()
        self._proxy_thread = proxy_thread

    def _stop(self, ctxt):
        self._server.stop()
        self._server.wait()

    def _stop_server(self, client):
        client.cast({}, '_stop')
        self._server_thread.join(1)

        self._proxy_server.stop()
        client.cast({}, '_stop')
        self._proxy_server.wait()
        self._proxy_thread.join(1)

    def _setup_client(self, transport, topic):
        return messaging.RPCClient(transport, messaging.Target(topic=topic))

    def _test_proxy_timeout(self, wait_for_reply, timeout):
        src_transport = messaging.get_transport(self.conf, url='fake:')
        src_target = messaging.Target(topic='foo_src', server='bar_src')
        dst_transport = messaging.get_transport(self.conf, url='fake:')
        dst_target = messaging.Target(topic='foo_dst', server='bar_dst')

        finished = False
        wait = threading.Condition()

        class TestEndpoint(object):
            def ping(self, ctxt, arg):
                with wait:
                    if not finished:
                        wait.wait()

        self._setup_server(src_transport, src_target,
                           dst_transport, dst_target, TestEndpoint())
        client = self._setup_client(src_transport, src_target.topic)
        try:
            client.prepare(timeout=0).call({}, 'ping', arg='testarg')
        except Exception as ex:
            self.assertIsInstance(ex, messaging.MessagingTimeout, ex)
        else:
            self.assertTrue(False)

        with wait:
            finished = True
            wait.notify()
        self._stop_server(client)

    def test_cast(self):
        src_transport = messaging.get_transport(self.conf, url='fake:')
        src_target = messaging.Target(topic='foo_src', server='bar_src')
        dst_transport = messaging.get_transport(self.conf, url='fake:')
        dst_target = messaging.Target(topic='foo_dst', server='bar_dst')

        class TestEndpoint(object):
            def __init__(self):
                self.pings = []

            def ping(self, ctxt, arg):
                self.pings.append(arg)

        endpoint = TestEndpoint()
        self._setup_server(src_transport, src_target,
                           dst_transport, dst_target, endpoint)
        client = self._setup_client(src_transport, src_target.topic)

        client.cast({}, 'ping', arg=[])
        client.cast({}, 'ping', arg={})
        client.cast({}, 'ping', arg='foo')
        client.cast({}, 'ping', arg='bar')

        self._stop_server(client)
        self.assertEqual(endpoint.pings, [[], {}, 'foo', 'bar'])

    def test_call(self):
        src_transport = messaging.get_transport(self.conf, url='fake:')
        src_target = messaging.Target(topic='foo_src', server='bar_src')
        dst_transport = messaging.get_transport(self.conf, url='fake:')
        dst_target = messaging.Target(topic='foo_dst', server='bar_dst')

        class TestEndpoint(object):
            def ping(self, ctxt, arg):
                return ('return', arg)

        self._setup_server(src_transport, src_target,
                           dst_transport, dst_target, TestEndpoint())
        client = self._setup_client(src_transport, src_target.topic)

        self.assertEqual(client.call({}, 'ping', arg=None), ('return', None))
        self.assertEqual(client.call({}, 'ping', arg=0), ('return', 0))
        self.assertEqual(client.call({}, 'ping', arg=False), ('return', False))
        self.assertEqual(client.call({}, 'ping', arg=[]), ('return', []))
        self.assertEqual(client.call({}, 'ping', arg={}), ('return', {}))
        self.assertEqual(client.call({}, 'ping', arg='foo'), ('return', 'foo'))

        self._stop_server(client)
