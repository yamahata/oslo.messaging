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

import sys
import threading
import uuid

import eventlet
import testscenarios

from oslo import messaging
from oslo.messaging._drivers import common as driver_common
from oslo.messaging._drivers import impl_unix
from tests import utils as test_utils


eventlet.monkey_patch()


load_tests = testscenarios.load_tests_apply_scenarios


class TestUnixBase(test_utils.BaseTestCase):
    def setUp(self, transport_driver='unix'):
        super(TestUnixBase, self).setUp()
        self.conf.set_override('rpc_unix_ipc_dir', 'rpc_unix_ipc_dir')


class TestUnixDriverLoad(TestUnixBase):
    def setUp(self, transport_driver='unix'):
        super(TestUnixDriverLoad, self).setUp()
        self.messaging_conf.transport_driver = transport_driver

    def test_driver_load(self):
        transport = messaging.get_transport(self.conf)
        transport.cleanup()
        self.assertIsInstance(transport._driver, impl_unix.UnixDriver)


class TestPUnixDriverLoad(TestUnixDriverLoad):
    def setUp(self):
        super(TestPUnixDriverLoad, self).setUp('punix')


class TestSendReceive(TestUnixBase):

    _n_senders = [
        ('single_sender', dict(n_senders=1)),
        ('multiple_senders', dict(n_senders=10)),
    ]

    _context = [
        ('empty_context', dict(ctxt={})),
        ('with_context', dict(ctxt={'user': 'mark'})),
    ]

    _reply = [
        ('rx_id', dict(rx_id=True, reply=None)),
        ('none', dict(rx_id=False, reply=None)),
        ('empty_list', dict(rx_id=False, reply=[])),
        ('empty_dict', dict(rx_id=False, reply={})),
        ('false', dict(rx_id=False, reply=False)),
        ('zero', dict(rx_id=False, reply=0)),
    ]

    _failure = [
        ('success', dict(failure=False)),
        ('failure', dict(failure=True, expected=False)),
        ('expected_failure', dict(failure=True, expected=True)),
    ]

    _timeout = [
        ('no_timeout', dict(timeout=None)),
        ('timeout', dict(timeout=0.01)),  # FIXME(markmc): timeout=0 is broken?
    ]

    @classmethod
    def generate_scenarios(cls):
        cls.scenarios = testscenarios.multiply_scenarios(cls._n_senders,
                                                         cls._context,
                                                         cls._reply,
                                                         cls._failure,
                                                         cls._timeout)

    def setUp(self):
        super(TestSendReceive, self).setUp()
        self.messaging_conf.transport_driver = 'unix'

    def test_send_receive(self):
        unix_path = 'test-' + uuid.uuid4().hex
        topic = 'testtopic'

        server_transport = messaging.get_transport(self.conf,
                                                   'punix:///%s' % unix_path)
        self.addCleanup(server_transport.cleanup)
        server_driver = server_transport._driver

        client_transport = messaging.get_transport(self.conf,
                                                   'unix:///%s' % unix_path)
        self.addCleanup(client_transport.cleanup)
        client_driver = client_transport._driver

        target = messaging.Target(topic=topic)

        senders = []
        replies = []
        msgs = []
        errors = []

        def stub_error(msg, *a, **kw):
            if (a and len(a) == 1 and isinstance(a[0], dict) and a[0]):
                a = a[0]
            errors.append(str(msg) % a)

        self.stubs.Set(driver_common.LOG, 'error', stub_error)

        def send_and_wait_for_reply(i):
            try:
                replies.append(client_driver.send(target,
                                                  self.ctxt,
                                                  {'tx_id': i},
                                                  wait_for_reply=True,
                                                  timeout=self.timeout))
                self.assertFalse(self.failure)
                self.assertIsNone(self.timeout)
            except (ZeroDivisionError, messaging.MessagingTimeout) as e:
                replies.append(e)
                self.assertTrue(self.failure or self.timeout is not None)

        while len(senders) < self.n_senders:
            senders.append(threading.Thread(target=send_and_wait_for_reply,
                                            args=(len(senders), )))

        server_listener = server_driver.listen(target)
        for i in range(len(senders)):
            senders[i].start()

            received = server_listener.poll()
            self.assertIsNotNone(received)
            self.assertEqual(received.ctxt, self.ctxt)
            self.assertEqual(received.message, {'tx_id': i})
            msgs.append(received)

        # reply in reverse, except reply to the first guy second from last
        order = list(range(len(senders)-1, -1, -1))
        if len(order) > 1:
            order[-1], order[-2] = order[-2], order[-1]

        for i in order:
            if self.timeout is None:
                if self.failure:
                    try:
                        raise ZeroDivisionError
                    except Exception:
                        failure = sys.exc_info()
                    msgs[i].reply(failure=failure,
                                  log_failure=not self.expected)
                elif self.rx_id:
                    msgs[i].reply({'rx_id': i})
                else:
                    msgs[i].reply(self.reply)
            senders[i].join()

        self.assertEqual(len(replies), len(senders))
        for i, reply in enumerate(replies):
            if self.timeout is not None:
                self.assertIsInstance(reply, messaging.MessagingTimeout)
            elif self.failure:
                self.assertIsInstance(reply, ZeroDivisionError)
            elif self.rx_id:
                self.assertEqual(reply, {'rx_id': order[i]})
            else:
                self.assertEqual(reply, self.reply)

        if not self.timeout and self.failure and not self.expected:
            self.assertTrue(len(errors) > 0, errors)
        else:
            self.assertEqual(len(errors), 0, errors)


TestSendReceive.generate_scenarios()


class TestRpcContext(test_utils.BaseTestCase):
    _exchanges = [
        ('exchange', dict(exchange='test_exchange')),
        ('exchange_none', dict(exchange=None)),
    ]
    _topics = [
        ('topic', dict(topic='testtopic')),
        ('topic_none', dict(topic=None)),
    ]
    _namespaces = [
        ('namespace', dict(namespace='test_namespace')),
        ('namespace_none', dict(namespace=None)),
    ]
    _versions = [
        ('version', dict(version=1)),
        ('version_none', dict(version=None)),
    ]
    _servers = [
        ('server', dict(server='test-server')),
        ('server_none', dict(server=None)),
    ]
    _fanouts = [
        ('fanout', dict(fanout=True)),
        ('fanout_false', dict(fanout=False)),
        ('fanout_none', dict(fanout=None)),
    ]

    _msgs = [
        ('msg_empty', dict(msg={})),
        ('msg_msg_id', dict(msg={'_msg_id': 'msg_id'})),
        ('msg_reply_q', dict(msg={'_reply_q': 'reply_q'})),
        ('msg_reply', dict(msg={'_msg_id': 'msg_id', '_reply_q': 'reply_q'}))
    ]

    @classmethod
    def generate_scenarios(cls):
        cls.scenarios = testscenarios.multiply_scenarios(
            cls._exchanges, cls._topics, cls._namespaces, cls._versions,
            cls._servers, cls._fanouts, cls._msgs)

    def test_pack_unpack(self):
        target = messaging.Target(
            exchange=self.exchange, topic=self.topic, namespace=self.namespace,
            version=self.namespace, server=self.server, fanout=self.fanout)

        ctxt = impl_unix.RpcContext(
            exchange=self.exchange, topic=self.topic, namespace=self.namespace,
            versoin=self.version, server=self.server, fanout=self.fanout)
        msg = self.msg.copy()
        impl_unix.RpcContext.pack(msg, target, ctxt)
        ctxt_ret = impl_unix.RpcContext.unpack(None, msg)

        for attr in impl_unix.RpcContext._TARGET_KEYS:
            self.assertEqual(getattr(ctxt_ret, attr), getattr(target, attr))
        if '_msg_id' in self.msg:
            self.assertEqual(ctxt_ret.msg_id, self.msg['_msg_id'])
        self.assertNotIn('_msg_id', msg)
        if '_reply_q' in self.msg:
            self.assertEqual(ctxt_ret.reply_q, self.msg['_reply_q'])
        self.assertNotIn('_reply_q', msg)
        self.assertEqual(ctxt_ret.to_dict(), ctxt.to_dict())


TestRpcContext.generate_scenarios()
