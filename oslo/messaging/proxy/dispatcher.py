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
import logging
import sys

import six

from oslo.messaging._drivers import base
from oslo.messaging.rpc import client
from oslo.messaging.rpc import dispatcher
from oslo.messaging import serializer as msg_serializer

LOG = logging.getLogger(__name__)


# TODO(yamahata): make this as abstract method of base.IncomingMessage
def _wait_for_reply(incoming):
    # avoid circular import
    from oslo.messaging._drivers import amqpdriver
    from oslo.messaging._drivers import impl_fake
    from oslo.messaging._drivers import impl_unix
    from oslo.messaging._drivers import impl_zmq

    if isinstance(incoming, amqpdriver.AMQPIncomingMessage):
        return '_msg_id' in incoming.message
    if isinstance(incoming, impl_unix.UnixIncomingMessage):
        return '_msg_id' in incoming.message
    if isinstance(incoming, impl_zmq.ZmqIncomingMessage):
        return incoming.message.get('method') == '-reply'
    if isinstance(incoming, impl_fake.FakeIncomingMessage):
        return incoming._reply_q is not None
    # Add more when more drivers are added
    return True


# TODO(yamahata): make this as abstract method of base.IncomingMessage
def _timeout(incoming):
    # there is no way to deduce timeout from incoming
    return None


class ProxyDispatcher(object):
    def __init__(self, src_target, src_serializer,
                 dst_transport, dst_target, dst_serializer):
        self._src_target = src_target
        self._src_serializer = (src_serializer or
                                msg_serializer.NoOpSerializer())
        self._dst_transport = dst_transport
        self._dst_target = dst_target
        self._dst_serializer = (dst_serializer or
                                msg_serializer.NoOpSerializer())

    def _listen(self, transport):
        return transport._listen(self._src_target)

    @contextlib.contextmanager
    def __call__(self, incoming):
        incoming.acknowledge()
        yield lambda: self._proxy_and_reply(incoming)

    def _proxy_and_reply(self, incoming):
        try:
            incoming.reply(self._proxy(incoming))
        except dispatcher.ExpectedException as e:
            LOG.debug('Expected exception during message handling (%s)' %
                      e.exc_info[1])
            incoming.reply(failure=e.exc_info, log_failure=False)
        except Exception as e:
            # sys.exc_info() is deleted by LOG.exception().
            exc_info = sys.exc_info()
            LOG.error('Exception during message handling: %s', e,
                      exc_info=exc_info)
            incoming.reply(failure=exc_info)
            # NOTE(dhellmann): Remove circular object reference
            # between the current stack frame and the traceback in
            # exc_info.
            del exc_info

    def _proxy(self, incoming):
        wait_for_reply = _wait_for_reply(incoming)
        timeout = _timeout(incoming)

        ctxt = self._src_serializer.deserialize_context(incoming.ctxt)
        msg_ctxt = self._dst_serializer.serialize_context(ctxt)
        msg = incoming.message.copy()
        args = msg.get('args', {})
        new_args = {}
        for argname, arg in six.iteritems(args):
            arg = self._src_serializer.deserialize_entity(ctxt, arg)
            arg = self._dst_serializer.serialize_entity(ctxt, arg)
            new_args[argname] = arg
        msg['args'] = new_args
        try:
            result = self._dst_transport._send(
                self._dst_target, msg_ctxt, msg, wait_for_reply, timeout)
        except base.TransportDriverError as ex:
            raise client.ClientSendError(self._dst_target, ex)
        if wait_for_reply:
            return self._dst_serializer.deserialize_entity(ctxt, result)
