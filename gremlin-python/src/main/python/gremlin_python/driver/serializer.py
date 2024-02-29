#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

import base64
import logging
import struct
import uuid
import io
try:
    import ujson as json
    if int(json.__version__[0]) < 2:
        logging.warning("Detected ujson version below 2.0.0. This is not recommended as precision may be lost.")
except ImportError:
    import json

from gremlin_python.structure.io import graphbinaryV1
from gremlin_python.structure.io import graphsonV2d0
from gremlin_python.structure.io import graphsonV3d0

__author__ = 'David M. Brown (davebshow@gmail.com), Lyndon Bauto (lyndonb@bitquilltech.com)'


class Processor:
    """Base class for OpProcessor serialization system."""

    def __init__(self, writer):
        self._writer = writer

    def get_op_args(self, op, args):
        op_method = getattr(self, op, None)
        if not op_method:
            raise Exception("Processor does not support op: {}".format(op))
        return op_method(args)


class Standard(Processor):

    def authentication(self, args):
        return args

    def eval(self, args):
        return args


class Session(Processor):

    def authentication(self, args):
        return args

    def eval(self, args):
        return args

    def close(self, args):
        return args

    def bytecode(self, args):
        gremlin = args['gremlin']
        args['gremlin'] = self._writer.to_dict(gremlin)
        aliases = args.get('aliases', '')
        if not aliases:
            aliases = {'g': 'g'}
        args['aliases'] = aliases
        return args


class Traversal(Processor):

    def authentication(self, args):
        return args

    def bytecode(self, args):
        gremlin = args['gremlin']
        args['gremlin'] = self._writer.to_dict(gremlin)
        aliases = args.get('aliases', '')
        if not aliases:
            aliases = {'g': 'g'}
        args['aliases'] = aliases
        return args


class GraphSONMessageSerializer(object):
    """
    Message serializer for GraphSON. Allow users to pass custom reader,
    writer, and version kwargs for custom serialization. Otherwise,
    use current GraphSON version as default.
    """

    # KEEP TRACK OF CURRENT DEFAULTS
    DEFAULT_READER_CLASS = graphsonV3d0.GraphSONReader
    DEFAULT_WRITER_CLASS = graphsonV3d0.GraphSONWriter
    DEFAULT_VERSION = b"application/vnd.gremlin-v3.0+json"

    def __init__(self, reader=None, writer=None, version=None):
        if not version:
            version = self.DEFAULT_VERSION
        self._version = version
        if not reader:
            reader = self.DEFAULT_READER_CLASS()
        self._graphson_reader = reader
        if not writer:
            writer = self.DEFAULT_WRITER_CLASS()
        self.standard = Standard(writer)
        self.traversal = Traversal(writer)
        self.session = Session(writer)

    @property
    def version(self):
        """Read only property"""
        return self._version

    def get_processor(self, processor):
        processor = getattr(self, processor, None)
        if not processor:
            raise Exception("Unknown processor")
        return processor

    def serialize_message(self, request_id, request_message):
        processor = request_message.processor
        op = request_message.op
        args = request_message.args
        if not processor:
            processor_obj = self.get_processor('standard')
        else:
            processor_obj = self.get_processor(processor)
        args = processor_obj.get_op_args(op, args)
        message = self.build_message(request_id, processor, op, args)
        return message

    def build_message(self, request_id, processor, op, args):
        message = {
            'requestId': {'@type': 'g:UUID', '@value': request_id},
            'processor': processor,
            'op': op,
            'args': args
        }
        return self.finalize_message(message, b"\x21", self.version)

    def finalize_message(self, message, mime_len, mime_type):
        message = json.dumps(message)
        message = b''.join([mime_len, mime_type, message.encode('utf-8')])
        return message

    def deserialize_message(self, message):
        # for parsing string message via HTTP connections
        msg = json.loads(message if isinstance(message, str) else message.decode('utf-8'))
        return self._graphson_reader.to_object(msg)


class GraphSONSerializersV2d0(GraphSONMessageSerializer):
    """Message serializer for GraphSON 2.0"""
    def __init__(self):
        reader = graphsonV2d0.GraphSONReader()
        writer = graphsonV2d0.GraphSONWriter()
        version = b"application/vnd.gremlin-v2.0+json"
        super(GraphSONSerializersV2d0, self).__init__(reader, writer, version)


class GraphSONSerializersV3d0(GraphSONMessageSerializer):
    """Message serializer for GraphSON 3.0"""
    def __init__(self):
        reader = graphsonV3d0.GraphSONReader()
        writer = graphsonV3d0.GraphSONWriter()
        version = b"application/vnd.gremlin-v3.0+json"
        super(GraphSONSerializersV3d0, self).__init__(reader, writer, version)


class GraphBinarySerializersV1(object):
    DEFAULT_READER_CLASS = graphbinaryV1.GraphBinaryReader
    DEFAULT_WRITER_CLASS = graphbinaryV1.GraphBinaryWriter
    DEFAULT_VERSION = b"application/vnd.graphbinary-v1.0"

    max_int64 = 0xFFFFFFFFFFFFFFFF
    header_struct = struct.Struct('>b32sBQQ')
    header_pack = header_struct.pack
    int_pack = graphbinaryV1.int32_pack
    int32_unpack = struct.Struct(">i").unpack

    def __init__(self, reader=None, writer=None, version=None):
        if not version:
            version = self.DEFAULT_VERSION
        self._version = version
        if not reader:
            reader = self.DEFAULT_READER_CLASS()
        self._graphbinary_reader = reader
        if not writer:
            writer = self.DEFAULT_WRITER_CLASS()
        self._graphbinary_writer = writer
        self.standard = Standard(writer)
        self.traversal = Traversal(writer)
        self.session = Session(writer)

    @property
    def version(self):
        """Read only property"""
        return self._version

    def get_processor(self, processor):
        processor = getattr(self, processor, None)
        if not processor:
            raise Exception("Unknown processor")
        return processor

    def serialize_message(self, request_id, request_message):
        processor = request_message.processor
        op = request_message.op
        args = request_message.args
        if not processor:
            processor_obj = self.get_processor('standard')
        else:
            processor_obj = self.get_processor(processor)
        args = processor_obj.get_op_args(op, args)
        message = self.build_message(request_id, processor, op, args)
        return message

    def build_message(self, request_id, processor, op, args):
        message = {
            'requestId': request_id,
            'processor': processor,
            'op': op,
            'args': args
        }
        return self.finalize_message(message, 0x20, self.version)

    def finalize_message(self, message, mime_len, mime_type):
        ba = bytearray()

        request_id = uuid.UUID(str(message['requestId']))
        ba.extend(self.header_pack(mime_len, mime_type, 0x81,
                                   (request_id.int >> 64) & self.max_int64, request_id.int & self.max_int64))

        op_bytes = message['op'].encode("utf-8")
        ba.extend(self.int_pack(len(op_bytes)))
        ba.extend(op_bytes)

        processor_bytes = message['processor'].encode("utf-8")
        ba.extend(self.int_pack(len(processor_bytes)))
        ba.extend(processor_bytes)

        args = message["args"]
        ba.extend(self.int_pack(len(args)))
        for k, v in args.items():
            self._graphbinary_writer.to_dict(k, ba)

            # processor_obj.get_op_args in serialize_message() seems to already handle bytecode. in python 3
            # because bytearray isn't bound to a type in graphbinary it falls through the writeObject() and
            # just works but python 2 bytearray is bound to ByteBufferType so it writes DataType.bytebuffer
            # rather than DataType.bytecode and the server gets confused. special casing this for now until
            # it can be refactored
            if k == "gremlin" and isinstance(v, bytearray):
                ba.extend(v)
            else:
                self._graphbinary_writer.to_dict(v, ba)

        return bytes(ba)

    def deserialize_message(self, message):
        # for parsing string message via HTTP connections
        b = io.BytesIO(base64.b64decode(message) if isinstance(message, str) else message)

        b.read(1)  # version

        request_id = str(self._graphbinary_reader.to_object(b, graphbinaryV1.DataType.uuid))
        status_code = self.int32_unpack(b.read(4))[0]
        status_msg = self._graphbinary_reader.to_object(b, graphbinaryV1.DataType.string)
        status_attrs = self._graphbinary_reader.to_object(b, graphbinaryV1.DataType.map, nullable=False)
        meta_attrs = self._graphbinary_reader.to_object(b, graphbinaryV1.DataType.map, nullable=False)
        result = self._graphbinary_reader.to_object(b)

        b.close()

        msg = {'requestId': request_id,
               'status': {'code': status_code,
                          'message': status_msg,
                          'attributes': status_attrs},
               'result': {'meta': meta_attrs,
                          'data': result}}

        return msg
