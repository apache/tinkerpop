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
import io

from gremlin_python.process.traversal import Traverser

try:
    import ujson as json
    if int(json.__version__[0]) < 2:
        logging.warning("Detected ujson version below 2.0.0. This is not recommended as precision may be lost.")
except ImportError:
    import json

from gremlin_python.structure.io import graphbinaryV4, graphsonV4
from gremlin_python.structure.io.util import Marker

__author__ = 'David M. Brown (davebshow@gmail.com), Lyndon Bauto (lyndonb@bitquilltech.com)'

"""
GraphSONV4
"""

class GraphSONSerializersV4(object):
    """
    Message serializer for GraphSON. Allow users to pass custom reader,
    writer, and version kwargs for custom serialization. Otherwise,
    use current GraphSON version as default.
    """

    # KEEP TRACK OF CURRENT DEFAULTS
    DEFAULT_READER_CLASS = graphsonV4.GraphSONReader
    DEFAULT_WRITER_CLASS = graphsonV4.GraphSONWriter
    DEFAULT_VERSION = b"application/vnd.gremlin-v4.0+json"

    def __init__(self, reader=None, writer=None, version=None):
        if not version:
            version = self.DEFAULT_VERSION
        self._version = version
        if not reader:
            reader = self.DEFAULT_READER_CLASS()
        self._graphson_reader = reader
        if not writer:
            writer = self.DEFAULT_WRITER_CLASS()
        self._graphson_writer = writer

    @property
    def version(self):
        """Read only property"""
        return self._version

    def serialize_message(self, request_message):
        message = self.build_message(request_message.fields, request_message.gremlin)
        return message

    def build_message(self, fields, gremlin):
        message = {
            'gremlin': gremlin  # gremlin wil always be a gremlin lang string script
        }
        for k, v in fields.items():
            message[k] = self._graphson_writer.to_dict(v)
        return self.finalize_message(message)

    def finalize_message(self, message):
        message = json.dumps(message)
        return message

    def deserialize_message(self, message, is_first_chunk=False):
        if is_first_chunk:
            msg = json.loads(message if isinstance(message, str) else message.decode('utf-8'))
            return self._graphson_reader.to_object(msg)
        else:
            # graphSON does not stream, all results are aggregated inside the first chunk
            return ""

"""
GraphBinaryV4
"""
class GraphBinarySerializersV4(object):
    DEFAULT_READER_CLASS = graphbinaryV4.GraphBinaryReader
    DEFAULT_WRITER_CLASS = graphbinaryV4.GraphBinaryWriter
    DEFAULT_VERSION = b"application/vnd.graphbinary-v4.0"

    max_int64 = 0xFFFFFFFFFFFFFFFF
    header_struct = struct.Struct('>b32sBQQ')
    header_pack = header_struct.pack
    int_pack = graphbinaryV4.int32_pack
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
        self._bulked = False

    @property
    def version(self):
        """Read only property"""
        return self._version

    def serialize_message(self, request_message):
        message = self.build_message(request_message.fields, request_message.gremlin)
        return message

    def build_message(self, fields, gremlin):
        message = {
            'fields': fields,
            'gremlin': gremlin
        }
        return self.finalize_message(message)

    def finalize_message(self, message):
        ba = bytearray()

        ba.extend(graphbinaryV4.uint8_pack(0x81))
        fields = message["fields"]
        ba.extend(self.int_pack(len(fields)))
        for k, v in fields.items():
            self._graphbinary_writer.to_dict(k, ba)
            self._graphbinary_writer.to_dict(v, ba)

        gremlin = message['gremlin']
        # TODO: hack to remove type code from gremlin value for V4 message format, writer doesn't seem to have a way to
        #  write value directly by passing serializer types, check back when removing bytecode
        gremlin_ba = bytearray()
        self._graphbinary_writer.to_dict(gremlin, gremlin_ba)
        ba.extend(gremlin_ba[2:])

        return bytes(ba)

    def deserialize_message(self, message, is_first_chunk=False):
        if len(message) == 0:
            return {'status': {'code': 204},
                    'result': {'meta': {},
                               'data': []}}

        # for parsing string message via HTTP connections
        b = io.BytesIO(base64.b64decode(message) if isinstance(message, str) else message)

        if is_first_chunk:
            b.read(1)  # version
            self._bulked = b.read(1)[0] == 0x01

        result, readable = self.read_payload(b)
        if not readable:
            return {
                'result': {'meta': {},
                           'data': result}
            }
        status_code = self.int32_unpack(b.read(4))[0]  # status code
        status_msg = self._graphbinary_reader.to_object(b, graphbinaryV4.DataType.string, nullable=True)
        status_ex = self._graphbinary_reader.to_object(b, graphbinaryV4.DataType.string, nullable=True)
        # meta_attrs = self._graphbinary_reader.to_object(b, graphbinaryV4.DataType.map, nullable=False)

        b.close()

        msg = {'status': {'code': status_code,
                          'message': status_msg,
                          'exception': status_ex},
               'result': {'meta': {},
                          'data': result}}

        return msg

    def read_payload(self, buffer):
        results = []
        readable = True
        while buffer.readable():  # find method or way to access readable bytes without using buffer.getvalue()
            if buffer.tell() == len(buffer.getvalue()):
                readable = False
                break
            if self._bulked:
                item = self._graphbinary_reader.to_object(buffer)
                if item == Marker.end_of_stream():
                    self._bulked = False  # no more data expected, reset bulked flag
                    break
                bulk = self._graphbinary_reader.to_object(buffer)
                results.append(Traverser(item, bulk))
            else:
                data = self._graphbinary_reader.to_object(buffer)
                if data == Marker.end_of_stream():
                    break
                results.append(data)

        return results, readable
