#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

from gremlin_python.driver.connection import GremlinServerError
from gremlin_python.process.traversal import Traverser
from gremlin_python.structure.io import graphbinaryV4
from gremlin_python.structure.io.util import Marker

__author__ = 'David M. Brown (davebshow@gmail.com), Lyndon Bauto (lyndonb@bitquilltech.com)'

"""
GraphBinaryV4
"""
class GraphBinarySerializersV4(object):
    DEFAULT_READER_CLASS = graphbinaryV4.GraphBinaryReader
    DEFAULT_WRITER_CLASS = graphbinaryV4.GraphBinaryWriter
    DEFAULT_VERSION = b"application/vnd.graphbinary-v4.0"

    int_pack = graphbinaryV4.int32_pack

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

        ba.extend(graphbinaryV4.uint8_pack(0x84))
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

    def deserialize_response_stream(self, stream):
        """
        Yield each result item from a GraphBinary response stream as it is read
        off the wire, then raise ``GremlinServerError`` if the trailing status
        is not a success code. ``Connection._receive`` dispatches through this
        method so custom response serializers can replace GraphBinary parsing.
        """
        reader = self._graphbinary_reader

        # Response header: version byte + flags byte
        stream.read(1)
        flags = stream.read(1)[0]
        bulked = flags == 0x01

        while True:
            obj = reader.to_object(stream)
            if obj == Marker.end_of_stream():
                break
            if bulked:
                bulk = reader.to_object(stream)
                yield Traverser(obj, bulk)
            else:
                yield obj

        # Trailer: status code, message, exception
        status_code = graphbinaryV4.int32_unpack(stream.read(4))
        msg_is_null = stream.read(1)[0] == 0x01
        status_message = '' if msg_is_null else reader.to_object(
            stream, graphbinaryV4.DataType.string, False)
        exc_is_null = stream.read(1)[0] == 0x01
        status_exception = '' if exc_is_null else reader.to_object(
            stream, graphbinaryV4.DataType.string, False)

        if status_code not in (200, 204):
            raise GremlinServerError({
                'code': status_code,
                'message': status_message,
                'exception': status_exception
            })
