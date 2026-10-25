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

"""GraphBinarySerializersV4: PDT-registry merge and finalize_message byte layout."""

from gremlin_python.driver.request import RequestMessage
from gremlin_python.driver.serializer import GraphBinarySerializersV4
from gremlin_python.structure.graph import PDTRegistry


class TestConfigurePdtRegistryMerge:
    """
    configure_pdt_registry sets the reader's registry on first call, but on
    subsequent calls it MERGES the four adapter maps into the existing registry
    rather than replacing it.
    """

    def test_second_registry_merges_all_adapter_maps(self):
        s = GraphBinarySerializersV4()

        first = PDTRegistry()
        first.register("com.example.Alpha", lambda fields: fields)
        first.register_primitive("Uint32", lambda v: int(v))

        second = PDTRegistry()
        second.register("com.example.Beta", lambda fields: fields)
        second.register_primitive("Uint64", lambda v: int(v))

        s.configure_pdt_registry(first)
        # First call installs the registry directly.
        assert s._graphbinary_reader.pdt_registry is first

        s.configure_pdt_registry(second)
        merged = s._graphbinary_reader.pdt_registry

        # Still the first registry object (merged into, not replaced).
        assert merged is first

        # Composite adapters from BOTH registries are present.
        assert "com.example.Alpha" in merged._composite_adapters_by_name
        assert "com.example.Beta" in merged._composite_adapters_by_name

        # Primitive adapters from BOTH registries are present.
        assert "Uint32" in merged._primitive_adapters_by_name
        assert "Uint64" in merged._primitive_adapters_by_name

    def test_merge_covers_by_class_maps(self):
        s = GraphBinarySerializersV4()

        first = PDTRegistry()
        first.register("com.example.Alpha",
                       deserialize_fn=lambda fields: fields,
                       serialize_fn=lambda o: {}, target_class=list)

        second = PDTRegistry()
        second.register("com.example.Beta",
                        deserialize_fn=lambda fields: fields,
                        serialize_fn=lambda o: {}, target_class=set)

        s.configure_pdt_registry(first)
        s.configure_pdt_registry(second)
        merged = s._graphbinary_reader.pdt_registry

        # by_class maps merged from both registries.
        assert merged.get_composite_adapter_by_class(list) is not None
        assert merged.get_composite_adapter_by_class(set) is not None


class TestFinalizeMessageBytes:
    """
    serialize_message -> build_message -> finalize_message produces the canonical
    GraphBinary V4 request byte layout:
      - 0x84 header byte
      - int32 field count
      - each field key/value as fully-qualified GraphBinary values
      - gremlin string value with its 2-byte (type-code + null-flag) prefix stripped
    """

    def test_empty_fields_canonical_bytes(self):
        s = GraphBinarySerializersV4()
        result = s.serialize_message(RequestMessage(fields={}, gremlin='g.V()'))
        # 0x84 | int32(0) field count | int32(5) string length | b'g.V()'
        assert result == b'\x84\x00\x00\x00\x00\x00\x00\x00\x05g.V()'

    def test_non_empty_fields_canonical_bytes(self):
        s = GraphBinarySerializersV4()
        result = s.serialize_message(RequestMessage(fields={'k': 'v'}, gremlin='g.V()'))
        # 0x84
        # | int32(1) field count
        # | key 'k'  : string type-code 0x03 + null-flag 0x00 + int32(1) + b'k'
        # | value 'v': string type-code 0x03 + null-flag 0x00 + int32(1) + b'v'
        # | gremlin  : int32(5) + b'g.V()'  (type-code + null-flag stripped)
        assert result == (
            b'\x84'
            b'\x00\x00\x00\x01'
            b'\x03\x00\x00\x00\x00\x01k'
            b'\x03\x00\x00\x00\x00\x01v'
            b'\x00\x00\x00\x05g.V()'
        )
