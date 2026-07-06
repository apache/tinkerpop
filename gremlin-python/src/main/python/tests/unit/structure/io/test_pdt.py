"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
"""

import pytest

from gremlin_python.structure.graph import CompositePDT, PDTRegistry, provider_defined
from gremlin_python.structure.graph import PrimitivePDT
from gremlin_python.structure.io.graphbinaryV4 import GraphBinaryWriter, GraphBinaryReader


class TestCompositePDT(object):
    graphbinary_writer = GraphBinaryWriter()
    graphbinary_reader = GraphBinaryReader()

    def test_empty_name_rejected(self):
        with pytest.raises(ValueError):
            CompositePDT("", {"x": 1})

    def test_none_name_rejected(self):
        with pytest.raises(ValueError):
            CompositePDT(None, {"x": 1})

    def test_non_string_key_rejected(self):
        with pytest.raises(TypeError):
            CompositePDT("com.example.Bad", {1: "value"})


class TestPDTRegistry(object):

    def test_hydrate_simple(self):
        registry = PDTRegistry()
        registry.register("com.example.Point", lambda fields: (fields["x"], fields["y"]))
        pdt = CompositePDT("com.example.Point", {"x": 1.0, "y": 2.0})
        result = registry.hydrate(pdt)
        assert result == (1.0, 2.0)

    def test_hydrate_no_adapter_returns_raw(self):
        registry = PDTRegistry()
        pdt = CompositePDT("com.example.Unknown", {"a": 1})
        result = registry.hydrate(pdt)
        assert result is pdt

    def test_hydrate_adapter_throws_falls_back(self):
        registry = PDTRegistry()
        registry.register("com.example.Bad", lambda fields: 1 / 0)
        pdt = CompositePDT("com.example.Bad", {"x": 1})
        result = registry.hydrate(pdt)
        assert result is pdt

    def test_hydrate_nested(self):
        from collections import namedtuple
        Inner = namedtuple("Inner", ["val"])
        Outer = namedtuple("Outer", ["child", "count"])
        registry = PDTRegistry()
        registry.register("com.example.Inner", lambda fields: Inner(fields["val"].upper()))
        registry.register("com.example.Outer", lambda fields: Outer(fields["child"], fields["count"]))
        inner = CompositePDT("com.example.Inner", {"val": "hello"})
        outer = CompositePDT("com.example.Outer", {"child": inner, "count": 42})
        result = registry.hydrate(outer)
        assert result == Outer(Inner("HELLO"), 42)

    def test_hydrate_non_pdt_passthrough(self):
        registry = PDTRegistry()
        assert registry.hydrate("plain string") == "plain string"
        assert registry.hydrate(42) == 42

    def test_dehydrate_simple(self):
        from collections import namedtuple
        Point = namedtuple("Point", ["x", "y"])
        registry = PDTRegistry()
        registry.register("com.example.Point",
                          deserialize_fn=lambda fields: Point(fields["x"], fields["y"]),
                          serialize_fn=lambda p: {"x": p.x, "y": p.y},
                          target_class=Point)
        adapter = registry.get_composite_adapter_by_class(Point)
        fields = adapter['serialize'](Point(1.0, 2.0))
        assert fields == {"x": 1.0, "y": 2.0}

    def test_dehydrate_no_adapter_returns_none(self):
        registry = PDTRegistry()
        assert registry.get_composite_adapter_by_class(str) is None

    def test_dehydrate_no_serialize_fn_returns_none(self):
        registry = PDTRegistry()
        registry.register("com.example.Thing", deserialize_fn=lambda fields: fields, target_class=dict)
        adapter = registry.get_composite_adapter_by_class(dict)
        assert adapter['serialize'] is None

    def test_hydrate_inner_registered_in_unregistered_outer(self):
        """A registered type ALWAYS hydrates even when nested inside an unregistered outer PDT."""
        from collections import namedtuple
        Inner = namedtuple("Inner", ["val"])
        registry = PDTRegistry()
        registry.register("com.example.Inner", lambda fields: Inner(fields["val"]))
        # "com.example.Outer" is intentionally NOT registered
        inner_pdt = CompositePDT("com.example.Inner", {"val": 42})
        outer_pdt = CompositePDT("com.example.Outer", {"child": inner_pdt, "count": 7})

        result = registry.hydrate(outer_pdt)

        # Outer stays raw CompositePDT (no adapter)
        assert isinstance(result, CompositePDT)
        assert result.name == "com.example.Outer"
        # Inner field MUST be hydrated to Inner(val=42)
        assert result.fields["child"] == Inner(val=42)
        # Non-PDT fields pass through unchanged
        assert result.fields["count"] == 7


class TestPDTRegistryBuild(object):

    def test_build_returns_registry_with_no_entry_points(self):
        registry = PDTRegistry.create()
        assert isinstance(registry, PDTRegistry)

    def test_build_loads_entry_point(self):
        from unittest.mock import patch, MagicMock

        mock_ep = MagicMock()
        mock_ep.name = "mock_adapter"
        mock_ep.load.return_value = lambda reg: reg.register("com.mock.Type", lambda fields: fields)

        with patch("importlib.metadata.entry_points") as mock_entry_points:
            import sys
            if sys.version_info >= (3, 10):
                mock_entry_points.return_value = [mock_ep]
            else:
                mock_entry_points.return_value = {'tinkerpop.pdt': [mock_ep]}

            registry = PDTRegistry.create()
            assert "com.mock.Type" in registry._composite_adapters_by_name

    def test_build_handles_failing_entry_point(self):
        from unittest.mock import patch, MagicMock

        mock_ep = MagicMock()
        mock_ep.name = "bad_adapter"
        mock_ep.load.side_effect = RuntimeError("boom")

        with patch("importlib.metadata.entry_points") as mock_entry_points:
            import sys
            if sys.version_info >= (3, 10):
                mock_entry_points.return_value = [mock_ep]
            else:
                mock_entry_points.return_value = {'tinkerpop.pdt': [mock_ep]}

            registry = PDTRegistry.create()
            assert isinstance(registry, PDTRegistry)
            assert len(registry._composite_adapters_by_name) == 0


class TestReaderAutoHydration(object):

    def test_reader_auto_hydrates_with_registry(self):
        registry = PDTRegistry()
        registry.register("com.example.Point", lambda fields: {"x": fields["x"], "y": fields["y"], "hydrated": True})
        writer = GraphBinaryWriter()
        reader = GraphBinaryReader(pdt_registry=registry)

        pdt = CompositePDT("com.example.Point", {"x": 1.0, "y": 2.0})
        result = reader.read_object(writer.write_object(pdt))
        assert result == {"x": 1.0, "y": 2.0, "hydrated": True}

    def test_reader_no_registry_returns_raw_pdt(self):
        writer = GraphBinaryWriter()
        reader = GraphBinaryReader()

        pdt = CompositePDT("com.example.Unregistered", {"x": 1.0, "y": 2.0})
        result = reader.read_object(writer.write_object(pdt))
        assert isinstance(result, CompositePDT)
        assert result == pdt


class TestProviderDefinedDecorator(object):

    def test_decorator_sets_metadata_with_name(self):
        @provider_defined(name="com.example.Point", included_fields=["x", "y"])
        class Point:
            pass

        assert Point._pdt_name == "com.example.Point"
        assert Point._pdt_included_fields == ["x", "y"]
        assert Point._pdt_excluded_fields is None

    def test_decorator_defaults_to_class_name(self):
        @provider_defined()
        class MyType:
            pass

        assert MyType._pdt_name == "MyType"
        assert MyType._pdt_included_fields is None
        assert MyType._pdt_excluded_fields is None

    def test_decorator_excluded_fields(self):
        @provider_defined(excluded_fields=["internal"])
        class Foo:
            pass

        assert Foo._pdt_excluded_fields == ["internal"]


class TestPdtRegistryWiring(object):

    def test_serializer_passes_registry_to_reader(self):
        pytest.importorskip("aiohttp")
        from gremlin_python.driver.serializer import GraphBinarySerializersV4
        registry = PDTRegistry()
        s = GraphBinarySerializersV4(pdt_registry=registry)
        assert s._graphbinary_reader.pdt_registry is registry

    def test_client_passes_registry_to_serializers(self):
        pytest.importorskip("aiohttp")
        from unittest.mock import patch
        from gremlin_python.driver.client import Client
        registry = PDTRegistry()
        with patch.object(Client, '_fill_pool'):
            c = Client("ws://localhost:8182/gremlin", "g", pdt_registry=registry)
            assert c._response_serializer._graphbinary_reader.pdt_registry is registry

    def test_driver_remote_connection_passes_registry(self):
        pytest.importorskip("aiohttp")
        from unittest.mock import patch
        from gremlin_python.driver.client import Client
        from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
        registry = PDTRegistry()
        with patch.object(Client, '_fill_pool'):
            drc = DriverRemoteConnection("ws://localhost:8182/gremlin", "g", pdt_registry=registry)
            assert drc._client._response_serializer._graphbinary_reader.pdt_registry is registry


class TestPrimitivePDT(object):

    def test_empty_name_rejected(self):
        with pytest.raises(ValueError):
            PrimitivePDT("", "123")

    def test_none_name_rejected(self):
        with pytest.raises(ValueError):
            PrimitivePDT(None, "123")

    def test_none_value_rejected(self):
        with pytest.raises(ValueError):
            PrimitivePDT("Uint32", None)

    def test_equality(self):
        a = PrimitivePDT("Uint32", "42")
        b = PrimitivePDT("Uint32", "42")
        assert a == b
        assert hash(a) == hash(b)

    def test_inequality(self):
        a = PrimitivePDT("Uint32", "42")
        b = PrimitivePDT("Uint32", "43")
        assert a != b

    def test_repr(self):
        pdt = PrimitivePDT("Uint32", "42")
        assert "Uint32" in repr(pdt)
        assert "42" in repr(pdt)


class TestPrimitivePDTGraphBinary(object):
    graphbinary_writer = GraphBinaryWriter()
    graphbinary_reader = GraphBinaryReader()

    def test_round_trip_simple(self):
        pdt = PrimitivePDT("Uint32", "42")
        ba = self.graphbinary_writer.write_object(pdt)
        result = self.graphbinary_reader.read_object(ba)
        assert isinstance(result, PrimitivePDT)
        assert result == pdt

    def test_round_trip_leading_zeros(self):
        """Opaque value: leading zeros must be preserved."""
        pdt = PrimitivePDT("Uint32", "007")
        ba = self.graphbinary_writer.write_object(pdt)
        result = self.graphbinary_reader.read_object(ba)
        assert result.value == "007"

    def test_round_trip_large_number(self):
        """Opaque value: large numbers preserved as string."""
        pdt = PrimitivePDT("BigNum", "99999999999999999999999999999")
        ba = self.graphbinary_writer.write_object(pdt)
        result = self.graphbinary_reader.read_object(ba)
        assert result.value == "99999999999999999999999999999"

    def test_round_trip_non_numeric(self):
        """Opaque value: non-numeric strings work."""
        pdt = PrimitivePDT("TinkerId", "abc-def-123")
        ba = self.graphbinary_writer.write_object(pdt)
        result = self.graphbinary_reader.read_object(ba)
        assert result.value == "abc-def-123"

    def test_round_trip_empty_value(self):
        """Edge case: empty string value."""
        pdt = PrimitivePDT("Empty", "")
        ba = self.graphbinary_writer.write_object(pdt)
        result = self.graphbinary_reader.read_object(ba)
        assert result.value == ""


class TestPrimitiveRegistryHydration(object):

    def test_hydrate_simple(self):
        registry = PDTRegistry()
        registry.register_primitive("Uint32", lambda v: int(v))
        pdt = PrimitivePDT("Uint32", "42")
        result = registry.hydrate_primitive(pdt)
        assert result == 42

    def test_hydrate_no_adapter_returns_raw(self):
        registry = PDTRegistry()
        pdt = PrimitivePDT("Unknown", "hello")
        result = registry.hydrate_primitive(pdt)
        assert result is pdt

    def test_hydrate_adapter_throws_falls_back(self):
        registry = PDTRegistry()
        registry.register_primitive("Bad", lambda v: 1 / 0)
        pdt = PrimitivePDT("Bad", "x")
        result = registry.hydrate_primitive(pdt)
        assert result is pdt

    def test_reader_auto_hydrates_primitive(self):
        registry = PDTRegistry()
        registry.register_primitive("Uint32", lambda v: int(v))
        writer = GraphBinaryWriter()
        reader = GraphBinaryReader(pdt_registry=registry)

        pdt = PrimitivePDT("Uint32", "42")
        result = reader.read_object(writer.write_object(pdt))
        assert result == 42

    def test_reader_no_registry_returns_raw(self):
        writer = GraphBinaryWriter()
        reader = GraphBinaryReader()

        pdt = PrimitivePDT("Uint32", "42")
        result = reader.read_object(writer.write_object(pdt))
        assert isinstance(result, PrimitivePDT)
        assert result == pdt


class TestPrimitiveNestedInComposite(object):

    def test_primitive_nested_in_composite_hydrates(self):
        """A PrimitivePDT nested as a field value in a composite PDT is hydrated."""
        registry = PDTRegistry()
        registry.register_primitive("Uint32", lambda v: int(v))
        registry.register("com.example.Wrapper", lambda fields: {"id": fields["id"], "count": fields["count"]})

        inner = PrimitivePDT("Uint32", "99")
        outer = CompositePDT("com.example.Wrapper", {"id": "abc", "count": inner})
        result = registry.hydrate(outer)
        assert result == {"id": "abc", "count": 99}

    def test_primitive_nested_in_unregistered_composite_hydrates(self):
        """Primitive nested inside an unregistered composite still hydrates."""
        registry = PDTRegistry()
        registry.register_primitive("Uint32", lambda v: int(v))

        inner = PrimitivePDT("Uint32", "7")
        outer = CompositePDT("com.example.Unregistered", {"val": inner})
        result = registry.hydrate(outer)
        assert isinstance(result, CompositePDT)
        assert result.fields["val"] == 7

    def test_graphbinary_primitive_nested_in_composite(self):
        """Round-trip a composite PDT containing a primitive PDT field via GraphBinary."""
        registry = PDTRegistry()
        registry.register_primitive("Uint32", lambda v: int(v))
        registry.register("com.example.Outer",
                          lambda fields: {"name": fields["name"], "count": fields["count"]})
        writer = GraphBinaryWriter()
        reader = GraphBinaryReader(pdt_registry=registry)

        inner = PrimitivePDT("Uint32", "5")
        outer = CompositePDT("com.example.Outer", {"name": "test", "count": inner})
        ba = writer.write_object(outer)
        result = reader.read_object(ba)
        assert result == {"name": "test", "count": 5}


class TestPrimitiveRegistryEntryPoints(object):

    def test_entry_points_can_register_primitives(self):
        """Verifies that the entry_points 'tinkerpop.pdt' mechanism works for primitives."""
        from unittest.mock import patch, MagicMock

        def register_primitives(registry):
            registry.register_primitive("Uint32", lambda v: int(v))

        mock_ep = MagicMock()
        mock_ep.name = "mock_primitive"
        mock_ep.load.return_value = register_primitives

        with patch("importlib.metadata.entry_points") as mock_entry_points:
            import sys
            if sys.version_info >= (3, 10):
                mock_entry_points.return_value = [mock_ep]
            else:
                mock_entry_points.return_value = {'tinkerpop.pdt': [mock_ep]}

            registry = PDTRegistry.create()
            pdt = PrimitivePDT("Uint32", "123")
            assert registry.hydrate_primitive(pdt) == 123
