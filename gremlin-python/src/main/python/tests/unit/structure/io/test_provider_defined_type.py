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

from gremlin_python.structure.graph import ProviderDefinedType, ProviderDefinedTypeRegistry, provider_defined
from gremlin_python.structure.io.graphbinaryV4 import GraphBinaryWriter, GraphBinaryReader


class TestProviderDefinedType(object):
    graphbinary_writer = GraphBinaryWriter()
    graphbinary_reader = GraphBinaryReader()

    def test_empty_name_rejected(self):
        with pytest.raises(ValueError):
            ProviderDefinedType("", {"x": 1})

    def test_none_name_rejected(self):
        with pytest.raises(ValueError):
            ProviderDefinedType(None, {"x": 1})

    def test_non_string_key_rejected(self):
        with pytest.raises(TypeError):
            ProviderDefinedType("com.example.Bad", {1: "value"})


class TestProviderDefinedTypeRegistry(object):

    def test_hydrate_simple(self):
        registry = ProviderDefinedTypeRegistry()
        registry.register("com.example.Point", lambda fields: (fields["x"], fields["y"]))
        pdt = ProviderDefinedType("com.example.Point", {"x": 1.0, "y": 2.0})
        result = registry.hydrate(pdt)
        assert result == (1.0, 2.0)

    def test_hydrate_no_adapter_returns_raw(self):
        registry = ProviderDefinedTypeRegistry()
        pdt = ProviderDefinedType("com.example.Unknown", {"a": 1})
        result = registry.hydrate(pdt)
        assert result is pdt

    def test_hydrate_adapter_throws_falls_back(self):
        registry = ProviderDefinedTypeRegistry()
        registry.register("com.example.Bad", lambda fields: 1 / 0)
        pdt = ProviderDefinedType("com.example.Bad", {"x": 1})
        result = registry.hydrate(pdt)
        assert result is pdt

    def test_hydrate_nested(self):
        from collections import namedtuple
        Inner = namedtuple("Inner", ["val"])
        Outer = namedtuple("Outer", ["child", "count"])
        registry = ProviderDefinedTypeRegistry()
        registry.register("com.example.Inner", lambda fields: Inner(fields["val"].upper()))
        registry.register("com.example.Outer", lambda fields: Outer(fields["child"], fields["count"]))
        inner = ProviderDefinedType("com.example.Inner", {"val": "hello"})
        outer = ProviderDefinedType("com.example.Outer", {"child": inner, "count": 42})
        result = registry.hydrate(outer)
        assert result == Outer(Inner("HELLO"), 42)

    def test_hydrate_non_pdt_passthrough(self):
        registry = ProviderDefinedTypeRegistry()
        assert registry.hydrate("plain string") == "plain string"
        assert registry.hydrate(42) == 42

    def test_dehydrate_simple(self):
        from collections import namedtuple
        Point = namedtuple("Point", ["x", "y"])
        registry = ProviderDefinedTypeRegistry()
        registry.register("com.example.Point",
                          deserialize_fn=lambda fields: Point(fields["x"], fields["y"]),
                          serialize_fn=lambda p: {"x": p.x, "y": p.y},
                          target_class=Point)
        adapter = registry.get_adapter_by_class(Point)
        fields = adapter['serialize'](Point(1.0, 2.0))
        assert fields == {"x": 1.0, "y": 2.0}

    def test_dehydrate_no_adapter_returns_none(self):
        registry = ProviderDefinedTypeRegistry()
        assert registry.get_adapter_by_class(str) is None

    def test_dehydrate_no_serialize_fn_returns_none(self):
        registry = ProviderDefinedTypeRegistry()
        registry.register("com.example.Thing", deserialize_fn=lambda fields: fields, target_class=dict)
        adapter = registry.get_adapter_by_class(dict)
        assert adapter['serialize'] is None

    def test_hydrate_inner_registered_in_unregistered_outer(self):
        """A registered type ALWAYS hydrates even when nested inside an unregistered outer PDT."""
        from collections import namedtuple
        Inner = namedtuple("Inner", ["val"])
        registry = ProviderDefinedTypeRegistry()
        registry.register("com.example.Inner", lambda fields: Inner(fields["val"]))
        # "com.example.Outer" is intentionally NOT registered
        inner_pdt = ProviderDefinedType("com.example.Inner", {"val": 42})
        outer_pdt = ProviderDefinedType("com.example.Outer", {"child": inner_pdt, "count": 7})

        result = registry.hydrate(outer_pdt)

        # Outer stays raw ProviderDefinedType (no adapter)
        assert isinstance(result, ProviderDefinedType)
        assert result.name == "com.example.Outer"
        # Inner field MUST be hydrated to Inner(val=42)
        assert result.fields["child"] == Inner(val=42)
        # Non-PDT fields pass through unchanged
        assert result.fields["count"] == 7


class TestProviderDefinedTypeRegistryBuild(object):

    def test_build_returns_registry_with_no_entry_points(self):
        registry = ProviderDefinedTypeRegistry.create()
        assert isinstance(registry, ProviderDefinedTypeRegistry)

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

            registry = ProviderDefinedTypeRegistry.create()
            assert "com.mock.Type" in registry._adapters_by_name

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

            registry = ProviderDefinedTypeRegistry.create()
            assert isinstance(registry, ProviderDefinedTypeRegistry)
            assert len(registry._adapters_by_name) == 0


class TestReaderAutoHydration(object):

    def test_reader_auto_hydrates_with_registry(self):
        registry = ProviderDefinedTypeRegistry()
        registry.register("com.example.Point", lambda fields: {"x": fields["x"], "y": fields["y"], "hydrated": True})
        writer = GraphBinaryWriter()
        reader = GraphBinaryReader(pdt_registry=registry)

        pdt = ProviderDefinedType("com.example.Point", {"x": 1.0, "y": 2.0})
        result = reader.read_object(writer.write_object(pdt))
        assert result == {"x": 1.0, "y": 2.0, "hydrated": True}

    def test_reader_no_registry_returns_raw_pdt(self):
        writer = GraphBinaryWriter()
        reader = GraphBinaryReader()

        pdt = ProviderDefinedType("com.example.Unregistered", {"x": 1.0, "y": 2.0})
        result = reader.read_object(writer.write_object(pdt))
        assert isinstance(result, ProviderDefinedType)
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
        registry = ProviderDefinedTypeRegistry()
        s = GraphBinarySerializersV4(pdt_registry=registry)
        assert s._graphbinary_reader.pdt_registry is registry

    def test_client_passes_registry_to_serializers(self):
        pytest.importorskip("aiohttp")
        from unittest.mock import patch
        from gremlin_python.driver.client import Client
        registry = ProviderDefinedTypeRegistry()
        with patch.object(Client, '_fill_pool'):
            c = Client("ws://localhost:8182/gremlin", "g", pdt_registry=registry)
            assert c._response_serializer._graphbinary_reader.pdt_registry is registry

    def test_driver_remote_connection_passes_registry(self):
        pytest.importorskip("aiohttp")
        from unittest.mock import patch
        from gremlin_python.driver.client import Client
        from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
        registry = ProviderDefinedTypeRegistry()
        with patch.object(Client, '_fill_pool'):
            drc = DriverRemoteConnection("ws://localhost:8182/gremlin", "g", pdt_registry=registry)
            assert drc._client._response_serializer._graphbinary_reader.pdt_registry is registry
