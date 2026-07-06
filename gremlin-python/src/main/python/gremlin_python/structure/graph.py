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

__author__ = 'Marko A. Rodriguez (http://markorodriguez.com)'


class Graph(object):
    def __init__(self):
        self.vertices = {}
        self.edges = {}

    def __repr__(self):
        return "graph[vertices:" + str(len(self.vertices)) + " edges:" + str(len(self.edges)) + "]"


class Element(object):
    def __init__(self, id, label, properties=None):
        self.id = id
        self.label = label
        self.properties = [] if properties is None else properties

    def __getitem__(self, key):
        for p in self.properties:
            if p.key == key:
                return p.value
        raise KeyError(key)

    def __contains__(self, key):
        for p in self.properties:
            if p.key == key:
                return True
        return False

    def keys(self):
        return set(p.key for p in self.properties)

    def values(self, *property_keys):
        if len(property_keys) == 0:
            return [p.value for p in self.properties]
        else:
            return [p.value for p in self.properties if p.key in property_keys]

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.id == other.id

    def __hash__(self):
        return hash(self.id)


class Vertex(Element):
    def __init__(self, id, label="vertex", properties=None):
        Element.__init__(self, id, label, properties)

    def __repr__(self):
        return "v[" + str(self.id) + "]"


class Edge(Element):
    def __init__(self, id, outV, label, inV, properties=None):
        Element.__init__(self, id, label, properties)
        self.outV = outV
        self.inV = inV

    def __repr__(self):
        return "e[" + str(self.id) + "][" + str(self.outV.id) + "-" + self.label + "->" + str(self.inV.id) + "]"


class VertexProperty(Element):
    def __init__(self, id, label, value, vertex, properties=None):
        Element.__init__(self, id, label, properties)
        self.value = value
        self.key = self.label
        self.vertex = vertex

    def __repr__(self):
        return "vp[" + str(self.label) + "->" + str(self.value)[0:20] + "]"


class Property(object):
    def __init__(self, key, value, element):
        self.key = key
        self.value = value
        self.element = element

    def __repr__(self):
        return "p[" + str(self.key) + "->" + str(self.value)[0:20] + "]"

    def __eq__(self, other):
        return isinstance(other, self.__class__) and \
               self.key == other.key and \
               self.value == other.value and \
               self.element == other.element

    def __hash__(self):
        return hash(self.key) + hash(self.value)


class Path(object):
    def __init__(self, labels, objects):
        self.labels = labels
        self.objects = objects

    def __repr__(self):
        return "path[" + ", ".join(map(str, self.objects)) + "]"

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.objects == other.objects and self.labels == other.labels

    def __hash__(self):
        return hash(str(self.objects)) + hash(str(self.labels))

    def __getitem__(self, key):
        if isinstance(key, str):
            objects = []
            for i, labels in enumerate(self.labels):
                if key in labels:
                    objects.append(self.objects[i])
            if 0 == len(objects):
                raise KeyError("The step with label " + key + " does not exist")
            return objects if len(objects) > 1 else objects[0]
        elif isinstance(key, int):
            return self.objects[key]
        else:
            raise TypeError("The path access key must be either a string label or integer index")

    def __len__(self):
        return len(self.objects)


class CompositePDT(object):
    def __init__(self, name, fields):
        if not name:
            raise ValueError("name cannot be null or empty")
        self._name = name
        self._fields = dict(fields) if fields else {}
        if any(not isinstance(k, str) for k in self._fields):
            raise TypeError("CompositePDT field keys must be strings")

    @property
    def name(self):
        return self._name

    @property
    def fields(self):
        return self._fields

    def __eq__(self, other):
        return isinstance(other, CompositePDT) and self._name == other._name and self._fields == other._fields

    def __hash__(self):
        try:
            return hash((self._name, frozenset(self._fields.items())))
        except TypeError:
            return hash(self._name)

    def __repr__(self):
        return f"pdt[{self._name}]{self._fields}"


class PrimitivePDT(object):
    """An immutable primitive provider-defined type consisting of a name and an opaque string value."""

    def __init__(self, name, value):
        if not name:
            raise ValueError("name cannot be null or empty")
        if value is None:
            raise ValueError("value cannot be null")
        self._name = name
        self._value = value

    @property
    def name(self):
        return self._name

    @property
    def value(self):
        return self._value

    def __eq__(self, other):
        return isinstance(other, PrimitivePDT) and self._name == other._name and self._value == other._value

    def __hash__(self):
        return hash((self._name, self._value))

    def __repr__(self):
        return f"pdt[{self._name}]({self._value})"


class PDTRegistry(object):
    def __init__(self):
        self._composite_adapters_by_name = {}
        self._composite_adapters_by_class = {}
        self._primitive_adapters_by_name = {}
        self._primitive_adapters_by_class = {}

    def register(self, type_name, deserialize_fn, serialize_fn=None, target_class=None):
        self._composite_adapters_by_name[type_name] = {
            'deserialize': deserialize_fn,
            'serialize': serialize_fn,
            'target_class': target_class
        }
        if target_class is not None:
            self._composite_adapters_by_class[target_class] = {
                'type_name': type_name,
                'serialize': serialize_fn,
            }

    def register_primitive(self, type_name, from_value, to_value=None, target_class=None):
        """Register a primitive PDT adapter.

        Args:
            type_name: The PDT type name string.
            from_value: Callable(str) -> object for deserialization.
            to_value: Callable(object) -> str for serialization (optional).
            target_class: The Python class this adapter produces (optional).
        """
        self._primitive_adapters_by_name[type_name] = {
            'from_value': from_value,
            'to_value': to_value,
            'target_class': target_class
        }
        if target_class is not None:
            self._primitive_adapters_by_class[target_class] = {
                'type_name': type_name,
                'to_value': to_value,
            }

    @classmethod
    def create(cls):
        """Create a registry populated by entry_points discovery.

        Providers register adapters via pyproject.toml:
            [project.entry-points."tinkerpop.pdt"]
            my_types = "my_package:register_pdt_types"

        Each entry point should be a callable that accepts a registry and registers adapters.
        """
        import sys
        registry = cls()
        if sys.version_info >= (3, 10):
            from importlib.metadata import entry_points
            eps = entry_points(group='tinkerpop.pdt')
        else:
            from importlib.metadata import entry_points
            all_eps = entry_points()
            eps = all_eps.get('tinkerpop.pdt', [])

        for ep in eps:
            try:
                factory = ep.load()
                factory(registry)
            except Exception as e:
                import logging
                logging.getLogger(__name__).warning(
                    f"Failed to load PDT adapter from entry point '{ep.name}': {e}")
        return registry

    def hydrate(self, pdt):
        """Attempt to hydrate a CompositePDT. Returns typed object or raw PDT."""
        if not isinstance(pdt, CompositePDT):
            return pdt

        # Always recurse into fields to hydrate nested registered PDTs.
        changed = False
        hydrated_fields = {}
        for k, v in pdt.fields.items():
            if isinstance(v, CompositePDT):
                h = self.hydrate(v)
                if h is not v:
                    changed = True
                hydrated_fields[k] = h
            elif isinstance(v, PrimitivePDT):
                h = self.hydrate_primitive(v)
                if h is not v:
                    changed = True
                hydrated_fields[k] = h
            else:
                hydrated_fields[k] = v

        adapter = self._composite_adapters_by_name.get(pdt.name)
        if adapter is None:
            return CompositePDT(pdt.name, hydrated_fields) if changed else pdt
        try:
            return adapter['deserialize'](hydrated_fields)
        except Exception as e:
            import logging
            logging.getLogger(__name__).warning(f"PDT hydration failed for '{pdt.name}': {e}")
            return pdt

    def hydrate_primitive(self, pdt):
        """Attempt to hydrate a PrimitivePDT. Returns typed object or raw PDT."""
        if not isinstance(pdt, PrimitivePDT):
            return pdt
        adapter = self._primitive_adapters_by_name.get(pdt.name)
        if adapter is None:
            return pdt
        try:
            return adapter['from_value'](pdt.value)
        except Exception as e:
            import logging
            logging.getLogger(__name__).warning(f"Primitive PDT hydration failed for '{pdt.name}': {e}")
            return pdt

    def get_composite_adapter_by_class(self, cls):
        """Return (type_name, serialize_fn) tuple for the given class, or None."""
        return self._composite_adapters_by_class.get(cls)

    def get_primitive_adapter_by_class(self, cls):
        """Return adapter dict for the given class, or None."""
        return self._primitive_adapters_by_class.get(cls)


# Module-level registry of @provider_defined decorated classes keyed by PDT name.
_pdt_decorated_types = {}


def provider_defined(name=None, included_fields=None, excluded_fields=None):
    """Decorator that marks a class as a Provider Defined Type."""
    def decorator(cls):
        cls._pdt_name = name or cls.__name__
        cls._pdt_included_fields = included_fields
        cls._pdt_excluded_fields = excluded_fields
        _pdt_decorated_types[cls._pdt_name] = cls
        return cls
    return decorator
