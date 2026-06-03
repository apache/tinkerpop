// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

class Graph {
  @override
  String toString() => 'graph[]';
}

abstract class Element {
  final dynamic id;
  final String label;
  final List<Property> properties;

  const Element(this.id, this.label, [this.properties = const []]);

  @override
  bool operator ==(Object other) =>
      other is Element && id == other.id;

  @override
  int get hashCode => id.hashCode;
}

class Vertex extends Element {
  const Vertex(super.id, super.label, [super.properties]);

  @override
  String toString() => 'v[$id]';
}

class Edge extends Element {
  final Vertex outV;
  final Vertex inV;

  const Edge(super.id, this.outV, super.label, this.inV,
      [super.properties]);

  @override
  String toString() => 'e[$id][${outV.id}-$label->${inV.id}]';
}

class VertexProperty extends Element {
  final dynamic value;
  String get key => label;

  const VertexProperty(super.id, super.label, this.value,
      [super.properties]);

  @override
  String toString() => 'vp[$label->$value]';
}

class Property<T> {
  final String key;
  final T value;

  const Property(this.key, this.value);

  @override
  bool operator ==(Object other) =>
      other is Property && key == other.key && value == other.value;

  @override
  int get hashCode => Object.hash(key, value);

  @override
  String toString() => 'p[$key->$value]';
}

class Path {
  final List<List<String>> labels;
  final List<dynamic> objects;

  const Path(this.labels, this.objects);

  @override
  String toString() => 'path[${objects.join(', ')}]';

  @override
  bool operator ==(Object other) {
    if (other is! Path) return false;
    if (identical(this, other)) return true;
    if (objects.length != other.objects.length) return false;
    for (int i = 0; i < objects.length; i++) {
      if (objects[i] != other.objects[i]) return false;
    }
    if (labels.length != other.labels.length) return false;
    for (int i = 0; i < labels.length; i++) {
      final a = labels[i], b = other.labels[i];
      if (a.length != b.length) return false;
      for (int j = 0; j < a.length; j++) {
        if (a[j] != b[j]) return false;
      }
    }
    return true;
  }

  @override
  int get hashCode => Object.hashAll([...objects, ...labels]);
}
