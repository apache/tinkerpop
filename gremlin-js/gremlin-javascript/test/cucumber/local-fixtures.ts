/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

/**
 * Local graph fixtures for Tiny Gremlin Cucumber tests.
 * Builds in-memory Graph instances that mirror the Java TinkerFactory graphs.
 */

import { Graph, Vertex, Edge, VertexProperty, Property } from '../../lib/structure/graph.js';

function makeVertex(id: any, label: string, ...props: any[]) {
  const v = new Vertex(id, label, []);
  for (let i = 0; i < props.length; i += 3) {
    // props: [vpId, key, value, ...]
    v.properties.push(new VertexProperty(props[i], props[i + 1], props[i + 2], []));
  }
  return v;
}

function makeEdge(id: any, outV: Vertex, label: string, inV: Vertex, ...props: any[]) {
  const e = new Edge(id, outV, label, inV, []);
  for (let i = 0; i < props.length; i += 2) {
    e.properties.push(new Property(props[i], props[i + 1]));
  }
  return e;
}

/**
 * Builds the "modern" toy graph matching TinkerFactory.generateModern().
 * Returns {graph, vertices, edges, vertexProperties} matching world.js cache format.
 */
export function buildModernGraph() {
  const graph = new Graph();

  const marko  = makeVertex(1,  'person',   0, 'name', 'marko',  1, 'age', 29);
  const vadas  = makeVertex(2,  'person',   2, 'name', 'vadas',  3, 'age', 27);
  const lop    = makeVertex(3,  'software', 4, 'name', 'lop',    5, 'lang', 'java');
  const josh   = makeVertex(4,  'person',   6, 'name', 'josh',   7, 'age', 32);
  const ripple = makeVertex(5,  'software', 8, 'name', 'ripple', 9, 'lang', 'java');
  const peter  = makeVertex(6,  'person',  10, 'name', 'peter', 11, 'age', 35);

  for (const v of [marko, vadas, lop, josh, ripple, peter]) {
    graph.vertices.set(v.id, v);
  }

  const e7  = makeEdge(7,  marko, 'knows',   vadas,  'weight', 0.5);
  const e8  = makeEdge(8,  marko, 'knows',   josh,   'weight', 1.0);
  const e9  = makeEdge(9,  marko, 'created', lop,    'weight', 0.4);
  const e10 = makeEdge(10, josh,  'created', ripple, 'weight', 1.0);
  const e11 = makeEdge(11, josh,  'created', lop,    'weight', 0.4);
  const e12 = makeEdge(12, peter, 'created', lop,    'weight', 0.2);

  for (const e of [e7, e8, e9, e10, e11, e12]) {
    graph.edges.set(e.id, e);
    graph._indexEdge(e);
  }

  // vertices map: name → Vertex (with properties stripped, matching world.js format)
  const vertices = new Map<string, any>();
  for (const v of graph.vertices.values()) {
    const nameVP = v.properties.find(p => (p as any).label === 'name');
    if (nameVP) vertices.set((nameVP as any).value, { ...v, properties: undefined });
  }

  // edges map: "outName-label->inName" → Edge
  const edges: Record<string, any> = {};
  for (const e of graph.edges.values()) {
    const outName = nameOf(e.outV);
    const inName  = nameOf(e.inV);
    const key = `${outName}-${e.label}->${inName}`;
    edges[key] = { ...e, properties: undefined };
  }

  // vertexProperties map: "vertexName-key->formattedValue" → VertexProperty
  const vertexProperties: Record<string, any> = {};
  for (const v of graph.vertices.values()) {
    const vName = nameOf(v);
    if (!vName) continue;
    for (const vp of v.properties as any[]) {
      const k = vp.label;
      let formattedVal = String(vp.value);
      if (k === 'weight') formattedVal = 'd[' + vp.value + '].d';
      else if (k === 'age' || k === 'since' || k === 'skill') formattedVal = 'd[' + vp.value + '].i';
      vertexProperties[`${vName}-${k}->${formattedVal}`] = vp;
    }
  }

  return { graph, vertices, edges, vertexProperties };
}

function nameOf(v: Vertex): string | null {
  const vp = (v.properties as any[])?.find?.((p: any) => p.label === 'name');
  return vp ? vp.value : null;
}

/**
 * Builds the "sink" (kitchen sink) graph matching TinkerFactory.generateKitchenSink().
 */
export function buildSinkGraph() {
  const graph = new Graph();

  const loop = makeVertex(1000, 'loops', 1000, 'name', 'loop');
  const a    = makeVertex(2000, 'message', 2000, 'name', 'a');
  const b    = makeVertex(2001, 'message', 2001, 'name', 'b');

  for (const v of [loop, a, b]) graph.vertices.set(v.id, v);

  const eSelf     = makeEdge(1001, loop, 'self', loop);
  const eLinkAB   = makeEdge(2002, a,    'link', b);
  const eLinkAA   = makeEdge(2003, a,    'link', a);

  for (const e of [eSelf, eLinkAB, eLinkAA]) {
    graph.edges.set(e.id, e);
    graph._indexEdge(e);
  }

  const vertices = new Map<string, any>();
  for (const v of graph.vertices.values()) {
    const nameVP = (v.properties as any[]).find((p: any) => p.label === 'name');
    if (nameVP) vertices.set(nameVP.value, { ...v, properties: undefined });
  }

  // Build edge map for sink graph
  const edges: Record<string, any> = {};
  for (const e of graph.edges.values()) {
    const outName = nameOf(e.outV);
    const inName  = nameOf(e.inV);
    const key = `${outName}-${e.label}->${inName}`;
    edges[key] = { ...e, properties: undefined };
  }

  return { graph, vertices, edges, vertexProperties: {} as Record<string, any> };
}

/**
 * Builds the "crew" graph matching TinkerFactory.generateTheCrew().
 */
export function buildCrewGraph() {
  const graph = new Graph();

  const marko      = makeVertex(1,  'person',   0,  'name', 'marko');
  const stephen    = makeVertex(7,  'person',   1,  'name', 'stephen');
  const matthias   = makeVertex(8,  'person',   2,  'name', 'matthias');
  const daniel     = makeVertex(9,  'person',   3,  'name', 'daniel');
  const gremlinV   = makeVertex(10, 'software', 4,  'name', 'gremlin');
  const tinkergraph = makeVertex(11, 'software', 5, 'name', 'tinkergraph');

  // location vertex properties (multi-valued — add as additional VPs)
  addLocationVPs(marko,     6,  ['san diego', 1997, 2001], ['santa cruz', 2001, 2004], ['brussels', 2004, 2005], ['santa fe', 2005, null]);
  addLocationVPs(stephen,   10, ['centreville', 1990, 2000], ['dulles', 2000, 2006], ['purcellville', 2006, null]);
  addLocationVPs(matthias,  13, ['bremen', 2004, 2007], ['baltimore', 2007, 2011], ['oakland', 2011, 2014], ['seattle', 2014, null]);
  addLocationVPs(daniel,    16, ['spremberg', 1982, 2005], ['kaiserslautern', 2005, 2009], ['aachen', 2009, null]);

  for (const v of [marko, stephen, matthias, daniel, gremlinV, tinkergraph]) {
    graph.vertices.set(v.id, v);
  }

  const edges_list = [
    makeEdge(13, marko,    'develops',  gremlinV,    'since', 2009),
    makeEdge(14, marko,    'develops',  tinkergraph, 'since', 2010),
    makeEdge(15, marko,    'uses',      gremlinV,    'skill', 4),
    makeEdge(16, marko,    'uses',      tinkergraph, 'skill', 5),
    makeEdge(17, stephen,  'develops',  gremlinV,    'since', 2010),
    makeEdge(18, stephen,  'develops',  tinkergraph, 'since', 2011),
    makeEdge(19, stephen,  'uses',      gremlinV,    'skill', 5),
    makeEdge(20, stephen,  'uses',      tinkergraph, 'skill', 4),
    makeEdge(21, matthias, 'develops',  gremlinV,    'since', 2012),
    makeEdge(22, matthias, 'uses',      gremlinV,    'skill', 3),
    makeEdge(23, matthias, 'uses',      tinkergraph, 'skill', 3),
    makeEdge(24, daniel,   'uses',      gremlinV,    'skill', 5),
    makeEdge(25, daniel,   'uses',      tinkergraph, 'skill', 3),
    makeEdge(26, gremlinV, 'traverses', tinkergraph),
  ];

  for (const e of edges_list) {
    graph.edges.set(e.id, e);
    graph._indexEdge(e);
  }

  const vertices = new Map<string, any>();
  for (const v of graph.vertices.values()) {
    const nameVP = (v.properties as any[]).find((p: any) => p.label === 'name');
    if (nameVP) vertices.set(nameVP.value, { ...v, properties: undefined });
  }

  const edges: Record<string, any> = {};
  for (const e of graph.edges.values()) {
    const outName = nameOf(e.outV);
    const inName  = nameOf(e.inV);
    const key = `${outName}-${e.label}->${inName}`;
    edges[key] = { ...e, properties: undefined };
  }

  return { graph, vertices, edges, vertexProperties: {} as Record<string, any> };
}

function addLocationVPs(v: Vertex, startId: number, ...locs: [string, number, number | null][]) {
  for (let i = 0; i < locs.length; i++) {
    const [city, start, end] = locs[i];
    const vpId = startId + i;
    const vp = new VertexProperty(vpId, 'location', city, []);
    (vp as any).properties = [
      new Property('startTime', start),
      ...(end != null ? [new Property('endTime', end)] : []),
    ];
    (v.properties as VertexProperty[]).push(vp);
  }
}

/**
 * Returns an empty graph matching the "empty" graph used in world.js.
 */
export function buildEmptyGraph() {
  return { graph: new Graph(), vertices: new Map<string, any>(), edges: {} as Record<string, any>, vertexProperties: {} as Record<string, any> };
}
