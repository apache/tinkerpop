/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

import { Buffer } from 'buffer';
import { Graph, Vertex, Edge, VertexProperty } from '../../../graph.js';

export default class GraphSerializer {
  constructor(ioc) {
    this.ioc = ioc;
    this.ioc.serializers[ioc.DataType.GRAPH] = this;
  }

  canBeUsedFor(value) {
    return value instanceof Graph;
  }

  serialize(item, fullyQualifiedFormat = true) {
    if (item === undefined || item === null) {
      if (fullyQualifiedFormat) {
        return Buffer.from([this.ioc.DataType.GRAPH, 0x01]);
      }
      // value-only null fallback: zero vertices + zero edges
      const zeroInt = [0x00, 0x00, 0x00, 0x00];
      return Buffer.from([...zeroInt, ...zeroInt]);
    }

    const bufs = [];
    if (fullyQualifiedFormat) {
      bufs.push(Buffer.from([this.ioc.DataType.GRAPH, 0x00]));
    }

    const vertices = item.vertices ? Array.from(item.vertices.values()) : [];
    const edges = item.edges ? Array.from(item.edges.values()) : [];

    // {vertex_count}
    bufs.push(this.ioc.intSerializer.serialize(vertices.length, false));

    // vertices
    for (const v of vertices) {
      // {id}
      bufs.push(this.ioc.anySerializer.serialize(v.id));

      // {label} as 1-element list (value-only)
      bufs.push(this.ioc.listSerializer.serialize([v.label], false));

      const vps = Array.isArray(v.properties) ? v.properties : [];

      // {vp_count}
      bufs.push(this.ioc.intSerializer.serialize(vps.length, false));

      for (const vp of vps) {
        // {vp_id}
        bufs.push(this.ioc.anySerializer.serialize(vp.id));

        // {vp_label} as 1-element list (value-only)
        bufs.push(this.ioc.listSerializer.serialize([vp.label], false));

        // {vp_value}
        bufs.push(this.ioc.anySerializer.serialize(vp.value));

        // {parent} (always null)
        bufs.push(this.ioc.unspecifiedNullSerializer.serialize(null));

        // {meta_props} as value-only List<Property>
        const metaProps = Array.isArray(vp.properties) ? vp.properties : [];
        bufs.push(this.ioc.listSerializer.serialize(metaProps, false));
      }
    }

    // {edge_count}
    bufs.push(this.ioc.intSerializer.serialize(edges.length, false));

    // edges
    for (const e of edges) {
      // {id}
      bufs.push(this.ioc.anySerializer.serialize(e.id));

      // {label} as 1-element list (value-only)
      bufs.push(this.ioc.listSerializer.serialize([e.label], false));

      // {inV_id}
      bufs.push(this.ioc.anySerializer.serialize(e.inV && e.inV.id));

      // {inV_label} (always null placeholder, fully-qualified)
      bufs.push(this.ioc.unspecifiedNullSerializer.serialize(null));

      // {outV_id}
      bufs.push(this.ioc.anySerializer.serialize(e.outV && e.outV.id));

      // {outV_label} (always null placeholder, fully-qualified)
      bufs.push(this.ioc.unspecifiedNullSerializer.serialize(null));

      // {parent} (always null)
      bufs.push(this.ioc.unspecifiedNullSerializer.serialize(null));

      // {edge_props} as value-only List<Property>
      const props = Array.isArray(e.properties) ? e.properties : [];
      bufs.push(this.ioc.listSerializer.serialize(props, false));
    }

    return Buffer.concat(bufs);
  }

  /**
   * Async deserialization of graph value bytes from a StreamReader.
   * @param {StreamReader} reader
   * @param {number} valueFlag
   * @param {number} typeCode
   * @returns {Promise<Graph>}
   */
  async deserializeValue(reader, valueFlag, typeCode) {
    const graph = new Graph();

    // {vertex_count} bare int
    const vertexCount = await this.ioc.intSerializer.deserializeBare(reader);
    for (let i = 0; i < vertexCount; i++) {
      // {id} fully qualified
      const vId = await this.ioc.anySerializer.deserialize(reader);

      // {label} value-only list, first element
      const vLabelList = await this.ioc.listSerializer.deserializeValue(reader, 0x00, this.ioc.DataType.LIST);
      const vLabel = Array.isArray(vLabelList) && vLabelList.length > 0 ? vLabelList[0] : vLabelList;

      const vertex = new Vertex(vId, vLabel, []);
      graph.vertices.set(vId, vertex);

      // {vp_count} bare int
      const vpCount = await this.ioc.intSerializer.deserializeBare(reader);
      for (let j = 0; j < vpCount; j++) {
        // {vp_id} fully qualified
        const vpId = await this.ioc.anySerializer.deserialize(reader);

        // {vp_label} value-only list, first element
        const vpLabelList = await this.ioc.listSerializer.deserializeValue(reader, 0x00, this.ioc.DataType.LIST);
        const vpLabel = Array.isArray(vpLabelList) && vpLabelList.length > 0 ? vpLabelList[0] : vpLabelList;

        // {vp_value} fully qualified
        const vpValue = await this.ioc.anySerializer.deserialize(reader);

        // {parent} fully qualified (always null)
        await this.ioc.anySerializer.deserialize(reader);

        // {meta_props} value-only list
        const metaProps = await this.ioc.listSerializer.deserializeValue(reader, 0x00, this.ioc.DataType.LIST);

        const vp = new VertexProperty(vpId, vpLabel, vpValue, metaProps || []);
        vertex.properties.push(vp);
      }
    }

    // {edge_count} bare int
    const edgeCount = await this.ioc.intSerializer.deserializeBare(reader);
    for (let i = 0; i < edgeCount; i++) {
      // {id} fully qualified
      const eId = await this.ioc.anySerializer.deserialize(reader);

      // {label} value-only list, first element
      const eLabelList = await this.ioc.listSerializer.deserializeValue(reader, 0x00, this.ioc.DataType.LIST);
      const eLabel = Array.isArray(eLabelList) && eLabelList.length > 0 ? eLabelList[0] : eLabelList;

      // {inV_id} fully qualified
      const inVId = await this.ioc.anySerializer.deserialize(reader);

      // {inV_label} fully qualified (always null placeholder) — discard
      await this.ioc.anySerializer.deserialize(reader);

      // {outV_id} fully qualified
      const outVId = await this.ioc.anySerializer.deserialize(reader);

      // {outV_label} fully qualified (always null placeholder) — discard
      await this.ioc.anySerializer.deserialize(reader);

      // {parent} fully qualified (always null) — discard
      await this.ioc.anySerializer.deserialize(reader);

      // {edge_props} value-only list
      const edgeProps = await this.ioc.listSerializer.deserializeValue(reader, 0x00, this.ioc.DataType.LIST);

      // Reuse vertex instances already in graph.vertices, otherwise build stand-ins
      const inV = graph.vertices.get(inVId) || new Vertex(inVId, '', []);
      const outV = graph.vertices.get(outVId) || new Vertex(outVId, '', []);

      const edge = new Edge(eId, outV, eLabel, inV, edgeProps || []);
      graph.edges.set(eId, edge);
      graph._indexEdge(edge);
    }

    return graph;
  }

  /**
   * Async fully-qualified deserialization from a StreamReader.
   * @param {StreamReader} reader
   * @returns {Promise<Graph|null>}
   */
  async deserialize(reader) {
    const type_code = await reader.readUInt8();
    if (type_code !== this.ioc.DataType.GRAPH) {
      throw new Error(`GraphSerializer: unexpected {type_code}=0x${type_code.toString(16)}`);
    }
    const value_flag = await reader.readUInt8();
    if (value_flag === 0x01) {
      return null;
    }
    if (value_flag !== 0x00) {
      throw new Error(`GraphSerializer: unexpected {value_flag}=0x${value_flag.toString(16)}`);
    }
    return this.deserializeValue(reader, value_flag, type_code);
  }
}
