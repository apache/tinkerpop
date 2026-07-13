#region License

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#endregion

using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Gremlin.Net.Structure.IO.GraphBinary4.Types
{
    /// <summary>
    /// A <see cref="Graph"/> serializer for GraphBinary. The wire format is a count-prefixed
    /// list of vertices (each with their vertex properties and meta-properties), followed by a
    /// count-prefixed list of edges (each with their properties). Vertex/edge labels are written
    /// as a single-element list, parent placeholders are written as <c>null</c>.
    /// </summary>
    public class GraphSerializer : SimpleTypeSerializer<Graph>
    {
        /// <summary>
        ///     Initializes a new instance of the <see cref="GraphSerializer" /> class.
        /// </summary>
        public GraphSerializer() : base(DataType.Graph)
        {
        }

        /// <inheritdoc />
        protected override async Task WriteValueAsync(Graph value, Stream stream, GraphBinaryWriter writer,
            CancellationToken cancellationToken = default)
        {
            await writer.WriteNonNullableValueAsync(value.Vertices.Count, stream, cancellationToken)
                .ConfigureAwait(false);
            foreach (var vertex in value.Vertices.Values)
            {
                await WriteVertexAsync(vertex, stream, writer, cancellationToken).ConfigureAwait(false);
            }

            await writer.WriteNonNullableValueAsync(value.Edges.Count, stream, cancellationToken)
                .ConfigureAwait(false);
            foreach (var edge in value.Edges.Values)
            {
                await WriteEdgeAsync(edge, stream, writer, cancellationToken).ConfigureAwait(false);
            }
        }

        private static async Task WriteVertexAsync(Vertex vertex, Stream stream, GraphBinaryWriter writer,
            CancellationToken cancellationToken)
        {
            await writer.WriteAsync(vertex.Id, stream, cancellationToken).ConfigureAwait(false);
            await writer.WriteNonNullableValueAsync(new List<string>(vertex.Labels), stream, cancellationToken)
                .ConfigureAwait(false);

            var vertexProperties = AsList<VertexProperty>(vertex.Properties);
            await writer.WriteNonNullableValueAsync(vertexProperties.Count, stream, cancellationToken)
                .ConfigureAwait(false);
            foreach (var vp in vertexProperties)
            {
                await writer.WriteAsync(vp.Id, stream, cancellationToken).ConfigureAwait(false);
                await writer.WriteNonNullableValueAsync(new List<string> { vp.Label }, stream, cancellationToken)
                    .ConfigureAwait(false);
                await writer.WriteAsync((object?)vp.Value, stream, cancellationToken).ConfigureAwait(false);

                // placeholder for the parent vertex
                await writer.WriteAsync(null, stream, cancellationToken).ConfigureAwait(false);

                var metaProperties = AsList<Property>(vp.Properties);
                await writer.WriteNonNullableValueAsync(metaProperties, stream, cancellationToken)
                    .ConfigureAwait(false);
            }
        }

        private static async Task WriteEdgeAsync(Edge edge, Stream stream, GraphBinaryWriter writer,
            CancellationToken cancellationToken)
        {
            await writer.WriteAsync(edge.Id, stream, cancellationToken).ConfigureAwait(false);
            await writer.WriteNonNullableValueAsync(new List<string>(edge.Labels), stream, cancellationToken)
                .ConfigureAwait(false);

            await writer.WriteAsync(edge.InV.Id, stream, cancellationToken).ConfigureAwait(false);
            // placeholder for the in-vertex label (always null in this context)
            await writer.WriteAsync(null, stream, cancellationToken).ConfigureAwait(false);

            await writer.WriteAsync(edge.OutV.Id, stream, cancellationToken).ConfigureAwait(false);
            // placeholder for the out-vertex label (always null in this context)
            await writer.WriteAsync(null, stream, cancellationToken).ConfigureAwait(false);

            // placeholder for the parent (never present)
            await writer.WriteAsync(null, stream, cancellationToken).ConfigureAwait(false);

            var edgeProperties = AsList<Property>(edge.Properties);
            await writer.WriteNonNullableValueAsync(edgeProperties, stream, cancellationToken)
                .ConfigureAwait(false);
        }

        /// <inheritdoc />
        protected override async Task<Graph> ReadValueAsync(Stream stream, GraphBinaryReader reader,
            CancellationToken cancellationToken = default)
        {
            var graph = new Graph();

            var vertexCount =
                (int)await reader.ReadNonNullableValueAsync<int>(stream, cancellationToken).ConfigureAwait(false);
            for (var i = 0; i < vertexCount; i++)
            {
                var vertex = await ReadVertexAsync(stream, reader, cancellationToken).ConfigureAwait(false);
                if (vertex.Id != null)
                {
                    graph.Vertices[vertex.Id] = vertex;
                }
            }

            var edgeCount =
                (int)await reader.ReadNonNullableValueAsync<int>(stream, cancellationToken).ConfigureAwait(false);
            for (var i = 0; i < edgeCount; i++)
            {
                var edge = await ReadEdgeAsync(graph, stream, reader, cancellationToken).ConfigureAwait(false);
                if (edge.Id != null)
                {
                    graph.Edges[edge.Id] = edge;
                }
            }

            return graph;
        }

        private static async Task<Vertex> ReadVertexAsync(Stream stream, GraphBinaryReader reader,
            CancellationToken cancellationToken)
        {
            var vId = await reader.ReadAsync(stream, cancellationToken).ConfigureAwait(false);
            var vLabelList = (List<string?>)await reader
                .ReadNonNullableValueAsync<List<string?>>(stream, cancellationToken).ConfigureAwait(false);
            var vLabel = vLabelList.Count > 0 ? vLabelList[0] ?? "" : "";

            var vpCount = (int)await reader.ReadNonNullableValueAsync<int>(stream, cancellationToken)
                .ConfigureAwait(false);
            var vertexProperties = new List<VertexProperty>(vpCount);
            for (var j = 0; j < vpCount; j++)
            {
                var vpId = await reader.ReadAsync(stream, cancellationToken).ConfigureAwait(false);
                var vpLabelList = (List<string?>)await reader
                    .ReadNonNullableValueAsync<List<string?>>(stream, cancellationToken).ConfigureAwait(false);
                var vpLabel = vpLabelList.Count > 0 ? vpLabelList[0] ?? "" : "";
                var vpValue = await reader.ReadAsync(stream, cancellationToken).ConfigureAwait(false);

                // discard the parent vertex placeholder
                await reader.ReadAsync(stream, cancellationToken).ConfigureAwait(false);

                var metaProps = await reader.ReadNonNullableValueAsync<List<object?>>(stream, cancellationToken)
                    .ConfigureAwait(false);
                var metaPropsArray = (metaProps as List<object>)?.ToArray() ?? Array.Empty<object>();

                vertexProperties.Add(new VertexProperty(vpId, vpLabel, vpValue, null, metaPropsArray));
            }

            return new Vertex(vId, vLabel, vertexProperties.Cast<object>().ToArray());
        }

        private static async Task<Edge> ReadEdgeAsync(Graph graph, Stream stream, GraphBinaryReader reader,
            CancellationToken cancellationToken)
        {
            var eId = await reader.ReadAsync(stream, cancellationToken).ConfigureAwait(false);
            var eLabelList = (List<string?>)await reader
                .ReadNonNullableValueAsync<List<string?>>(stream, cancellationToken).ConfigureAwait(false);
            var eLabel = eLabelList.Count > 0 ? eLabelList[0] ?? "" : "";

            var inVId = await reader.ReadAsync(stream, cancellationToken).ConfigureAwait(false);
            // discard the in-vertex label placeholder (always null on the wire)
            await reader.ReadAsync(stream, cancellationToken).ConfigureAwait(false);
            var outVId = await reader.ReadAsync(stream, cancellationToken).ConfigureAwait(false);
            // discard the out-vertex label placeholder (always null on the wire)
            await reader.ReadAsync(stream, cancellationToken).ConfigureAwait(false);
            // discard the parent placeholder
            await reader.ReadAsync(stream, cancellationToken).ConfigureAwait(false);

            var edgeProps = await reader.ReadNonNullableValueAsync<List<object?>>(stream, cancellationToken)
                .ConfigureAwait(false);
            var edgePropsArray = (edgeProps as List<object>)?.ToArray() ?? Array.Empty<object>();

            var inVertex = ResolveVertex(graph, inVId);
            var outVertex = ResolveVertex(graph, outVId);

            return new Edge(eId, outVertex, eLabel, inVertex, edgePropsArray);
        }

        private static Vertex ResolveVertex(Graph graph, object? vertexId)
        {
            if (vertexId != null && graph.Vertices.TryGetValue(vertexId, out var existing))
            {
                return existing;
            }
            return new Vertex(vertexId, "");
        }

        private static List<T> AsList<T>(IEnumerable? source)
        {
            if (source == null)
            {
                return new List<T>();
            }
            return source.Cast<T>().ToList();
        }
    }
}
