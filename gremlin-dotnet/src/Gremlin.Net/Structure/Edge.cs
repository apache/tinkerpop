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

namespace Gremlin.Net.Structure
{
    /// <summary>
    ///     Represents an edge between to vertices.
    /// </summary>
    public class Edge : Element
    {
        /// <summary>
        ///     Initializes a new instance of the <see cref="Edge" /> class.
        /// </summary>
        /// <param name="id">The id of the edge.</param>
        /// <param name="outV">The outgoing/tail vertex of the edge.</param>
        /// <param name="label">The label of the edge.</param>
        /// <param name="inV">The incoming/head vertex of the edge.</param>
        public Edge(object id, Vertex outV, string label, Vertex inV)
            : base(id, label)
        {
            OutV = outV;
            InV = inV;
        }

        /// <summary>
        ///     Gets or sets the incoming/head vertex of this edge.
        /// </summary>
        public Vertex InV { get; set; }

        /// <summary>
        ///     Gets or sets the outgoing/tail vertex of this edge.
        /// </summary>
        public Vertex OutV { get; set; }

        /// <inheritdoc />
        public override string ToString()
        {
            return $"e[{Id}][{OutV.Id}-{Label}->{InV.Id}]";
        }
    }
}