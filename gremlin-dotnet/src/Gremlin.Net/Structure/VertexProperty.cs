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

using System.Linq;

namespace Gremlin.Net.Structure
{
    /// <summary>
    ///     A <see cref="VertexProperty" /> denotes a key/value pair associated with a <see cref="Vertex" />.
    /// </summary>
    public class VertexProperty : Element
    {
        /// <summary>
        ///     Initializes a new instance of the <see cref="VertexProperty" /> class.
        /// </summary>
        /// <param name="id">The id of the vertex property.</param>
        /// <param name="label">The label of the vertex property.</param>
        /// <param name="value">The id of the vertex property.</param>
        /// <param name="vertex">The (optional) <see cref="Vertex" /> that owns this <see cref="VertexProperty" />.</param>
        /// <param name="properties">Optional properties of the VertexProperty.</param>
        public VertexProperty(object? id, string label, dynamic? value, Vertex? vertex = null, dynamic[]? properties = null)
            : base(id, label, properties)
        {
            Value = value;
            Vertex = vertex;
        }

        /// <summary>
        ///     The value of this <see cref="VertexProperty" />.
        /// </summary>
        public dynamic? Value { get; }

        /// <summary>
        ///     The <see cref="Vertex" /> that owns this <see cref="VertexProperty" />.
        /// </summary>
        public Vertex? Vertex { get; }

        /// <summary>
        ///     The key of this <see cref="VertexProperty" />.
        /// </summary>
        public string Key => Label;

        /// <summary>
        /// Get property by key
        /// </summary>
        /// <returns>property or null when not found</returns>
        public Property? Property(string key)
        {
            return Properties?.Cast<Property>().FirstOrDefault(p => p.Key == key);
        }

        /// <inheritdoc />
        public override string ToString()
        {
            return $"vp[{Label}->{Value}]";
        }
    }
}