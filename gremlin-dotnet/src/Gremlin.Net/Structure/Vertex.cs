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
    ///     Represents a vertex.
    /// </summary>
    public class Vertex : Element
    {
        /// <summary>
        ///     The default label to use for a vertex.
        /// </summary>
        public const string DefaultLabel = "vertex";

        /// <summary>
        ///     Initializes a new instance of the <see cref="Vertex" /> class.
        /// </summary>
        /// <param name="id">The id of the vertex.</param>
        /// <param name="label">The label of the vertex.</param>
        public Vertex(object id, string label = DefaultLabel)
            : base(id, label)
        {
        }

        /// <inheritdoc />
        public override string ToString()
        {
            return $"v[{Id}]";
        }
    }
}