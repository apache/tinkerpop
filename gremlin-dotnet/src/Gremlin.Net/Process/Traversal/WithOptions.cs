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

using Gremlin.Net.Structure;

namespace Gremlin.Net.Process.Traversal.Step.Util
{
    /// <summary>
    ///     Configuration options to be passed to the <c>With()</c> modulator.
    /// </summary>
    public class WithOptions
    {
        #region PropertyMap

        /// <summary>
        ///     Configures the tokens to be included in value maps.
        /// </summary>
        public static readonly string Tokens = "~tinkerpop.valueMap.tokens";

        /// <summary>
        ///     Include no tokens.
        /// </summary>
        public static readonly int None = 0;

        /// <summary>
        ///     Include ids (affects all <see cref="Element" /> value maps).
        /// </summary>
        public static readonly int Ids = 1;

        /// <summary>
        ///     Include labels (affects all <see cref="Vertex" /> and <see cref="Edge" /> value maps).
        /// </summary>
        public static readonly int Labels = 2;

        /// <summary>
        ///     Include keys (affects all <see cref="VertexProperty" /> value maps).
        /// </summary>
        public static readonly int Keys = 4;

        /// <summary>
        ///     Include values (affects all <see cref="VertexProperty" /> value maps).
        /// </summary>
        public static readonly int Values = 8;

        /// <summary>
        ///     Include all tokens;
        /// </summary>
        public static readonly int All = 15;

        #endregion

        #region Index

        /// <summary>
        ///     Configures the indexer to be used in <see cref="GraphTraversal{S,E}.Index{E2}" />.
        /// </summary>
        public static readonly string Indexer = "~tinkerpop.index.indexer";

        /// <summary>
        ///     Index items using 2-item lists.
        /// </summary>
        public static readonly int List = 0;

        /// <summary>
        ///     Index items using a map.
        /// </summary>
        public static readonly int Map = 1;

        #endregion
    }
}
