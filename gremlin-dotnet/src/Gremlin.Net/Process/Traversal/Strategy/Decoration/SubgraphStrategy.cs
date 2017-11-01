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

namespace Gremlin.Net.Process.Traversal.Strategy.Decoration
{
    /// <summary>
    ///     Provides a way to limit the view of a <see cref="ITraversal" />.
    /// </summary>
    public class SubgraphStrategy : AbstractTraversalStrategy
    {
        /// <summary>
        ///     Initializes a new instance of the <see cref="SubgraphStrategy" /> class.
        /// </summary>
        public SubgraphStrategy()
        {
        }

        /// <summary>
        ///     Initializes a new instance of the <see cref="SubgraphStrategy" /> class.
        /// </summary>
        /// <param name="vertexCriterion">Constrains vertices for the <see cref="ITraversal" />.</param>
        /// <param name="edgeCriterion">Constrains edges for the <see cref="ITraversal" />.</param>
        /// <param name="vertexPropertyCriterion">Constrains vertex properties for the <see cref="ITraversal" />.</param>
        public SubgraphStrategy(ITraversal vertexCriterion = null, ITraversal edgeCriterion = null,
            ITraversal vertexPropertyCriterion = null)
        {
            if (vertexCriterion != null)
                Configuration["vertices"] = vertexCriterion;
            if (edgeCriterion != null)
                Configuration["edges"] = edgeCriterion;
            if (vertexPropertyCriterion != null)
                Configuration["vertexProperties"] = vertexPropertyCriterion;
        }
    }
}