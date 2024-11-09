﻿#region License

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

using System.Collections.Generic;
using System.Linq;

namespace Gremlin.Net.Process.Traversal.Strategy.Decoration
{
#pragma warning disable 1591
    public class VertexProgramStrategy : AbstractTraversalStrategy
    {
        private const string JavaFqcn = ComputerDecorationNamespace + nameof(VertexProgramStrategy);
        
        public VertexProgramStrategy() : base(JavaFqcn)
        {
        }

        public VertexProgramStrategy(string? graphComputer = null, int? workers = null, string? persist = null,
            string? result = null, ITraversal? vertices = null, ITraversal? edges = null,
            Dictionary<string, dynamic>? configuration = null)
            : this()
        {
            if (graphComputer != null)
                Configuration["graphComputer"] = graphComputer;
            if (workers != null)
                Configuration["workers"] = workers;
            if (persist != null)
                Configuration["persist"] = persist;
            if (result != null)
                Configuration["result"] = result;
            if (vertices != null)
                Configuration["vertices"] = vertices;
            if (edges != null)
                Configuration["edges"] = edges;
            configuration?.ToList().ForEach(x => Configuration[x.Key] = x.Value);
        }
    }
#pragma warning restore 1591
}