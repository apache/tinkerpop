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

using System.Collections.Generic;
using System.Linq;
using Gremlin.Net.Process.Traversal;

namespace Gremlin.Net.UnitTest.Process.Traversal
{
    public class TestTraversal : DefaultTraversal<object, object>
    {
        public TestTraversal(List<object> traverserObjs)
        {
            var traversers = new List<Traverser>(traverserObjs.Count);
            traverserObjs.ForEach(o => traversers.Add(new Traverser(o)));
            Traversers = traversers;
            Bytecode = new Bytecode();
        }

        public TestTraversal(IReadOnlyList<object> traverserObjs, IReadOnlyList<long> traverserBulks)
        {
            var traversers = new List<Traverser>(traverserObjs.Count);
            traversers.AddRange(traverserObjs.Select((t, i) => new Traverser(t, traverserBulks[i])));
            Traversers = traversers;
            Bytecode = new Bytecode();
        }

        public TestTraversal(IList<ITraversalStrategy> traversalStrategies)
        {
            TraversalStrategies = traversalStrategies;
        }
    }
}