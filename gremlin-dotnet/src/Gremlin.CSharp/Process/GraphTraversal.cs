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
using Gremlin.Net.Process.Traversal;

namespace Gremlin.CSharp.Process
{
    public class GraphTraversal : DefaultTraversal
    {
        public GraphTraversal()
            : this(new List<ITraversalStrategy>(), new Bytecode())
        {
        }

        public GraphTraversal(ICollection<ITraversalStrategy> traversalStrategies, Bytecode bytecode)
        {
            TraversalStrategies = traversalStrategies;
            Bytecode = bytecode;
        }

        public GraphTraversal V(params object[] args)
        {
            Bytecode.AddStep("V", args);
            return this;
        }

        public GraphTraversal AddE(params object[] args)
        {
            Bytecode.AddStep("addE", args);
            return this;
        }

        public GraphTraversal AddInE(params object[] args)
        {
            Bytecode.AddStep("addInE", args);
            return this;
        }

        public GraphTraversal AddOutE(params object[] args)
        {
            Bytecode.AddStep("addOutE", args);
            return this;
        }

        public GraphTraversal AddV(params object[] args)
        {
            Bytecode.AddStep("addV", args);
            return this;
        }

        public GraphTraversal Aggregate(params object[] args)
        {
            Bytecode.AddStep("aggregate", args);
            return this;
        }

        public GraphTraversal And(params object[] args)
        {
            Bytecode.AddStep("and", args);
            return this;
        }

        public GraphTraversal As(params object[] args)
        {
            Bytecode.AddStep("as", args);
            return this;
        }

        public GraphTraversal Barrier(params object[] args)
        {
            Bytecode.AddStep("barrier", args);
            return this;
        }

        public GraphTraversal Both(params object[] args)
        {
            Bytecode.AddStep("both", args);
            return this;
        }

        public GraphTraversal BothE(params object[] args)
        {
            Bytecode.AddStep("bothE", args);
            return this;
        }

        public GraphTraversal BothV(params object[] args)
        {
            Bytecode.AddStep("bothV", args);
            return this;
        }

        public GraphTraversal Branch(params object[] args)
        {
            Bytecode.AddStep("branch", args);
            return this;
        }

        public GraphTraversal By(params object[] args)
        {
            Bytecode.AddStep("by", args);
            return this;
        }

        public GraphTraversal Cap(params object[] args)
        {
            Bytecode.AddStep("cap", args);
            return this;
        }

        public GraphTraversal Choose(params object[] args)
        {
            Bytecode.AddStep("choose", args);
            return this;
        }

        public GraphTraversal Coalesce(params object[] args)
        {
            Bytecode.AddStep("coalesce", args);
            return this;
        }

        public GraphTraversal Coin(params object[] args)
        {
            Bytecode.AddStep("coin", args);
            return this;
        }

        public GraphTraversal Constant(params object[] args)
        {
            Bytecode.AddStep("constant", args);
            return this;
        }

        public GraphTraversal Count(params object[] args)
        {
            Bytecode.AddStep("count", args);
            return this;
        }

        public GraphTraversal CyclicPath(params object[] args)
        {
            Bytecode.AddStep("cyclicPath", args);
            return this;
        }

        public GraphTraversal Dedup(params object[] args)
        {
            Bytecode.AddStep("dedup", args);
            return this;
        }

        public GraphTraversal Drop(params object[] args)
        {
            Bytecode.AddStep("drop", args);
            return this;
        }

        public GraphTraversal Emit(params object[] args)
        {
            Bytecode.AddStep("emit", args);
            return this;
        }

        public GraphTraversal Filter(params object[] args)
        {
            Bytecode.AddStep("filter", args);
            return this;
        }

        public GraphTraversal FlatMap(params object[] args)
        {
            Bytecode.AddStep("flatMap", args);
            return this;
        }

        public GraphTraversal Fold(params object[] args)
        {
            Bytecode.AddStep("fold", args);
            return this;
        }

        public GraphTraversal From(params object[] args)
        {
            Bytecode.AddStep("from", args);
            return this;
        }

        public GraphTraversal Group(params object[] args)
        {
            Bytecode.AddStep("group", args);
            return this;
        }

        public GraphTraversal GroupCount(params object[] args)
        {
            Bytecode.AddStep("groupCount", args);
            return this;
        }

        public GraphTraversal GroupV3d0(params object[] args)
        {
            Bytecode.AddStep("groupV3d0", args);
            return this;
        }

        public GraphTraversal Has(params object[] args)
        {
            Bytecode.AddStep("has", args);
            return this;
        }

        public GraphTraversal HasId(params object[] args)
        {
            Bytecode.AddStep("hasId", args);
            return this;
        }

        public GraphTraversal HasKey(params object[] args)
        {
            Bytecode.AddStep("hasKey", args);
            return this;
        }

        public GraphTraversal HasLabel(params object[] args)
        {
            Bytecode.AddStep("hasLabel", args);
            return this;
        }

        public GraphTraversal HasNot(params object[] args)
        {
            Bytecode.AddStep("hasNot", args);
            return this;
        }

        public GraphTraversal HasValue(params object[] args)
        {
            Bytecode.AddStep("hasValue", args);
            return this;
        }

        public GraphTraversal Id(params object[] args)
        {
            Bytecode.AddStep("id", args);
            return this;
        }

        public GraphTraversal Identity(params object[] args)
        {
            Bytecode.AddStep("identity", args);
            return this;
        }

        public GraphTraversal In(params object[] args)
        {
            Bytecode.AddStep("in", args);
            return this;
        }

        public GraphTraversal InE(params object[] args)
        {
            Bytecode.AddStep("inE", args);
            return this;
        }

        public GraphTraversal InV(params object[] args)
        {
            Bytecode.AddStep("inV", args);
            return this;
        }

        public GraphTraversal Inject(params object[] args)
        {
            Bytecode.AddStep("inject", args);
            return this;
        }

        public GraphTraversal Is(params object[] args)
        {
            Bytecode.AddStep("is", args);
            return this;
        }

        public GraphTraversal Key(params object[] args)
        {
            Bytecode.AddStep("key", args);
            return this;
        }

        public GraphTraversal Label(params object[] args)
        {
            Bytecode.AddStep("label", args);
            return this;
        }

        public GraphTraversal Limit(params object[] args)
        {
            Bytecode.AddStep("limit", args);
            return this;
        }

        public GraphTraversal Local(params object[] args)
        {
            Bytecode.AddStep("local", args);
            return this;
        }

        public GraphTraversal Loops(params object[] args)
        {
            Bytecode.AddStep("loops", args);
            return this;
        }

        public GraphTraversal Map(params object[] args)
        {
            Bytecode.AddStep("map", args);
            return this;
        }

        public GraphTraversal MapKeys(params object[] args)
        {
            Bytecode.AddStep("mapKeys", args);
            return this;
        }

        public GraphTraversal MapValues(params object[] args)
        {
            Bytecode.AddStep("mapValues", args);
            return this;
        }

        public GraphTraversal Match(params object[] args)
        {
            Bytecode.AddStep("match", args);
            return this;
        }

        public GraphTraversal Max(params object[] args)
        {
            Bytecode.AddStep("max", args);
            return this;
        }

        public GraphTraversal Mean(params object[] args)
        {
            Bytecode.AddStep("mean", args);
            return this;
        }

        public GraphTraversal Min(params object[] args)
        {
            Bytecode.AddStep("min", args);
            return this;
        }

        public GraphTraversal Not(params object[] args)
        {
            Bytecode.AddStep("not", args);
            return this;
        }

        public GraphTraversal Option(params object[] args)
        {
            Bytecode.AddStep("option", args);
            return this;
        }

        public GraphTraversal Optional(params object[] args)
        {
            Bytecode.AddStep("optional", args);
            return this;
        }

        public GraphTraversal Or(params object[] args)
        {
            Bytecode.AddStep("or", args);
            return this;
        }

        public GraphTraversal Order(params object[] args)
        {
            Bytecode.AddStep("order", args);
            return this;
        }

        public GraphTraversal OtherV(params object[] args)
        {
            Bytecode.AddStep("otherV", args);
            return this;
        }

        public GraphTraversal Out(params object[] args)
        {
            Bytecode.AddStep("out", args);
            return this;
        }

        public GraphTraversal OutE(params object[] args)
        {
            Bytecode.AddStep("outE", args);
            return this;
        }

        public GraphTraversal OutV(params object[] args)
        {
            Bytecode.AddStep("outV", args);
            return this;
        }

        public GraphTraversal PageRank(params object[] args)
        {
            Bytecode.AddStep("pageRank", args);
            return this;
        }

        public GraphTraversal Path(params object[] args)
        {
            Bytecode.AddStep("path", args);
            return this;
        }

        public GraphTraversal PeerPressure(params object[] args)
        {
            Bytecode.AddStep("peerPressure", args);
            return this;
        }

        public GraphTraversal Profile(params object[] args)
        {
            Bytecode.AddStep("profile", args);
            return this;
        }

        public GraphTraversal Program(params object[] args)
        {
            Bytecode.AddStep("program", args);
            return this;
        }

        public GraphTraversal Project(params object[] args)
        {
            Bytecode.AddStep("project", args);
            return this;
        }

        public GraphTraversal Properties(params object[] args)
        {
            Bytecode.AddStep("properties", args);
            return this;
        }

        public GraphTraversal Property(params object[] args)
        {
            Bytecode.AddStep("property", args);
            return this;
        }

        public GraphTraversal PropertyMap(params object[] args)
        {
            Bytecode.AddStep("propertyMap", args);
            return this;
        }

        public GraphTraversal Range(params object[] args)
        {
            Bytecode.AddStep("range", args);
            return this;
        }

        public GraphTraversal Repeat(params object[] args)
        {
            Bytecode.AddStep("repeat", args);
            return this;
        }

        public GraphTraversal Sack(params object[] args)
        {
            Bytecode.AddStep("sack", args);
            return this;
        }

        public GraphTraversal Sample(params object[] args)
        {
            Bytecode.AddStep("sample", args);
            return this;
        }

        public GraphTraversal Select(params object[] args)
        {
            Bytecode.AddStep("select", args);
            return this;
        }

        public GraphTraversal SideEffect(params object[] args)
        {
            Bytecode.AddStep("sideEffect", args);
            return this;
        }

        public GraphTraversal SimplePath(params object[] args)
        {
            Bytecode.AddStep("simplePath", args);
            return this;
        }

        public GraphTraversal Store(params object[] args)
        {
            Bytecode.AddStep("store", args);
            return this;
        }

        public GraphTraversal Subgraph(params object[] args)
        {
            Bytecode.AddStep("subgraph", args);
            return this;
        }

        public GraphTraversal Sum(params object[] args)
        {
            Bytecode.AddStep("sum", args);
            return this;
        }

        public GraphTraversal Tail(params object[] args)
        {
            Bytecode.AddStep("tail", args);
            return this;
        }

        public GraphTraversal TimeLimit(params object[] args)
        {
            Bytecode.AddStep("timeLimit", args);
            return this;
        }

        public GraphTraversal Times(params object[] args)
        {
            Bytecode.AddStep("times", args);
            return this;
        }

        public GraphTraversal To(params object[] args)
        {
            Bytecode.AddStep("to", args);
            return this;
        }

        public GraphTraversal ToE(params object[] args)
        {
            Bytecode.AddStep("toE", args);
            return this;
        }

        public GraphTraversal ToV(params object[] args)
        {
            Bytecode.AddStep("toV", args);
            return this;
        }

        public GraphTraversal Tree(params object[] args)
        {
            Bytecode.AddStep("tree", args);
            return this;
        }

        public GraphTraversal Unfold(params object[] args)
        {
            Bytecode.AddStep("unfold", args);
            return this;
        }

        public GraphTraversal Union(params object[] args)
        {
            Bytecode.AddStep("union", args);
            return this;
        }

        public GraphTraversal Until(params object[] args)
        {
            Bytecode.AddStep("until", args);
            return this;
        }

        public GraphTraversal Value(params object[] args)
        {
            Bytecode.AddStep("value", args);
            return this;
        }

        public GraphTraversal ValueMap(params object[] args)
        {
            Bytecode.AddStep("valueMap", args);
            return this;
        }

        public GraphTraversal Values(params object[] args)
        {
            Bytecode.AddStep("values", args);
            return this;
        }

        public GraphTraversal Where(params object[] args)
        {
            Bytecode.AddStep("where", args);
            return this;
        }
	}
}
