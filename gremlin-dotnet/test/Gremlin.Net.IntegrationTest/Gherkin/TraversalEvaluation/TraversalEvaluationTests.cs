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
using System.Linq;
using Gremlin.Net.Structure;
using Xunit;

namespace Gremlin.Net.IntegrationTest.Gherkin.TraversalEvaluation
{
    public class TraversalEvaluationTests
    {
        [Fact]
        public void Traversal_Parser_Should_Parse_Into_Tokens()
        {
            var items = new[]
            {
                Tuple.Create("g.V().count()", new[] {new Token("count")}),
                Tuple.Create("g.V().values(\"name\")",
                    new[] {new Token("values", new StringParameter("name"))}),
                Tuple.Create("g.V().constant(123l)", 
                    new[] {new Token("constant", new[] {LiteralParameter.Create(123L)})}),
                Tuple.Create("g.V().constant(123)", 
                    new[] {new Token("constant", new[] {LiteralParameter.Create(123)})}),
                Tuple.Create("g.V().constant(123.50)", 
                    new[] {new Token("constant", new[] {LiteralParameter.Create(123.50m)})}),
                Tuple.Create("g.V().constant(123.1f)", 
                    new[] {new Token("constant", new[] {LiteralParameter.Create(123.1f)})}),
                Tuple.Create("g.V().has(\"no\").count()",
                    new[] {new Token("has", new StringParameter("no")), new Token("count")}),
                Tuple.Create("g.V().has(\"lang\", \"java\")",
                    new[] {new Token("has", new[] {new StringParameter("lang"), new StringParameter("java")})}),
                Tuple.Create("g.V().where(__.in(\"knows\"))",
                    new[] {new Token("where", new[] {new StaticTraversalParameter(
                        new[] {new Token("__"), new Token("in", new StringParameter("knows"))}, "__.in(\"knows\")")})}),
                Tuple.Create("g.V().has(\"age\",P.gt(27))", 
                    new[] {new Token("has", new ITokenParameter[] { new StringParameter("age"),
                        new TraversalPredicateParameter(
                            new[] { new Token("P"), new Token("gt", LiteralParameter.Create(27)) }) })}),
                Tuple.Create("g.V().count(Scope.local)", 
                    new[] { new Token("count", new TraversalEnumParameter("Scope.local"))}),
                Tuple.Create("g.V().\n  count()", new[] { new Token("count")}),
                Tuple.Create("g.V().\n has ( \"a\" ) \n.  \ncount()",
                    new[] {new Token("has", new StringParameter("a")), new Token("count")}),
                Tuple.Create("g.V().choose(__.outE(),__.as(\"a\"))", new []
                {
                    new Token("choose", new ITokenParameter[] { 
                        new StaticTraversalParameter(new[] {new Token("__"), new Token("outE")}, "__.outE()"),
                        new StaticTraversalParameter(
                            new[] {new Token("__"), new Token("as", new StringParameter("a"))}, "__.as(\"a\")")
                    })
                })
            };
            foreach (var item in items)
            {
                var parts = TraversalParser.ParseTraversal(item.Item1);
                Assert.Equal(item.Item2, parts.Skip(2));
            }
        }

        [Fact]
        public void GetTraversal_Should_Invoke_Traversal_Methods()
        {
            var traversalTexts = new []
            {
                Tuple.Create("g.V().count()", 2),
//                Tuple.Create("g.V().constant(123L)", 2), // Can be parsed using the new type-safe API
                Tuple.Create("g.V().has(\"no\").count()", 3),
                Tuple.Create("g.V().values(\"age\")", 2),
                Tuple.Create("g.V().valueMap(true, \"name\", \"age\")", 2),
                Tuple.Create("g.V().where(__.in(\"created\").count().is(1)).values(\"name\")", 3),
                Tuple.Create("g.V().count(Scope.local)", 2),
                Tuple.Create("g.V().values(\"age\").is(P.lte(30))", 3),
                Tuple.Create("g.V().optional(__.out().optional(__.out())).path().limit(1)", 4),
                Tuple.Create("g.V(1).as(\"a\").out(\"knows\").as(\"b\").\n  select(\"a\", \"b\").by(\"name\")", 6),
                Tuple.Create(
                    "g.V().hasLabel(\"software\").group().by(\"name\").by(__.bothE().values(\"weight\").sum())", 5),
                Tuple.Create("g.V().choose(__.outE().count().is(0L),__.as(\"a\"),__.as(\"b\"))" +
                                  "\n.choose(__.select(\"a\"),__.select(\"a\"),__.select(\"b\"))", 3),
                Tuple.Create("g.V().repeat(__.out()).times(2) ", 3),
                Tuple.Create("g.V().local(__.match(\n   __.as(\"project\").in(\"created\").as(\"person\"),\n__.as(\"person\").values(\"name\").as(\"name\"))).select(\"name\", \"project\").by().by(\"name\")", 5),
                Tuple.Create("g.V().as(\"a\").out().as(\"a\").out().as(\"a\").select(\"a\").by(__.unfold().values(\"name\").fold()).tail(Scope.local, 2)", 9),
                Tuple.Create("g.V().coin(1.0)", 2)
            };
            var g = new Graph().Traversal();
            foreach (var tuple in traversalTexts)
            {
                var traversal = TraversalParser.GetTraversal(tuple.Item1, g, null);
                Assert.NotNull(traversal);
                Assert.Equal(tuple.Item2, traversal.Bytecode.StepInstructions.Count);
            }
        }
    }
}