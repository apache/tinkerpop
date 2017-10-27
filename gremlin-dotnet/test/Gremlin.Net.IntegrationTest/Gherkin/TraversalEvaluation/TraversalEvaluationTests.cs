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
using Xunit.Abstractions;

namespace Gremlin.Net.IntegrationTest.Gherkin.TraversalEvaluation
{
    public class TraversalEvaluationTests
    {
        private readonly ITestOutputHelper _output;

        public TraversalEvaluationTests(ITestOutputHelper output)
        {
            _output = output;
        }
        
        [Fact]
        public void Traversal_Parser_Should_Parse_Into_Tokens()
        {
            var items = new[]
            {
                Tuple.Create("g.V().count()", new[] {new Token("count")}),
                Tuple.Create("g.V().values(\"name\")",
                    new[] {new Token("values", new StringParameter("name"))}),
                Tuple.Create("g.V().constant(123l)", 
                    new[] {new Token("constant", new[] {NumericParameter.Create(123L)})}),
                Tuple.Create("g.V().has(\"no\").count()",
                    new[] {new Token("has", new StringParameter("no")), new Token("count")}),
                Tuple.Create("g.V().has(\"lang\", \"java\")",
                    new[] {new Token("has", new[] {new StringParameter("lang"), new StringParameter("java")})}),
                Tuple.Create("g.V().where(__.in(\"knows\"))",
                    new[] {new Token("where", new[] {new StaticTraversalParameter(
                        new[] {new Token("__"), new Token("in", new StringParameter("knows"))})})}),
                Tuple.Create("g.V().has(\"age\", P.gt(27)", 
                    new[] {new Token("has", new ITokenParameter[] { new StringParameter("age"),
                        new TraversalPredicateParameter(
                            new[] { new Token("P"), new Token("gt", NumericParameter.Create(27)) }) })})
            };
            foreach (var item in items)
            {
                var parts = TraversalParser.ParseTraversal(item.Item1);
                _output.WriteLine("Parsing " + item.Item1);
                if (parts[parts.Count-1].Parameters != null)
                {
                    _output.WriteLine("{0}", parts[parts.Count-1].Parameters.Count);
                    _output.WriteLine("Values: " +
                                      string.Join(", ", parts[parts.Count - 1].Parameters.Select(p => p.ToString())));
                }
                Assert.Equal(new[] {new Token("g"), new Token("V")}.Concat(item.Item2), parts);
            }
        }

        [Fact]
        public void GetTraversal_Should_Invoke_Traversal_Methods()
        {
            var traversalTexts = new []
            {
                "g.V().count()",
                "g.V().constant(123)",
                "g.V().has(\"no\").count()",
                "g.V().values(\"age\")",
                "g.V().valueMap(\"name\", \"age\")",
                "g.V().repeat(__.both()).times(5)"
            };
            var g = new Graph().Traversal();
            foreach (var text in traversalTexts)
            {
                Assert.NotNull(TraversalParser.GetTraversal(text, g));
            }
        }
    }
}