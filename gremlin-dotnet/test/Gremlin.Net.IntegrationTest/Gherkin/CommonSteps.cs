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
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using Gherkin.Ast;
using Gremlin.Net.IntegrationTest.Gherkin.Attributes;
using Gremlin.Net.IntegrationTest.Gherkin.TraversalEvaluation;
using Gremlin.Net.Process.Traversal;
using Gremlin.Net.Structure;
using Xunit;

namespace Gremlin.Net.IntegrationTest.Gherkin
{
    internal class GeneralDefinitions : StepDefinition
    {
        private GraphTraversalSource _g;
        private readonly IDictionary<string, object> _parameters = new Dictionary<string, object>();
        private dynamic _traversal;
        private object[] _result;

        private static readonly IDictionary<Regex, Func<string, object>> Parsers =
            new Dictionary<string, Func<string, object>>
            {
                {@"d\[(\d+)\]", x => Convert.ToInt64(x)},
                {@"d\[(\d+(?:\.\d+)?)\]", x => Convert.ToDouble(x)},
                {@"v\[(.+)\]", ToVertex},
                {@"v\[(.+)\]\.id", x => ToVertex(x).Id},
                {@"v\[(.+)\]\.sid", x => ToVertex(x).Id.ToString()},
                {@"e\[(.+)\]", ToEdge},
                {@"e\[(.+)\].id", s => ToEdge(s).Id},
                {@"e\[(.+)\].sid", s => ToEdge(s).Id.ToString()},
                {@"p\[(.+)\]", ToPath},
                {@"l\[(.+)\]", ToList},
                {@"s\[(.+)\]", ToSet},
                {@"m\[(.+)\]", ToMap},
            }.ToDictionary(kv => new Regex("^" + kv.Key + "$", RegexOptions.Compiled), kv => kv.Value);
        
        [Given("the modern graph")]
        public void ChooseModernGraph()
        {
            var connection = ConnectionFactory.CreateRemoteConnection();
            _g = new Graph().Traversal().WithRemote(connection);
        }

        [Given("using the parameter (\\w+) defined as \"(.*)\"")]
        public void UsingParameter(string name, string value)
        {
            _parameters.Add(name, ParseValue(value));
        }

        [Given("the traversal of")]
        public void TranslateTraversal(string traversalText)
        {
            if (_g == null)
            {
                throw new InvalidOperationException("g should be a traversal source");
            }
            _traversal = TraversalParser.GetTraversal(traversalText, _g, _parameters);
        }

        [When("iterated to list")]
        public void IterateToList()
        {
            if (!(_traversal is ITraversal))
            {
                throw new InvalidOperationException("Traversal should be set before iterating");
            }
            IEnumerable enumerable = _traversal.ToList();
            _result = enumerable.Cast<object>().ToArray();
        }

        [When("iterated next")]
        public void IterateNext()
        {
            if (!(_traversal is ITraversal))
            {
                throw new InvalidOperationException("Traversal should be set before iterating");
            }
            _result = _traversal.Next();
        }

        [Then("the result should be (\\w+)")]
        public void AssertResult(string characterizedAs, DataTable table)
        {
            TableRow[] rows;
            switch (characterizedAs)
            {
                case "empty":
                    Assert.Equal(0, _result.Length);
                    return;
                case "ordered":
                    rows = table.Rows.ToArray();
                    Assert.Equal(rows.Length, _result.Length);
                    for (var i = 0; i < rows.Length; i++)
                    {
                        var row = rows[i];
                        var cells = row.Cells.ToArray();
                        var expectedValue = ParseValue(cells[0].Value);
                        var resultItem = ConvertResultItem(null, _result[i]);
                        Assert.Equal(expectedValue, resultItem);
                    }
                    break;
                case "unordered":
                    rows = table.Rows.ToArray();
                    Assert.Equal(rows.Length, _result.Length);
                    foreach (var row in rows)
                    {
                        var cells = row.Cells.ToArray();
                        var expectedValue = ParseValue(cells[0].Value);
                        // Convert all the values in the result to the type
                        var convertedResult = _result.Select(item => ConvertResultItem(null, item));
                        Assert.Contains(expectedValue, convertedResult);
                    }
                    break;
                default:
                    throw new NotSupportedException($"Result as '{characterizedAs}' not supported");
            }
        }

        private object ConvertResultItem(string typeName, object value)
        {
            if (typeName == "map")
            {
                // We need to convert the original typed value into IDictionary<string, string>
                return StringMap(
                    Assert.IsAssignableFrom<IDictionary>(value));
            }
            return value;
        }

        private IDictionary<string, string> StringMap(IDictionary originalMap)
        {
            var result = new Dictionary<string, string>(originalMap.Count);
            foreach (var key in originalMap.Keys)
            {
                result.Add(key.ToString(), originalMap[key]?.ToString());
            }
            return result;
        }

        private static IDictionary ToMap(string arg)
        {
            throw new NotImplementedException();
        }

        private static ICollection ToSet(string arg)
        {
            throw new NotImplementedException();
        }

        private static IList ToList(string arg)
        {
            throw new NotImplementedException();
        }

        private static Vertex ToVertex(string name)
        {
            return ScenarioData.Instance.ModernVertices[name];
        }

        private static Edge ToEdge(string s)
        {
            throw new NotImplementedException();
        }

        private static Path ToPath(string arg)
        {
            throw new NotImplementedException();
        }

        private static object ParseValue(string stringValue)
        {
            Func<string, object> parser = null;
            string extractedValue = null;
            foreach (var kv in Parsers)
            {
                var match = kv.Key.Match(stringValue);
                if (match.Success)
                {
                    parser = kv.Value;
                    extractedValue = match.Groups[1].Value;
                    break;
                }
            }
            return parser != null ? parser(extractedValue) : stringValue;
        }
    }
}