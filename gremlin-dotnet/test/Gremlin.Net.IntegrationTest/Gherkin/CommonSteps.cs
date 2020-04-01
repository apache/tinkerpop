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
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Text.RegularExpressions;
using Gherkin.Ast;
using Gremlin.Net.IntegrationTest.Gherkin.Attributes;
using Gremlin.Net.IntegrationTest.Gherkin.TraversalEvaluation;
using Gremlin.Net.Process.Traversal;
using Gremlin.Net.Structure;
using Xunit;

using static Gremlin.Net.Process.Traversal.AnonymousTraversalSource;

namespace Gremlin.Net.IntegrationTest.Gherkin
{
    internal class CommonSteps : StepDefinition
    {
        private GraphTraversalSource _g;
        private string _graphName;
        private readonly IDictionary<string, object> _parameters = new Dictionary<string, object>();
        private ITraversal _traversal;
        private object[] _result;
        private static readonly JsonSerializerOptions JsonDeserializingOptions = new JsonSerializerOptions
            {PropertyNamingPolicy = JsonNamingPolicy.CamelCase};

        private static readonly IDictionary<Regex, Func<string, string, object>> Parsers =
            new Dictionary<string, Func<string, string, object>>
            {
                {@"d\[([\d.]+)\]\.([ilfdm])", ToNumber},
                {@"D\[(.+)\]", ToDirection},
                {@"v\[(.+)\]", ToVertex},
                {@"v\[(.+)\]\.id", (x, graphName) => ToVertex(x, graphName).Id},
                {@"v\[(.+)\]\.sid", (x, graphName) => ToVertex(x, graphName).Id.ToString()},
                {@"e\[(.+)\]", ToEdge},
                {@"e\[(.+)\].id", (x, graphName) => ToEdge(x, graphName).Id},
                {@"e\[(.+)\].sid", (x, graphName) => ToEdge(x, graphName).Id.ToString()},
                {@"p\[(.+)\]", ToPath},
                {@"l\[(.*)\]", ToList},
                {@"s\[(.*)\]", ToSet},
                {@"m\[(.+)\]", ToMap},
                {@"c\[(.+)\]", ToLambda},
                {@"t\[(.+)\]", ToT},
                {"null", (_, __) => null}
            }.ToDictionary(kv => new Regex("^" + kv.Key + "$", RegexOptions.Compiled), kv => kv.Value);

        private static readonly IDictionary<char, Func<string, object>> NumericParsers =
            new Dictionary<char, Func<string, object>>
            {
                { 'i', s => Convert.ToInt32(s) },
                { 'l', s => Convert.ToInt64(s) },
                { 'f', s => Convert.ToSingle(s, CultureInfo.InvariantCulture) },
                { 'd', s => Convert.ToDouble(s, CultureInfo.InvariantCulture) },
                { 'm', s => Convert.ToDecimal(s, CultureInfo.InvariantCulture) }
            };

        [Given("the (\\w+) graph")]
        public void ChooseModernGraph(string graphName)
        {
            if (graphName == "empty")
            {
                ScenarioData.CleanEmptyData();
            }
            var data = ScenarioData.GetByGraphName(graphName);
            _graphName = graphName;
            _g = Traversal().WithRemote(data.Connection);
        }

        [Given("using the parameter (\\w+) defined as \"(.*)\"")]
        public void UsingParameter(string name, string value)
        {
            var parsedValue = ParseValue(value.Replace("\\\"", "\""), _graphName);
            _parameters.Add(name, parsedValue);
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

        [Given("the graph initializer of")]
        public void InitTraversal(string traversalText)
        {
            var traversal = TraversalParser.GetTraversal(traversalText, _g, _parameters);
            traversal.Iterate();
            
            // We may have modified the so-called `empty` graph
            if (_graphName == "empty")
            {
                ScenarioData.ReloadEmptyData();
            }
        }

        [Given("an unsupported test")]
        public void UnsupportedTest()
        {
            
        }

        [When("iterated to list")]
        public void IterateToList()
        {
            if (_traversal == null)
            {
                throw new InvalidOperationException("Traversal should be set before iterating");
            }
            ITraversal t = _traversal;
            var list = new List<object>();
            while (t.MoveNext())
            {
                list.Add(t.Current);
            }
            _result = list.ToArray();
        }

        [When("iterated next")]
        public void IterateNext()
        {
            if (_traversal == null)
            {
                throw new InvalidOperationException("Traversal should be set before iterating");
            }
            _traversal.MoveNext();
            var result = _traversal.Current;
            switch (result)
            {
                case null:
                    _result = null;
                    return;
                case object[] arrayResult:
                    _result = arrayResult;
                    return;
                case IEnumerable enumerableResult:
                    _result = enumerableResult.Cast<object>().ToArray();
                    return;
            }
            throw new InvalidCastException($"Can not convert instance of {result.GetType()} to object[]");
        }

        [Then("the result should be (\\w+)")]
        public void AssertResult(string characterizedAs, DataTable table = null)
        {
            var ordered = characterizedAs == "ordered";
            switch (characterizedAs)
            {
                case "empty":
                    Assert.Equal(0, _result.Length);
                    return;
                case "ordered":
                case "unordered":
                case "of":
                    Assert.NotNull(table);
                    var rows = table.Rows.ToArray();
                    Assert.Equal("result", rows[0].Cells.First().Value);
                    var expected = rows.Skip(1).Select(x => ParseValue(x.Cells.First().Value, _graphName));

                    if (ordered)
                    {
                        Assert.Equal(expected, _result);
                    }
                    else
                    {
                        var expectedArray = expected.ToArray();
                        foreach (var resultItem in _result)
                        {
                            Assert.Contains(resultItem, expectedArray);
                        }
                        if (characterizedAs != "of")
                        {
                            Assert.Equal(expectedArray.Length, _result.Length);   
                        }
                    }
                    break;
                default:
                    throw new NotSupportedException($"Result as '{characterizedAs}' not supported");
            }
        }

        [Then("the result should have a count of (\\d+)")]
        public void AssertCount(int count)
        {
            Assert.Equal(count, _result.Length);
        }

        [Then("the graph should return (\\d+) for count of (.+)")]
        public void AssertTraversalCount(int expectedCount, string traversalText)
        {
            if (traversalText.StartsWith("\""))
            {
                traversalText = traversalText.Substring(1, traversalText.Length - 2);
            }
            var traversal = TraversalParser.GetTraversal(traversalText, _g, _parameters);
            var count = 0;
            while (traversal.MoveNext())
            {
                count++;
            }
            Assert.Equal(expectedCount, count);
        }

        [Then("nothing should happen because")]
        public void AssertNothing(string reason)
        {
            
        }

        private static object ToMap(string stringMap, string graphName)
        {
            var jsonMap = JsonSerializer.Deserialize<JsonElement>(stringMap, JsonDeserializingOptions);
            return ParseMapValue(jsonMap, graphName);
        }

        private static object ToLambda(string stringLambda, string graphName)
        {
            return Lambda.Groovy(stringLambda);
        }

        private static object ToT(string enumName, string graphName)
        {
            return T.GetByValue(enumName);
        }

        private static object ToDirection(string enumName, string graphName)
        {
            return Direction.GetByValue(enumName);
        }

        private static object ToNumber(string stringNumber, string graphName)
        {
            return NumericParsers[stringNumber[stringNumber.Length - 1]](
                stringNumber.Substring(0, stringNumber.Length - 1));
        }

        private static object ParseMapValue(JsonElement value, string graphName)
        {
            switch (value.ValueKind)
            {
                case JsonValueKind.Object:
                {
                    return value.EnumerateObject().ToDictionary(property => ParseValue(property.Name, graphName),
                        property => ParseMapValue(property.Value, graphName));
                }
                case JsonValueKind.Array:
                    return value.EnumerateArray().Select(v => ParseMapValue(v, graphName)).ToArray();
                case JsonValueKind.Number:
                {
                    // This can maybe be simplified when this issue is resolved:
                    // https://github.com/dotnet/runtime/issues/31274
                    if (value.TryGetInt32(out var integer))
                    {
                        return integer;
                    }

                    if (value.TryGetDouble(out var floating))
                    {
                        return floating;
                    }

                    throw new ArgumentOutOfRangeException(nameof(value), value, "Not a supported number type");
                }
                case JsonValueKind.String:
                    return ParseValue(value.GetString(), graphName);
                case JsonValueKind.True:
                    return true;
                case JsonValueKind.False:
                    return false;
                case JsonValueKind.Null:
                    return null;
                default:
                    throw new ArgumentOutOfRangeException(nameof(value.ValueKind), value.ValueKind,
                        "JSON type not supported");
            }
        }

        private static ISet<object> ToSet(string stringSet, string graphName)
        {
            return new HashSet<object>(ToList(stringSet, graphName));
        }

        private static IList<object> ToList(string stringList, string graphName)
        {
            if (stringList == "")
            {
                return new List<object>(0);
            }
            return stringList.Split(',').Select(x => ParseValue(x, graphName)).ToList();
        }

        private static Vertex ToVertex(string name, string graphName)
        {
            return ScenarioData.GetByGraphName(graphName).Vertices[name];
        }

        private static Edge ToEdge(string name, string graphName)
        {
            return ScenarioData.GetByGraphName(graphName).Edges[name];
        }

        private static Path ToPath(string value, string graphName)
        {
            return new Path(new List<ISet<string>>(0), value.Split(',').Select(x => ParseValue(x, graphName)).ToList());
        }

        private static object ParseValue(string stringValue, string graphName)
        {
            Func<string, string, object> parser = null;
            string extractedValue = null;
            foreach (var kv in Parsers)
            {
                var match = kv.Key.Match(stringValue);
                if (match.Success)
                {
                    parser = kv.Value;
                    extractedValue = match.Groups[1].Value;
                    if (match.Groups.Count > 2)
                    {
                        extractedValue += match.Groups[2].Value;
                    }
                    break;
                }
            }
            return parser != null ? parser(extractedValue, graphName) : stringValue;
        }
    }
}