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
using System.Numerics;
using System.Text.Json;
using System.Text.RegularExpressions;
using Gherkin.Ast;
using Gremlin.Net.IntegrationTest.Gherkin.Attributes;
using Gremlin.Net.Process.Traversal;
using Gremlin.Net.Structure;
using Gremlin.Net.Structure.IO.GraphSON;
using Xunit;

using static Gremlin.Net.Process.Traversal.AnonymousTraversalSource;

namespace Gremlin.Net.IntegrationTest.Gherkin
{
    internal class CommonSteps : StepDefinition
    {
        private GraphTraversalSource? _g;
        private string? _graphName;
        private readonly IDictionary<string, object?> _parameters = new Dictionary<string, object?>();
        private ITraversal? _traversal;
        private object?[]? _result;
        private Exception? _error;

        private static readonly JsonSerializerOptions JsonDeserializingOptions =
            new() { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };
        
        public static ScenarioData ScenarioData { get; set; } = new ScenarioData(new GraphSON3MessageSerializer());

        private static readonly IDictionary<Regex, Func<string, string, object?>> Parsers =
            new Dictionary<string, Func<string, string, object?>>
            {
                {@"str\[(.*)\]", (x, graphName) => x }, //returns the string value as is
                {@"vp\[(.+)\]", ToVertexProperty},
                {@"dt\[(.+)\]", ToDateTime},
                {@"d\[(.*)\]\.([bsilfdmn])", ToNumber},
                {@"D\[(.+)\]", ToDirection},
                {@"M\[(.+)\]", ToMerge},
                {@"v\[(.+)\]", ToVertex},
                {@"v\[(.+)\]\.id", (x, graphName) => ToVertex(x, graphName).Id},
                {@"v\[(.+)\]\.sid", (x, graphName) => ToVertex(x, graphName).Id!.ToString()},
                {@"e\[(.+)\]", ToEdge},
                {@"e\[(.+)\].id", (x, graphName) => ToEdge(x, graphName).Id},
                {@"e\[(.+)\].sid", (x, graphName) => ToEdge(x, graphName).Id!.ToString()},
                {@"p\[(.+)\]", ToPath},
                {@"l\[(.*)\]", ToList},
                {@"s\[(.*)\]", ToSet},
                {@"m\[(.+)\]", ToMap},
                {@"c\[(.+)\]", ToLambda},
                {@"t\[(.+)\]", ToT},
                {"null", (_, __) => null},
                {"true", (_, __) => true},
                {"false", (_, __) => false},
                {@"d\[NaN\]", (_, __) => Double.NaN},
                {@"d\[Infinity\]", (_, __) => Double.PositiveInfinity},
                {@"d\[-Infinity\]", (_, __) => Double.NegativeInfinity}
            }.ToDictionary(kv => new Regex("^" + kv.Key + "$", RegexOptions.Compiled), kv => kv.Value);

        private static readonly IDictionary<char, Func<string, object>> NumericParsers =
            new Dictionary<char, Func<string, object>>
            {
                { 'b', s => Convert.ToByte(s) },
                { 's', s => Convert.ToInt16(s) },
                { 'i', s => Convert.ToInt32(s) },
                { 'l', s => Convert.ToInt64(s) },
                { 'f', s => Convert.ToSingle(s, CultureInfo.InvariantCulture) },
                { 'd', s => Convert.ToDouble(s, CultureInfo.InvariantCulture) },
                { 'm', s => Convert.ToDecimal(s, CultureInfo.InvariantCulture) },
                { 'n', s => BigInteger.Parse(s) }
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
            _g = Traversal().With(data.Connection);
        }

        [Given("using the parameter (\\w+) defined as \"(.*)\"")]
        public void UsingParameter(string name, string value)
        {
            var parsedValue = ParseValue(value.Replace("\\\"", "\""), _graphName!);
            _parameters.Add(name, parsedValue);
        }

        [Given("the traversal of")]
        public void TranslateTraversal(string traversalText)
        {
            if (_g == null)
            {
                throw new InvalidOperationException("g should be a traversal source");
            }

            if (ScenarioData.CurrentFeature!.Tags.Any(t => t.Name == "@GraphComputerOnly"))
            {
                _g = _g.WithComputer();
            }

            _traversal =
                Gremlin.UseTraversal(ScenarioData.CurrentScenario!.Name, _g, _parameters);
        }

        [Given("the graph initializer of")]
        public void InitTraversal(string traversalText)
        {
            var traversal =
                Gremlin.UseTraversal(ScenarioData.CurrentScenario!.Name, _g, _parameters);
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
            try
            {
                while (t.MoveNext())
                {
                    list.Add(t.Current);
                }
                _result = list.ToArray();
            }
            catch (Exception ex)
            {
                _error = ex;
            }
        }

        [When("iterated next")]
        public void IterateNext()
        {
            if (_traversal == null)
            {
                throw new InvalidOperationException("Traversal should be set before iterating");
            }

            try
            {
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
            }
            catch (Exception ex)
            {
                _error = ex;
            }
        }

        [Then("the traversal will raise an error")]
        public void TraversalWillRaiseError()
        {
            Assert.NotNull(_error);

            // consume the error now that it has been asserted
            _error = null;
        }

        [Then("the traversal will raise an error with message (\\w+) text of \"(.*)\"")]
        public void TraversalWillRaiseErrorWithMessage(string comparison, string expectedMessage)
        {
            Assert.NotNull(_error);

            switch (comparison) {
                case "containing":
                    Assert.Contains(expectedMessage.ToUpper(), _error.Message.ToUpper());
                    break;
                case "starting":
                    Assert.StartsWith(expectedMessage.ToUpper(), _error.Message.ToUpper());
                    break;
                case "ending":
                    Assert.EndsWith(expectedMessage.ToUpper(), _error.Message.ToUpper());
                    break;
                default:
                    throw new NotSupportedException(
                            "Unknown comparison of " + comparison + " - must be one of: containing, starting or ending");
            }

            // consume the error now that it has been asserted
            _error = null;
        }

        [Then("the result should be (\\w+)")]
        public void AssertResult(string characterizedAs, DataTable? table = null)
        {
            AssertThatNoErrorWasThrown();

            var ordered = characterizedAs == "ordered";
            switch (characterizedAs)
            {
                case "empty":
                    Assert.Empty(_result!);
                    return;
                case "ordered":
                case "unordered":
                case "of":
                    Assert.NotNull(table);
                    var rows = table.Rows.ToArray();
                    Assert.Equal("result", rows[0].Cells.First().Value);
                    var expected = rows.Skip(1).Select(x => ParseValue(x.Cells.First().Value, _graphName!));

                    if (ordered)
                    {
                        Assert.Equal(expected, _result!);
                    }
                    else
                    {
                        var expectedArray = expected.ToArray();
                        foreach (var resultItem in _result!)
                        {
                            if (resultItem is Dictionary<object, object> resultItemDict)
                            {
                                var expectedArrayContainsResultDictionary = false;
                                foreach (var expectedItem in expectedArray)
                                {
                                    if (expectedItem is not Dictionary<object, object> expectedItemDict) continue;
                                    if (!expectedItemDict.DeepEqual(resultItemDict)) continue;
                                    expectedArrayContainsResultDictionary = true;
                                    break;
                                }
                                Assert.True(expectedArrayContainsResultDictionary);
                            }
                            else if (resultItem is HashSet<object> resultItemSet)
                            {
                                var expectedArrayContainsResultAsSet = false;
                                foreach (var expectedItem in expectedArray)
                                {
                                    if (expectedItem is not HashSet<object> expectedItemSet) continue;
                                    if (!expectedItemSet.SetEquals(resultItemSet)) continue;
                                    expectedArrayContainsResultAsSet = true;
                                    break;
                                }
                                Assert.True(expectedArrayContainsResultAsSet);
                            }
                            else if (resultItem is double resultItemDouble &&
                                     expectedArray.Select(e => e!.GetType()).Any(t => t == typeof(decimal)))
                            {
                                // Java seems to use BigDecimal by default sometimes where .NET uses double, but we only
                                // care for the value not its type here. So we just convert these to decimal (equivalent
                                // to Java's BigDecimal) and then check whether the item is in the expected results.
                                Assert.Contains((decimal)resultItemDouble, expectedArray);
                            }
                            else
                            {
                                Assert.Contains(resultItem, expectedArray);
                            }
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
            AssertThatNoErrorWasThrown();

            Assert.Equal(count, _result!.Length);
        }

        [Then("the graph should return (\\d+) for count of (.+)")]
        public void AssertTraversalCount(int expectedCount, string traversalText)
        {
            AssertThatNoErrorWasThrown();

            if (traversalText.StartsWith("\""))
            {
                traversalText = traversalText.Substring(1, traversalText.Length - 2);
            }
            
            var traversal =
                Gremlin.UseTraversal(ScenarioData.CurrentScenario!.Name, _g, _parameters);
            
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

        private void AssertThatNoErrorWasThrown()
        {
            if (_error != null) throw _error;
        }

        private static object ToMap(string stringMap, string graphName)
        {
            var jsonMap = JsonSerializer.Deserialize<JsonElement>(stringMap, JsonDeserializingOptions);
            return ParseMapValue(jsonMap, graphName)!;
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

        private static object ToMerge(string enumName, string graphName)
        {
            return Merge.GetByValue(enumName);
        }

        private static object ToNumber(string stringNumber, string graphName)
        {
            return NumericParsers[stringNumber[stringNumber.Length - 1]](
                stringNumber.Substring(0, stringNumber.Length - 1));
        }

        private static object? ParseMapValue(JsonElement value, string graphName)
        {
            switch (value.ValueKind)
            {
                case JsonValueKind.Object:
                {
                    return value.EnumerateObject().ToDictionary(property => ParseValue(property.Name, graphName)!,
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
                    return ParseValue(value.GetString()!, graphName);
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

        private static ISet<object?> ToSet(string stringSet, string graphName)
        {
            return new HashSet<object?>(ToList(stringSet, graphName));
        }

        private static IList<object?> ToList(string stringList, string graphName)
        {
            if (stringList == "")
            {
                return new List<object?>(0);
            }
            return stringList.Split(',').Select(x => ParseValue(x, graphName)).ToList();
        }

        private static object ToDateTime(string date, string graphName)
        {
            return DateTimeOffset.Parse(date);
        }

        private static Vertex ToVertex(string name, string graphName)
        {
            if (ScenarioData.GetByGraphName(graphName).Vertices.ContainsKey(name))
            {
                return ScenarioData.GetByGraphName(graphName).Vertices[name];
            }
            else
            {
                return new Vertex(name);
            }
        }

        private static Edge ToEdge(string name, string graphName)
        {
            return ScenarioData.GetByGraphName(graphName).Edges[name];
        }

        private static VertexProperty ToVertexProperty(string triplet, string graphName)
        {
            return ScenarioData.GetByGraphName(graphName).VertexProperties[triplet];
        }

        private static Path ToPath(string value, string graphName)
        {
            return new Path(new List<ISet<string>>(0),
                value.Split(',').Select(x => ParseValue(x, graphName)).ToList());
        }

        private static object? ParseValue(string stringValue, string graphName)
        {
            Func<string, string, object?>? parser = null;
            string? extractedValue = null;
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
            return parser != null ? parser(extractedValue!, graphName) : stringValue;
        }
    }
}