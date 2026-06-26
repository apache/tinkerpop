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
 *   http://www.apache.org/licenses/LICENSE-2.0
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
using System.Threading.Tasks;
using Gherkin.Ast;
using Gremlin.Net.IntegrationTest.Gherkin.Attributes;
using Gremlin.Net.Process.Traversal;
using Gremlin.Net.Structure;
using Gremlin.Net.Structure.IO.GraphBinary4;
using Xunit;

using static Gremlin.Net.Process.Traversal.AnonymousTraversalSource;

namespace Gremlin.Net.IntegrationTest.Gherkin
{
    internal class CommonSteps : StepDefinition
    {
        private static readonly bool Parameterize =
            Environment.GetEnvironmentVariable("PARAMETERIZE") == "true";

        private GraphTraversalSource? _g;
        private string? _graphName;
        private readonly IDictionary<string, object?> _parameters = new Dictionary<string, object?>();
        private readonly IDictionary<string, object?> _sideEffects = new Dictionary<string, object?>();
        private ITraversal? _traversal;
        private object?[]? _result;
        private Exception? _error;

        private static readonly JsonSerializerOptions JsonDeserializingOptions =
            new() { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };
        
        public static ScenarioData ScenarioData { get; set; } = new ScenarioData(new GraphBinary4MessageSerializer());

        private static readonly IDictionary<Regex, Func<string, string, object?>> Parsers =
            new Dictionary<string, Func<string, string, object?>>
            {
                {@"str\[(.*)\]", (x, graphName) => x }, //returns the string value as is
                {@"vp\[(.+)\]", ToVertexProperty},
                {@"prop\[(.+)\]", ToProperty},
                {@"dt\[(.+)\]", ToDateTime},
                {@"uuid\[(.+)\]", ToUuid},
                {@"char\[(.)\]", ToChar},
                {@"dur\[(.+)\]", ToDuration},
                {@"bin\[(.*)\]", ToBinary},
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
                { 'b', s => Convert.ToSByte(s) },
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
            var isMultiLabel = ScenarioData.CurrentScenario != null &&
                (ScenarioData.CurrentScenario.Tags.Any(t => t.Name == "@MultiLabel") ||
                 (ScenarioData.CurrentFeature != null && ScenarioData.CurrentFeature.Tags.Any(t => t.Name == "@MultiLabel")));
            var isMultiLabelDefault = ScenarioData.CurrentScenario != null &&
                (ScenarioData.CurrentScenario.Tags.Any(t => t.Name == "@MultiLabelDefault") ||
                 (ScenarioData.CurrentFeature != null && ScenarioData.CurrentFeature.Tags.Any(t => t.Name == "@MultiLabelDefault")));

            if (isMultiLabelDefault && graphName == "empty")
            {
                ScenarioData.CleanMultilabelData();
                var data = ScenarioData.GetByGraphName("multilabel");
                _graphName = "multilabel";
                _g = Traversal().With(data.Connection).With("multilabel");
            }
            else if (isMultiLabel && graphName == "empty")
            {
                ScenarioData.CleanMultilabelData();
                var data = ScenarioData.GetByGraphName("multilabel");
                _graphName = "multilabel";
                _g = Traversal().With(data.Connection);
            }
            else
            {
                if (graphName == "empty")
                {
                    ScenarioData.CleanEmptyData();
                }
                var data = ScenarioData.GetByGraphName(graphName);
                _graphName = graphName;
                _g = Traversal().With(data.Connection);
            }
        }

        [Given("using the parameter (\\w+) defined as \"(.*)\"")]
        public void UsingParameter(string name, string value)
        {
            var parsedValue = ParseValue(value.Replace("\\\"", "\""), _graphName!);
            _parameters.Add(name, parsedValue);
        }

        [Given("using the parameter (\\w+) of P.(\\w+)\\(\"(.*)\"\\)")]
        public void UsingParameterP(string name, string pval, string value)
        {
            var parsedValue = ParseValue(value.Replace("\\\"", "\""), _graphName!);
            _parameters.Add(name, new P(pval, parsedValue));
        }
        
        [Given("using the side effect (\\w+) defined as \"(.*)\"")]
        public void UsingSideEffect(string key, string value)
        {
            var parsedValue = ParseValue(value.Replace("\\\"", "\""), _graphName!);
            _sideEffects.Add(key, parsedValue);
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

            if (Parameterize)
            {
                _traversal =
                    Gremlin.UseParameterizedTraversal(ScenarioData.CurrentScenario!.Name, _g, _parameters, _sideEffects);
            }
            else
            {
                _traversal =
                    Gremlin.UseTraversal(ScenarioData.CurrentScenario!.Name, _g, _parameters, _sideEffects);
            }
        }

        [Given("the graph initializer of")]
        public void InitTraversal(string traversalText)
        {
            ITraversal traversal;
            if (Parameterize)
            {
                traversal = Gremlin.UseParameterizedTraversal(ScenarioData.CurrentScenario!.Name, _g, _parameters, _sideEffects);
            }
            else
            {
                traversal = Gremlin.UseTraversal(ScenarioData.CurrentScenario!.Name, _g, _parameters, _sideEffects);
            }
            traversal.Iterate();
            
            // We may have modified the so-called `empty` or `multilabel` graph
            if (_graphName == "empty")
            {
                ScenarioData.ReloadEmptyData();
            }
            else if (_graphName == "multilabel")
            {
                ScenarioData.ReloadMultilabelData();
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
            var list = new List<object>();
            try
            {
                // Use the base ITraversal interface's MoveNextAsync() and CurrentObject
                // to avoid type conversion issues with ITraversal<TStart, TEnd>.Current
                while (_traversal.MoveNextAsync().AsTask().GetAwaiter().GetResult())
                {
                    list.Add(_traversal.CurrentObject!);
                }
                _result = list.ToArray();
            }
            catch (Exception ex)
            {
                _error = UnwrapException(ex);
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
                _traversal.MoveNextAsync().AsTask().GetAwaiter().GetResult();
                var result = _traversal.CurrentObject;
                switch (result)
                {
                    case null:
                        _result = null;
                        return;
                    case object[] arrayResult:
                        _result = arrayResult;
                        return;
                    case Graph graphResult:
                        // Graph is a container of vertices/edges but not iterable itself; wrap in a
                        // single-element array so assertions like AssertSubgraphStructure can find it.
                        _result = new object?[] { graphResult };
                        return;
                    case Tree treeResult:
                        // Tree should be treated as a single result so AssertTreeStructure can inspect it;
                        // wrap it in a single-element array rather than letting a later case flatten it.
                        _result = new object?[] { treeResult };
                        return;
                    case IEnumerable enumerableResult:
                        _result = enumerableResult.Cast<object>().ToArray();
                        return;
                }
            }
            catch (Exception ex)
            {
                _error = UnwrapException(ex);
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

            // Unwrap AggregateException if the real exception is nested inside.
            // With streaming deserialization, exceptions from the channel/async pipeline
            // may be wrapped in AggregateException depending on how the async task is
            // awaited (e.g., Task.Wait() vs GetAwaiter().GetResult()).
            var errorMessage = UnwrapException(_error).Message;

            switch (comparison) {
                case "containing":
                    Assert.Contains(expectedMessage.ToUpper(), errorMessage.ToUpper());
                    break;
                case "starting":
                    Assert.StartsWith(expectedMessage.ToUpper(), errorMessage.ToUpper());
                    break;
                case "ending":
                    Assert.EndsWith(expectedMessage.ToUpper(), errorMessage.ToUpper());
                    break;
                default:
                    throw new NotSupportedException(
                            "Unknown comparison of " + comparison + " - must be one of: containing, starting or ending");
            }

            // consume the error now that it has been asserted
            _error = null;
        }

        /// <summary>
        ///     Unwraps an exception to find the innermost meaningful exception.
        ///     With streaming deserialization, a ResponseException thrown in the background
        ///     deserialization task may be wrapped in AggregateException when the consuming
        ///     code uses Task.Wait() or similar synchronous blocking patterns.
        /// </summary>
        private static Exception UnwrapException(Exception ex)
        {
            while (true)
            {
                switch (ex)
                {
                    case AggregateException agg when agg.InnerExceptions.Count == 1:
                        ex = agg.InnerExceptions[0];
                        continue;
                    default:
                        return ex;
                }
            }
        }


        [Then("the result should be a tree with a structure of")]
        public void AssertTreeStructure(string expectedTree)
        {
            AssertThatNoErrorWasThrown();

            // The traversal yields a single Tree as the only result.
            Assert.NotNull(_result);
            var tree = Assert.IsType<Tree>(_result![0]);

            // an empty doc string represents an empty tree
            var roots = ParseTree(expectedTree);

            // validate that the tree matches the parsed expected structure
            Assert.Equal(roots.Count, tree.RootNodes().Count);
            foreach (var root in roots)
            {
                Assert.True(tree.HasChild(root.Value), $"Tree not matching at {root.Value}");
                ValidateTreeStructure(tree.ChildAt(root.Value), root);
            }
        }

        private static void ValidateTreeStructure(Tree actualTree, TreeNode expectedNode)
        {
            Assert.Equal(expectedNode.Children.Count, actualTree.RootNodes().Count);
            foreach (var child in expectedNode.Children)
            {
                Assert.True(actualTree.HasChild(child.Value), $"Tree not matching at {child.Value}");
                ValidateTreeStructure(actualTree.ChildAt(child.Value), child);
            }
        }

        /// <summary>
        ///     Parses the ascii-tree structure as taken from the Gherkin feature file. Mirrors the Java
        ///     <c>StepDefinition.parseTree</c>: a line's depth is the number of leading spaces divided by three,
        ///     and the node value is the line with the leading <c>|--</c> marker stripped and trimmed.
        /// </summary>
        private List<TreeNode> ParseTree(string asciiTree)
        {
            var roots = new List<TreeNode>();
            if (string.IsNullOrEmpty(asciiTree)) return roots;

            var lines = Regex.Split(asciiTree, "\r\n|\r|\n");
            var levelMap = new Dictionary<int, TreeNode>();

            foreach (var line in lines)
            {
                if (line.Length == 0) continue;

                var level = CountLeadingTreeLevels(line);
                var value = line.Replace("|--", "").Trim();

                var node = new TreeNode(ParseValue(value, _graphName!));
                if (level == 0)
                {
                    roots.Add(node);
                }
                else
                {
                    levelMap[level - 1].Children.Add(node);
                }

                levelMap[level] = node;
            }

            return roots;
        }

        private static int CountLeadingTreeLevels(string line)
        {
            var count = 0;
            foreach (var c in line)
            {
                if (c == ' ') count++;
                else break;
            }

            return count / 3; // 3 spaces per level
        }

        /// <summary>
        ///     An internal tree-structure that holds the expected tree defined in the gherkin feature files.
        /// </summary>
        private sealed class TreeNode
        {
            public object? Value { get; }
            public List<TreeNode> Children { get; }

            public TreeNode(object? value)
            {
                Value = value;
                Children = new List<TreeNode>();
            }
        }

        [Then("the result should be a subgraph with the following")]
        public void AssertSubgraphStructure(DataTable? table = null)
        {
            AssertThatNoErrorWasThrown();

            // The Cap step yields a single Graph instance as the only result.
            Assert.NotNull(_result);
            var sg = Assert.IsType<Graph>(_result![0]);

            if (table == null)
            {
                return;
            }

            var rows = table.Rows.ToArray();
            var columnName = rows[0].Cells.First().Value;
            var assertingVertices = columnName == "vertices";

            if (assertingVertices)
            {
                var expectedVertices = rows.Skip(1)
                    .Select(r => (Vertex)ParseValue(r.Cells.First().Value, _graphName!)!)
                    .ToList();
                Assert.Equal(expectedVertices.Count, sg.Vertices.Count);

                foreach (var expected in expectedVertices)
                {
                    Assert.NotNull(expected.Id);
                    Assert.True(sg.Vertices.ContainsKey(expected.Id!),
                        $"Expected subgraph to contain vertex with id {expected.Id}");
                    var actual = sg.Vertices[expected.Id!];
                    Assert.Equal(expected.Label, actual.Label);

                    var variableKey = actual.Label == "person" ? "age" : "lang";
                    Assert.Equal(expected.Property("name")?.Value, actual.Property("name")?.Value);
                    Assert.Equal(expected.Property(variableKey)?.Value, actual.Property(variableKey)?.Value);
                }
            }
            else
            {
                var expectedEdges = rows.Skip(1)
                    .Select(r => (Edge)ParseValue(r.Cells.First().Value, _graphName!)!)
                    .ToList();
                Assert.Equal(expectedEdges.Count, sg.Edges.Count);

                foreach (var expected in expectedEdges)
                {
                    Assert.NotNull(expected.Id);
                    Assert.True(sg.Edges.ContainsKey(expected.Id!),
                        $"Expected subgraph to contain edge with id {expected.Id}");
                    var actual = sg.Edges[expected.Id!];
                    Assert.Equal(expected.Label, actual.Label);
                    Assert.Equal(expected.Property("weight")?.Value, actual.Property("weight")?.Value);
                    Assert.Equal(expected.OutV.Id, actual.OutV.Id);
                    Assert.Equal(expected.InV.Id, actual.InV.Id);
                }
            }
        }

        [Then("the result should be (\\w+)\\s*")]
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
                        var expectedList = expected.ToList();
                        var resultList = _result!.ToList();
                        Assert.Equal(expectedList.Count, resultList.Count);
                        for (int i = 0; i < expectedList.Count; i++)
                        {
                            if (expectedList[i] is Property expectedProp && resultList[i] is Property resultProp)
                            {
                                Assert.Equal(expectedProp.Key, resultProp.Key);
                                Assert.Equal(expectedProp.Value, resultProp.Value);
                            }
                            else
                            {
                                Assert.Equal(expectedList[i], resultList[i]);
                            }
                        }
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
                            else if (resultItem is Property resultProperty)
                            {
                                var found = false;
                                foreach (var expectedItem in expectedArray)
                                {
                                    if (expectedItem is Property expectedProperty &&
                                        resultProperty.Key == expectedProperty.Key &&
                                        Equals(resultProperty.Value, expectedProperty.Value))
                                    {
                                        found = true;
                                        break;
                                    }
                                }
                                Assert.True(found, $"Property {resultItem} not found in expected results");
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
            
            var traversal = Parameterize
                ? (ITraversal) Gremlin.UseParameterizedTraversal(ScenarioData.CurrentScenario!.Name, _g, _parameters, _sideEffects)
                : (ITraversal) Gremlin.UseTraversal(ScenarioData.CurrentScenario!.Name, _g, _parameters, _sideEffects);
            
            var count = 0;
            while (traversal.MoveNextAsync().AsTask().GetAwaiter().GetResult())
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
            return SplitByElement(stringList).Select(x => ParseValue(x, graphName)).ToList();
        }

        private static List<string> SplitByElement(string s)
        {
            var result = new List<string>();
            var depth = 0;
            var current = new System.Text.StringBuilder();
            foreach (var c in s)
            {
                if (c == '[')
                {
                    depth++;
                    current.Append(c);
                }
                else if (c == ']')
                {
                    depth--;
                    current.Append(c);
                }
                else if (c == ',' && depth == 0)
                {
                    result.Add(current.ToString().Trim());
                    current.Clear();
                }
                else
                {
                    current.Append(c);
                }
            }
            if (current.Length > 0)
                result.Add(current.ToString().Trim());
            return result;
        }

        private static object ToDateTime(string date, string graphName)
        {
            return DateTimeOffset.Parse(date);
        }

        private static object ToUuid(string uuid, string graphName)
        {
            if (Guid.TryParse(uuid, out Guid result))
            {
                return result;
            }
            return null;
        }

        private static object ToChar(string value, string graphName)
        {
            return value[0];
        }

        private static object ToDuration(string value, string graphName)
        {
            var parts = value.Split(',');
            var seconds = long.Parse(parts[0].Trim());
            var nanos = int.Parse(parts[1].Trim());
            var isPositive = parts.Length < 3 || bool.Parse(parts[2].Trim());
            // TimeSpan has 100-nanosecond tick resolution; sub-100ns values are truncated
            var ts = TimeSpan.FromSeconds(seconds) + TimeSpan.FromTicks(nanos / 100);
            return isPositive ? ts : ts.Negate();
        }

        private static object ToBinary(string base64, string graphName)
        {
            return Convert.FromBase64String(base64);
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

        private static object ToProperty(string value, string graphName)
        {
            var commaIndex = value.IndexOf(',');
            if (commaIndex < 0)
            {
                return new Property(value, null);
            }
            var key = value.Substring(0, commaIndex);
            var valueStr = value.Substring(commaIndex + 1);
            var parsedValue = ParseValue(valueStr, graphName);
            return new Property(key, parsedValue);
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