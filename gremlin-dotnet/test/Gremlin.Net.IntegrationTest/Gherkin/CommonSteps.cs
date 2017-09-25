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
using System.Net.Http.Headers;
using System.Runtime.CompilerServices;
using Gherkin.Ast;
using Gremlin.Net.IntegrationTest.Gherkin.Attributes;
using Gremlin.Net.Process.Traversal;
using Gremlin.Net.Structure;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Xunit;

namespace Gremlin.Net.IntegrationTest.Gherkin
{
    internal class GeneralDefinitions : StepDefinition
    {
        private GraphTraversalSource _g;
        private dynamic _traversal;
        private object[] _result;

        private static readonly IDictionary<string, Func<GraphTraversalSource, ITraversal>> FixedTranslations = 
            new Dictionary<string, Func<GraphTraversalSource, ITraversal>>
            {
                { "g.V().has(\"no\").count()", g => g.V().Has("no").Count() }
            };
        
        [Given("the modern graph")]
        public void ChooseModernGraph()
        {
            var connection = ConnectionFactory.CreateRemoteConnection();
            _g = new Graph().Traversal().WithRemote(connection);
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

        [Given("the traversal of")]
        public void TranslateTraversal(string traversalText)
        {
            if (_g == null)
            {
                throw new InvalidOperationException("g should be a traversal source");
            }
            _traversal = TraversalTranslations.GetTraversal(traversalText, _g);
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
                        var typeName = cells[0].Value;
                        var expectedValue = ConvertExpectedToType(typeName, cells[1].Value);
                        var resultItem = ConvertResultItem(typeName, _result[i]);
                        Assert.Equal(expectedValue, resultItem);
                    }
                    break;
                case "unordered":
                    rows = table.Rows.ToArray();
                    Assert.Equal(rows.Length, _result.Length);
                    foreach (var row in rows)
                    {
                        var cells = row.Cells.ToArray();
                        var typeName = cells[0].Value;
                        var expectedValue = ConvertExpectedToType(typeName, cells[1].Value);
                        // Convert all the values in the result to the type
                        var convertedResult = _result.Select(item => ConvertResultItem(typeName, item));
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

        private object ConvertExpectedToType(string typeName, string stringValue)
        {
            switch (typeName)
            {
                case "numeric":
                    return Convert.ToInt64(stringValue);
                case "string":
                    return stringValue;
                case "map":
                    IDictionary<string, JToken> jsonObject = JObject.Parse(stringValue);
                    return jsonObject.ToDictionary(item => item.Key, item => item.Value.ToString());
            }
            throw new NotSupportedException($"Data table result with subtype of {typeName} not supported");
        }
    }
}