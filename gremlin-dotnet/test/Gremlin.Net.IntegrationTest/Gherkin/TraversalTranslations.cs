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
using System.Collections.Generic;
using System.Reflection;
using Gremlin.Net.Process.Traversal;

namespace Gremlin.Net.IntegrationTest.Gherkin
{
    internal class TraversalTranslations
    {
        private static readonly IDictionary<string, Func<GraphTraversalSource, ITraversal>> FixedTranslations = 
            new Dictionary<string, Func<GraphTraversalSource, ITraversal>>
            {
                { "g.V().has(\"no\").count()", g => g.V().Has("no").Count() },
                { "g.V().fold().count(Scope.local)", g => g.V().Fold<object>().Count(Scope.Local)}
            };

        internal static ITraversal GetTraversal(string traversalText, GraphTraversalSource g)
        {
            Func<GraphTraversalSource, ITraversal> traversalBuilder;
            if (!FixedTranslations.TryGetValue(traversalText, out traversalBuilder))
            {
                return BuildFromMethods(traversalText, g);
            }
            return traversalBuilder(g);
        }

        private static ITraversal BuildFromMethods(string traversalText, GraphTraversalSource g)
        {
            var parts = traversalText.Split('.');
            if (parts[0] != "g")
            {
                throw BuildException(traversalText);
            }
            ITraversal traversal;
            switch (parts[1])
            {
                case "V()":
                    traversal = g.V();
                    break;
                case "E()":
                    traversal = g.E();
                    break;
                default:
                    throw BuildException(traversalText);
            }
            for (var i = 2; i < parts.Length; i++)
            {
                var name = GetCsharpName(parts[i], traversalText);
                var method = traversal.GetType().GetMethod(name);
                if (method == null)
                {
                    throw new InvalidOperationException($"Traversal method '{parts[i]}' not found for testing");
                }
                if (method.IsGenericMethod)
                {
                    throw new InvalidOperationException(
                        $"Can not build traversal to test as '{name}()' method is generic");
                }
                traversal = (ITraversal) method.Invoke(traversal, new object[] { new object[0]});
            }
            return traversal;
        }

        private static string GetCsharpName(string part, string traversalText)
        {
            if (!part.EndsWith("()"))
            {
                throw BuildException(traversalText);
            }
            // Transform to PascalCasing and remove the parenthesis
            return char.ToUpper(part[0]) + part.Substring(1, part.Length - 3);
        }

        private static Exception BuildException(string traversalText)
        {
            return new InvalidOperationException($"Can not build a traversal to test from '{traversalText}'");
        }
    }
}