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
using System.Reflection;
using Gremlin.Net.Process.Traversal;

namespace Gremlin.Net.IntegrationTest.Gherkin.TraversalEvaluation
{
    internal class ModernGraphTypeInformation
    {
        private static readonly IDictionary<string, Type> PropertyInfos = new Dictionary<string, Type>
        {
            {"age", typeof(int)},
            {"name", typeof(string)},
            {"lang", typeof(string)},
            {"weight", typeof(float)},
            {"foo", typeof(object)}, // used when for invalid property key lookups
            {"friendWeight", typeof(float)},  // used in an AddVertex.feature test
            {"performances", typeof(int)} // grateful dead graph
        };
        
        /// <summary>
        /// Gets the type argument information based on the modern graph information.
        /// </summary>s
        public static Type GetTypeArguments(MethodInfo method, object[] parameterValues, int genericTypeIndex)
        {
            var isGeneric = method.DeclaringType.GetTypeInfo().IsGenericType;
            if (!isGeneric)
            {
                // Maintain object for anonymous traversal
                return typeof(object);
            }
            switch (method.Name)
            {
                case nameof(GraphTraversal<object,object>.Properties):
                case nameof(GraphTraversal<object,object>.Values) when parameterValues.Length == 1:
                    // The parameter contains the element property names
                    var properties = ((IEnumerable) parameterValues[parameterValues.Length - 1]).Cast<string>();
                    var types = properties.Select(GetElementPropertyType).ToArray();
                    if (types.Distinct().Count() == 1)
                    {
                        return types[0];
                    }
                    return typeof(object);
                case nameof(GraphTraversal<object,object>.Group) when genericTypeIndex == 0:
                    // Use IDictionary<string, object> for Group
                    return typeof(string);
                case nameof(GraphTraversal<object,object>.Sum):
                    return typeof(long);
                case nameof(GraphTraversal<object,object>.Limit):
                case nameof(GraphTraversal<object,object>.Optional):
                case nameof(GraphTraversal<object,object>.Coalesce):
                case nameof(GraphTraversal<object,object>.Match):
                    // Maintain the same type
                    return method.DeclaringType.GetGenericArguments()[1];
                default:
                    // default to object for this methods
                    return typeof(object);
            }
        }

        private static Type GetElementPropertyType(string name)
        {
            PropertyInfos.TryGetValue(name, out var type);
            return type;
        }
    }
}