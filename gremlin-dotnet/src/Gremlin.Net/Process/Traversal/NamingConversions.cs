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

namespace Gremlin.Net.Process.Traversal
{
    internal static class NamingConversions
    {
        /// <summary>
        /// Gets the Java name equivalent for a given enum value
        /// </summary>
        internal static string GetEnumJavaName(string typeName, string value)
        {
            var key = $"{typeName}.{value}";
            string javaName;
            if (!CSharpToJavaEnums.TryGetValue(key, out javaName))
            {
                throw new KeyNotFoundException($"Java name for {key} not found");
            }
            return javaName;
        }

        private static readonly IDictionary<string, string> CSharpToJavaEnums = new Dictionary<string, string>
        {
            {"T.Value", "value"},
            {"Order.Decr", "decr"},
            {"Order.KeyDecr", "keyDecr"},
            {"T.Key", "key"},
            {"Column.Values", "values"},
            {"Order.KeyIncr", "keyIncr"},
            {"Operator.Or", "or"},
            {"Order.ValueIncr", "valueIncr"},
            {"Cardinality.List", "list"},
            {"Order.Incr", "incr"},
            {"Pop.All", "all"},
            {"Operator.SumLong", "sumLong"},
            {"Pop.First", "first"},
            {"T.Label", "label"},
            {"Cardinality.Set", "set"},
            {"Order.Shuffle", "shuffle"},
            {"Direction.In", "IN"},
            {"Direction.Both", "BOTH"},
            {"Scope.Local", "local"},
            {"Operator.Max", "max"},
            {"Direction.Out", "OUT"},
            {"Scope.Global", "global"},
            {"Pick.Any", "any"},
            {"Order.ValueDecr", "valueDecr"},
            {"Column.Keys", "keys"},
            {"Operator.AddAll", "addAll"},
            {"Operator.Mult", "mult"},
            {"Pick.None", "none"},
            {"Pop.Last", "last"},
            {"Operator.And", "and"},
            {"T.Id", "id"},
            {"Operator.Min", "min"},
            {"Barrier.NormSack", "normSack"},
            {"Operator.Minus", "minus"},
            {"Cardinality.Single", "single"},
            {"Operator.Assign", "assign"},
            {"Operator.Div", "div"},
            {"Operator.Sum", "sum"}
        };
    }
}