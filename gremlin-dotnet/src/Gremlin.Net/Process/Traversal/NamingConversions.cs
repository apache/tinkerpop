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

// THIS IS A GENERATED FILE - DO NOT MODIFY THIS FILE DIRECTLY - see pom.xml
namespace Gremlin.Net.Process.Traversal
{
    internal static class NamingConversions
    {
        /// <summary>
        ///     Gets the Java name equivalent for a given enum value
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

        internal static readonly IDictionary<string, string> CSharpToJavaEnums = new Dictionary<string, string>
        {
            {"Barrier.NormSack", "normSack"},
            {"Cardinality.List", "list"},
            {"Cardinality.Set", "set"},
            {"Cardinality.Single", "single"},
            {"Column.Keys", "keys"},
            {"Column.Values", "values"},
            {"Direction.Both", "BOTH"},
            {"Direction.In", "IN"},
            {"Direction.Out", "OUT"},
            {"GraphSONVersion.V1_0", "V1_0"},
            {"GraphSONVersion.V2_0", "V2_0"},
            {"GraphSONVersion.V3_0", "V3_0"},
            {"GryoVersion.V1_0", "V1_0"},
            {"GryoVersion.V3_0", "V3_0"},
            {"Operator.AddAll", "addAll"},
            {"Operator.And", "and"},
            {"Operator.Assign", "assign"},
            {"Operator.Div", "div"},
            {"Operator.Max", "max"},
            {"Operator.Min", "min"},
            {"Operator.Minus", "minus"},
            {"Operator.Mult", "mult"},
            {"Operator.Or", "or"},
            {"Operator.Sum", "sum"},
            {"Operator.SumLong", "sumLong"},
            {"Order.Decr", "decr"},
            {"Order.Incr", "incr"},
            {"Order.Shuffle", "shuffle"},
            {"Order.Asc", "asc"},
            {"Order.Desc", "desc"},
            {"Pick.Any", "any"},
            {"Pick.None", "none"},
            {"Pop.All", "all"},
            {"Pop.First", "first"},
            {"Pop.Last", "last"},
            {"Pop.Mixed", "mixed"},
            {"Scope.Global", "global"},
            {"Scope.Local", "local"},
            {"T.Id", "id"},
            {"T.Key", "key"},
            {"T.Label", "label"},
            {"T.Value", "value"}

        };
    }
}