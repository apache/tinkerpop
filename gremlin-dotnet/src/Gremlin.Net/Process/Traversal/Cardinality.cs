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

namespace Gremlin.Net.Process.Traversal
{
#pragma warning disable 1591

    public class Cardinality : EnumWrapper
    {
        private Cardinality(string enumValue)
            : base("Cardinality", enumValue)
        {
        }

        public static Cardinality List => new Cardinality("list");

        public static Cardinality Set => new Cardinality("set");

        public static Cardinality Single => new Cardinality("single");

        private static readonly IDictionary<string, Cardinality> Properties = new Dictionary<string, Cardinality>
        {
            { "list", List },
            { "set", Set },
            { "single", Single },
        };

        /// <summary>
        /// Gets the Cardinality enumeration by value.
        /// </summary>
        public static Cardinality GetByValue(string value)
        {
            if (!Properties.TryGetValue(value, out var property))
            {
                throw new ArgumentException($"No matching Cardinality for value '{value}'");
            }
            return property;
        }
    }


#pragma warning restore 1591
}