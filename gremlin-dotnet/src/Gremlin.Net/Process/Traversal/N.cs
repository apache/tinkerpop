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

    public class N : EnumWrapper, IFunction
    {
        private N(string enumValue)
            : base("N", enumValue)
        {
        }

        public static N Byte => new N("byte");
        public static N Short => new N("short");
        public static N Int => new N("int");
        public static N Long => new N("long");
        public static N Float => new N("float");
        public static N Double => new N("double");
        public static N BigInt => new N("bigInt");
        public static N BigDecimal => new N("bigDecimal");

        private static readonly Dictionary<string, N> Properties = new()
        {
            { "byte", Byte },
            { "short", Short },
            { "int", Int },
            { "long", Long },
            { "float", Float },
            { "double", Double },
            { "bigInt", BigInt },
            { "bigDecimal", BigDecimal },
        };

        /// <summary>
        /// Gets the Merge enumeration by value.
        /// </summary>
        public static N GetByValue(string value)
        {
            if (!Properties.TryGetValue(value, out var property))
            {
                throw new ArgumentException($"No matching N for value '{value}'");
            }
            return property;
        }
    }


#pragma warning restore 1591
}