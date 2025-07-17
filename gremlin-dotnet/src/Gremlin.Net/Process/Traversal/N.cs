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

        public static N NByte => new N("nbyte");
        public static N NShort => new N("nshort");
        public static N NInt => new N("nint");
        public static N NLong => new N("nlong");
        public static N NFloat => new N("nfloat");
        public static N NDouble => new N("ndouble");
        public static N NBigInt => new N("nbigInt");
        public static N NBigDecimal => new N("nbigDecimal");

        private static readonly Dictionary<string, N> Properties = new()
        {
            { "nbyte", NByte },
            { "nshort", NShort },
            { "nint", NInt },
            { "nlong", NLong },
            { "nfloat", NFloat },
            { "ndouble", NDouble },
            { "nbigInt", NBigInt },
            { "nbigDecimal", NBigDecimal },
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