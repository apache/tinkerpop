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

    public class GraphSONVersion : EnumWrapper
    {
        private GraphSONVersion(string enumValue)
            : base("GraphSONVersion", enumValue)
        {
        }

        public static GraphSONVersion V1_0 => new GraphSONVersion("V1_0");

        public static GraphSONVersion V2_0 => new GraphSONVersion("V2_0");

        public static GraphSONVersion V3_0 => new GraphSONVersion("V3_0");

        private static readonly IDictionary<string, GraphSONVersion> Properties = new Dictionary<string, GraphSONVersion>
        {
            { "V1_0", V1_0 },
            { "V2_0", V2_0 },
            { "V3_0", V3_0 },
        };

        /// <summary>
        /// Gets the GraphSONVersion enumeration by value.
        /// </summary>
        public static GraphSONVersion GetByValue(string value)
        {
            if (!Properties.TryGetValue(value, out var property))
            {
                throw new ArgumentException($"No matching GraphSONVersion for value '{value}'");
            }
            return property;
        }
    }


#pragma warning restore 1591
}