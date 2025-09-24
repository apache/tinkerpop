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

    public class GType : EnumWrapper, IFunction
    {
        private GType(string enumValue)
            : base("GType", enumValue)
        {
        }

        public static GType BigDecimal => new GType("BIGDECIMAL");
        public static GType BigInt => new GType("BIGINT");
        public static GType Binary => new GType("BINARY");
        public static GType Boolean => new GType("BOOLEAN");
        public static GType Byte => new GType("BYTE");
        public static GType Char => new GType("CHAR");
        public static GType DateTime => new GType("DATETIME");
        public static GType Double => new GType("DOUBLE");
        public static GType Duration => new GType("DURATION");
        public static GType Edge => new GType("EDGE");
        public static GType Float => new GType("FLOAT");
        public static GType Graph => new GType("GRAPH");
        public static GType Int => new GType("INT");
        public static GType List => new GType("LIST");
        public static GType Long => new GType("LONG");
        public static GType Map => new GType("MAP");
        public static GType Null => new GType("NULL");
        public static GType Number => new GType("NUMBER");
        public static GType Path => new GType("PATH");
        public static GType Property => new GType("PROPERTY");
        public static GType Set => new GType("SET");
        public static GType Short => new GType("SHORT");
        public static GType String => new GType("STRING");
        public static GType Tree => new GType("TREE");
        public static GType UUID => new GType("UUID");
        public static GType Vertex => new GType("VERTEX");
        public static GType VP => new GType("VP");

        private static readonly Dictionary<string, GType> Properties = new()
        {
            { "BIGDECIMAL", BigDecimal },
            { "BIGINT", BigInt },
            { "BINARY", Binary },
            { "BOOLEAN", Boolean },
            { "BYTE", Byte },
            { "CHAR", Char },
            { "DATETIME", DateTime },
            { "DOUBLE", Double },
            { "DURATION", Duration },
            { "EDGE", Edge },
            { "FLOAT", Float },
            { "GRAPH", Graph },
            { "INT", Int },
            { "LIST", List },
            { "LONG", Long },
            { "MAP", Map },
            { "NULL", Null },
            { "NUMBER", Number },
            { "PATH", Path },
            { "PROPERTY", Property },
            { "SET", Set },
            { "SHORT", Short },
            { "STRING", String },
            { "TREE", Tree },
            { "UUID", UUID },
            { "VERTEX", Vertex },
            { "VP", VP },
        };

        /// <summary>
        /// Gets the GType enumeration by value.
        /// </summary>
        public static GType GetByValue(string value)
        {
            if (!Properties.TryGetValue(value, out var property))
            {
                throw new ArgumentException($"No matching GType for value '{value}'");
            }
            return property;
        }
    }


#pragma warning restore 1591
}
