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
using System.Numerics;
using Gremlin.Net.Process.Traversal;
using Gremlin.Net.Structure;

namespace Gremlin.Net.UnitTest.Structure.IO.GraphBinary4
{
    /// <summary>
    /// Defines the supported types for GraphBinary 4.0 IO and provides test entries for round-trip testing.
    /// 
    /// The following models aren't supported:
    /// tinker-graph              Graph type not implemented
    /// traversal-tree            Tree type not implemented  
    /// max-offsetdatetime        DateTimeOffset.MaxValue exceeds serialization range
    /// min-offsetdatetime        DateTimeOffset.MinValue exceeds serialization range
    /// forever-duration          TimeSpan cannot represent Duration.FOREVER
    /// pos-bigdecimal            Java BigDecimal precision (33 digits) exceeds C# decimal (28-29 digits)
    /// neg-bigdecimal            Java BigDecimal precision (33 digits) exceeds C# decimal (28-29 digits)
    /// var-type-map              Dictionary doesn't support null key
    /// </summary>
    public static class Model
    {
        private static readonly Property SinceProperty = new("since", 2009, null);
        private static readonly Property StartTime2005 = new("startTime", 2005, null);
        private static readonly Property EndTime2005 = new("endTime", 2005, null);
        private static readonly Property StartTime2001 = new("startTime", 2001, null);
        private static readonly Property EndTime2004 = new("endTime", 2004, null);
        private static readonly Property StartTime1997 = new("startTime", 1997, null);
        private static readonly Property EndTime2001 = new("endTime", 2001, null);
        private static readonly Property PropertyAB = new("a", "b", null);
        
        private static readonly VertexProperty SantaFe = new(9, "location", "santa fe", null, 
            new dynamic[] { new Property("startTime", 2005, null) });
        private static readonly VertexProperty Brussels = new(8, "location", "brussels", null,
            new dynamic[] { StartTime2005, EndTime2005 });
        private static readonly VertexProperty SantaCruz = new(7, "location", "santa cruz", null,
            new dynamic[] { StartTime2001, EndTime2004 });
        private static readonly VertexProperty SanDiego = new(6, "location", "san diego", null,
            new dynamic[] { StartTime1997, EndTime2001 });
        private static readonly VertexProperty NameMarko = new(0, "name", "marko", null);

        public static Dictionary<string, object?> Entries { get; } = new()
        {
            // BigInteger
            ["pos-biginteger"] = BigInteger.Parse("123456789987654321123456789987654321"),
            ["neg-biginteger"] = BigInteger.Parse("-123456789987654321123456789987654321"),
            
            // Byte (sbyte in C#)
            ["min-byte"] = sbyte.MinValue,  // -128
            ["max-byte"] = sbyte.MaxValue,  // 127
            
            // Binary (byte[])
            ["empty-binary"] = Array.Empty<byte>(),
            ["str-binary"] = System.Text.Encoding.UTF8.GetBytes("some bytes for you"),
            
            // Double
            ["max-double"] = double.MaxValue,
            ["min-double"] = double.Epsilon,
            ["neg-max-double"] = -double.MaxValue,
            ["neg-min-double"] = -double.Epsilon,
            ["nan-double"] = double.NaN,
            ["pos-inf-double"] = double.PositiveInfinity,
            ["neg-inf-double"] = double.NegativeInfinity,
            ["neg-zero-double"] = -0.0,
            
            // Float
            // Note: Java's Float.MIN_VALUE is the smallest positive float (equivalent to C#'s float.Epsilon),
            // not the most negative float.
            ["max-float"] = float.MaxValue,
            ["min-float"] = float.Epsilon,       // Java Float.MIN_VALUE = smallest positive float
            ["neg-max-float"] = -float.MaxValue,
            ["neg-min-float"] = -float.Epsilon,  // negated Java Float.MIN_VALUE
            ["nan-float"] = float.NaN,
            ["pos-inf-float"] = float.PositiveInfinity,
            ["neg-inf-float"] = float.NegativeInfinity,
            ["neg-zero-float"] = -0.0f,
            
            // Char
            ["single-byte-char"] = 'a',
            ["multi-byte-char"] = '\u03A9',  // Greek capital letter Omega
            
            // Null
            ["unspecified-null"] = null,
            
            // Boolean
            ["true-boolean"] = true,
            ["false-boolean"] = false,
            
            // String
            ["single-byte-string"] = "abc",
            ["mixed-string"] = "abc\u0391\u0392\u0393",  // abc + Greek letters Alpha, Beta, Gamma
            
            // BulkSet (represented as List with duplicates)
            ["var-bulklist"] = new List<object?> { "marko", "josh", "josh" },
            ["empty-bulklist"] = new List<object?>(),
            
            // Duration (TimeSpan)
            ["zero-duration"] = TimeSpan.Zero,
            
            // Edge
            ["traversal-edge"] = new Edge(13, new Vertex(1, "person"), "develops", new Vertex(10, "software"),
                new dynamic[] { SinceProperty }),
            ["no-prop-edge"] = new Edge(13, new Vertex(1, "person"), "develops", new Vertex(10, "software")),
            
            // Int
            ["max-int"] = int.MaxValue,
            ["min-int"] = int.MinValue,
            
            // Long
            ["max-long"] = long.MaxValue,
            ["min-long"] = long.MinValue,
            
            // List
            ["var-type-list"] = new List<object?> { 1, "person", true, null },
            ["empty-list"] = new List<object?>(),
            
            // Map
            ["var-type-map"] = new Dictionary<object, object?>
            {
                { "test", 123 },
                { DateTimeOffset.FromUnixTimeMilliseconds(1481295), "red" },
                { new List<object?> { 1, 2, 3 }, DateTimeOffset.FromUnixTimeMilliseconds(1481295) },
                // Note: null key not supported in C# Dictionary
            },
            ["empty-map"] = new Dictionary<object, object?>(),
            
            // Path
            ["traversal-path"] = new Path(
                new List<ISet<string>> { new HashSet<string>(), new HashSet<string>(), new HashSet<string>() },
                new List<object?> { new Vertex(1, "person"), new Vertex(10, "software"), new Vertex(11, "software") }),
            ["empty-path"] = new Path(new List<ISet<string>>(), new List<object?>()),
            ["prop-path"] = new Path(
                new List<ISet<string>> { new HashSet<string>(), new HashSet<string>(), new HashSet<string>() },
                new List<object?>
                {
                    new Vertex(1, "person", new dynamic[]
                    {
                        new VertexProperty(123, "name", "stephen", null, new dynamic[]
                        {
                            new VertexProperty(0, "name", "marko", null),
                            new VertexProperty(6, "location", new List<object?>(), null)
                        })
                    }),
                    new Vertex(10, "software"),
                    new Vertex(11, "software")
                }),
            
            // Property
            ["edge-property"] = new Property("since", 2009, null),
            ["null-property"] = new Property("", null, null),
            
            // Set
            ["var-type-set"] = new HashSet<object?> { 2, "person", true, null },
            ["empty-set"] = new HashSet<object?>(),
            
            // Short
            ["max-short"] = short.MaxValue,
            ["min-short"] = short.MinValue,
            
            // UUID (Guid)
            ["specified-uuid"] = Guid.Parse("41d2e28a-20a4-4ab0-b379-d810dede3786"),
            ["nil-uuid"] = Guid.Empty,
            
            // Vertex
            ["no-prop-vertex"] = new Vertex(1, "person"),
            ["traversal-vertex"] = new Vertex(1, "person", new dynamic[]
            {
                new Property("name", NameMarko, null),
                new Property("location", new List<object?> { SanDiego, SantaCruz, Brussels, SantaFe }, null)
            }),
            
            // VertexProperty
            ["traversal-vertexproperty"] = new VertexProperty(0L, "name", "marko", null),
            ["meta-vertexproperty"] = new VertexProperty(1, "person", "stephen", null, 
                new dynamic[] { PropertyAB }),
            ["set-cardinality-vertexproperty"] = new VertexProperty(1, "person", 
                new HashSet<object?> { "stephen", "marko" }, null, new dynamic[] { PropertyAB }),
            
            // T enum
            ["id-t"] = T.Id,
            
            // Direction enum
            ["out-direction"] = Direction.Out,
        };
    }
}
