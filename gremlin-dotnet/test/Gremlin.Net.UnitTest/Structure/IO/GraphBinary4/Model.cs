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
using System.Linq;
using System.Numerics;
using Gremlin.Net.Process.Traversal;
using Gremlin.Net.Structure;

namespace Gremlin.Net.UnitTest.Structure.IO.GraphBinary4
{
    /// <summary>
    /// Defines the supported types for GraphBinary 4.0 IO and provides test entries for round-trip testing.
    /// 
    /// The following models aren't supported:
    /// max-offsetdatetime        DateTimeOffset.MaxValue exceeds serialization range
    /// min-offsetdatetime        DateTimeOffset.MinValue exceeds serialization range
    /// forever-duration          TimeSpan cannot represent Duration.FOREVER
    /// pos-bigdecimal            Java BigDecimal precision (33 digits) exceeds C# decimal (28-29 digits)
    /// neg-bigdecimal            Java BigDecimal precision (33 digits) exceeds C# decimal (28-29 digits)
    /// four-byte-char            C# char cannot represent a supplementary Unicode code point as a single scalar value
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

        // tree for g.V(10).out().tree(): v[10] -> v[11]
        private static Tree BuildTraversalTree()
        {
            var tree = new Tree();
            tree.GetOrCreateChild(new Vertex(10, "software")).GetOrCreateChild(new Vertex(11, "software"));
            return tree;
        }

        private static Tree BuildNullKeyTree()
        {
            var tree = new Tree();
            tree.GetOrCreateChild(null);
            return tree;
        }

        private static Tree BuildMixedKeyTypesTree()
        {
            var tree = new Tree();
            tree.GetOrCreateChild("name");
            tree.GetOrCreateChild(123);
            return tree;
        }

        private static Tree BuildDeepNestingTree()
        {
            var tree = new Tree();
            tree.GetOrCreateChild("root").GetOrCreateChild("branch").GetOrCreateChild("leaf");
            return tree;
        }

        public static Dictionary<string, object?> Entries { get; } = new()
        {
            // BigDecimal
            ["zero-bigdecimal"] = 0m,
            ["scale-zero-bigdecimal"] = 1234m,
            ["negative-scale-bigdecimal"] = 123400m,
            ["small-decimal-bigdecimal"] = 12.34m,

            // BigInteger
            ["pos-biginteger"] = BigInteger.Parse("123456789987654321123456789987654321"),
            ["neg-biginteger"] = BigInteger.Parse("-123456789987654321123456789987654321"),
            ["zero-biginteger"] = BigInteger.Zero,
            ["sign-boundary-pos-biginteger"] = new BigInteger(128),
            ["sign-boundary-neg-biginteger"] = new BigInteger(-129),

            // Provider-defined types
            ["uint8-primitive-pdt"] = new PrimitivePDT("Uint8", "10"),
            ["point-composite-pdt"] = new CompositePDT("Point", new Dictionary<string, object?>
            {
                { "x", 1 },
                { "y", 2 },
            }),
            
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
            ["two-byte-char"] = '\u03A9',  // Greek capital letter Omega
            ["three-byte-char"] = '\u20AC',
            
            // Null
            ["unspecified-null"] = null,
            ["null-int"] = null,
            ["null-long"] = null,
            ["null-string"] = null,
            ["null-list"] = null,
            ["null-map"] = null,
            ["null-set"] = null,
            
            // Boolean
            ["true-boolean"] = true,
            ["false-boolean"] = false,
            
            // String
            ["single-byte-string"] = "abc",
            ["mixed-string"] = "abc\u0391\u0392\u0393",  // abc + Greek letters Alpha, Beta, Gamma
            ["empty-string"] = "",
            
            // BulkSet (represented as List with duplicates)
            ["var-bulklist"] = new List<object?> { "marko", "josh", "josh" },
            ["empty-bulklist"] = new List<object?>(),
            
            // Duration (TimeSpan)
            ["zero-duration"] = TimeSpan.Zero,
            ["positive-duration"] = TimeSpan.FromSeconds(123),
            ["negative-duration"] = TimeSpan.FromSeconds(-123),
            ["nanos-duration"] = TimeSpan.FromSeconds(123) + TimeSpan.FromTicks(4567),
            
            // Edge
            ["traversal-edge"] = new Edge(13, new Vertex(1, "person"), "develops", new Vertex(10, "software"),
                new dynamic[] { SinceProperty }),
            ["no-prop-edge"] = new Edge(13, new Vertex(1, "person"), "develops", new Vertex(10, "software")),
            ["tinker-graph"] = CrewGraphFactory.Create(),
            
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
            ["ordered-string-int-map"] = new Dictionary<object, object?>
            {
                { "delta", 4 },
                { "alpha", 1 },
                { "charlie", 3 },
                { "bravo", 2 },
                { "echo", 5 },
                { "foxtrot", 6 },
            },
            
            // Path
            ["traversal-path"] = new Path(
                new List<ISet<string>> { new HashSet<string>(), new HashSet<string>(), new HashSet<string>() },
                new List<object?> { new Vertex(1, "person"), new Vertex(10, "software"), new Vertex(11, "software") }),
            ["empty-path"] = new Path(new List<ISet<string>>(), new List<object?>()),
            ["path-zero-labels"] = new Path(
                new List<ISet<string>> { new HashSet<string>() },
                new List<object?> { "marko" }),
            ["path-multiple-labels"] = new Path(
                new List<ISet<string>> { new HashSet<string> { "a", "b" } },
                new List<object?> { "marko" }),

            // Tree
            ["traversal-tree"] = BuildTraversalTree(),
            ["empty-tree"] = new Tree(),
            ["tree-null-key"] = BuildNullKeyTree(),
            ["tree-mixed-key-types"] = BuildMixedKeyTypesTree(),
            ["tree-deep-nesting"] = BuildDeepNestingTree(),

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
            ["multi-label-vertex"] = new Vertex(1, "person", labels: new[] { "person", "employee" }),
            ["empty-label-vertex"] = new Vertex(1, "", labels: Array.Empty<string>()),
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

            // Merge enum
            ["merge-on-create"] = Merge.OnCreate,
            ["merge-on-match"] = Merge.OnMatch,
            ["merge-out-v"] = Merge.OutV,
            ["merge-in-v"] = Merge.InV,
        };

        private static class CrewGraphFactory
        {
            public static Graph Create()
            {
                var graph = new Graph();
                var v1 = AddVertex(graph, 1, "person",
                    Vp(0L, "name", "marko"),
                    Vp(6L, "location", "san diego", new Property("startTime", 1997, null),
                        new Property("endTime", 2001, null)),
                    Vp(7L, "location", "santa cruz", new Property("startTime", 2001, null),
                        new Property("endTime", 2004, null)),
                    Vp(8L, "location", "brussels", new Property("startTime", 2004, null),
                        new Property("endTime", 2005, null)),
                    Vp(9L, "location", "santa fe", new Property("startTime", 2005, null)));
                var v7 = AddVertex(graph, 7, "person",
                    Vp(1L, "name", "stephen"),
                    Vp(10L, "location", "centreville", new Property("startTime", 1990, null),
                        new Property("endTime", 2000, null)),
                    Vp(11L, "location", "dulles", new Property("startTime", 2000, null),
                        new Property("endTime", 2006, null)),
                    Vp(12L, "location", "purcellville", new Property("startTime", 2006, null)));
                var v8 = AddVertex(graph, 8, "person",
                    Vp(2L, "name", "matthias"),
                    Vp(13L, "location", "bremen", new Property("startTime", 2004, null),
                        new Property("endTime", 2007, null)),
                    Vp(14L, "location", "baltimore", new Property("startTime", 2007, null),
                        new Property("endTime", 2011, null)),
                    Vp(15L, "location", "oakland", new Property("startTime", 2011, null),
                        new Property("endTime", 2014, null)),
                    Vp(16L, "location", "seattle", new Property("startTime", 2014, null)));
                var v9 = AddVertex(graph, 9, "person",
                    Vp(3L, "name", "daniel"),
                    Vp(17L, "location", "spremberg", new Property("startTime", 1982, null),
                        new Property("endTime", 2005, null)),
                    Vp(18L, "location", "kaiserslautern", new Property("startTime", 2005, null),
                        new Property("endTime", 2009, null)),
                    Vp(19L, "location", "aachen", new Property("startTime", 2009, null)));
                var v10 = AddVertex(graph, 10, "software", Vp(4L, "name", "gremlin"));
                var v11 = AddVertex(graph, 11, "software", Vp(5L, "name", "tinkergraph"));

                AddEdge(graph, 13, v1, "develops", v10, new Property("since", 2009, null));
                AddEdge(graph, 14, v1, "develops", v11, new Property("since", 2010, null));
                AddEdge(graph, 15, v1, "uses", v10, new Property("skill", 4, null));
                AddEdge(graph, 16, v1, "uses", v11, new Property("skill", 5, null));
                AddEdge(graph, 17, v7, "develops", v10, new Property("since", 2010, null));
                AddEdge(graph, 18, v7, "develops", v11, new Property("since", 2011, null));
                AddEdge(graph, 19, v7, "uses", v10, new Property("skill", 5, null));
                AddEdge(graph, 20, v7, "uses", v11, new Property("skill", 4, null));
                AddEdge(graph, 21, v8, "develops", v10, new Property("since", 2012, null));
                AddEdge(graph, 22, v8, "uses", v10, new Property("skill", 3, null));
                AddEdge(graph, 23, v8, "uses", v11, new Property("skill", 3, null));
                AddEdge(graph, 24, v9, "uses", v10, new Property("skill", 5, null));
                AddEdge(graph, 25, v9, "uses", v11, new Property("skill", 3, null));
                AddEdge(graph, 26, v10, "traverses", v11);

                return graph;
            }

            private static VertexProperty Vp(object id, string label, object? value, params Property[] metaProperties)
            {
                return new VertexProperty(id, label, value, null, metaProperties.Cast<dynamic>().ToArray());
            }

            private static Vertex AddVertex(Graph graph, object id, string label,
                params VertexProperty[] vertexProperties)
            {
                var vertex = new Vertex(id, label, vertexProperties.Cast<dynamic>().ToArray());
                graph.Vertices[id] = vertex;
                return vertex;
            }

            private static Edge AddEdge(Graph graph, object id, Vertex outV, string label, Vertex inV,
                params Property[] properties)
            {
                var edge = new Edge(id, outV, label, inV, properties.Cast<dynamic>().ToArray());
                graph.Edges[id] = edge;
                return edge;
            }
        }
    }
}
