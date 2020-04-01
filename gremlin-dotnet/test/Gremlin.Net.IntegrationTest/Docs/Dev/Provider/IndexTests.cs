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
using System.Text.Json;
using Gremlin.Net.Driver;
using Gremlin.Net.Structure.IO.GraphSON;
using Xunit;
using Xunit.Sdk;

namespace Gremlin.Net.IntegrationTest.Docs.Dev.Provider
{
// tag::myTypeSerialization[]
internal class MyType
{
    public static string GraphsonPrefix = "providerx";
    public static string GraphsonBaseType = "MyType";
    public static string GraphsonType = GraphSONUtil.FormatTypeName(GraphsonPrefix, GraphsonBaseType);

    public MyType(int x, int y)
    {
        X = x;
        Y = y;
    }

    public int X { get; }
    public int Y { get; }
}

internal class MyClassWriter : IGraphSONSerializer
{
    public Dictionary<string, dynamic> Dictify(dynamic objectData, GraphSONWriter writer)
    {
        MyType myType = objectData;
        var valueDict = new Dictionary<string, object>
        {
            {"x", myType.X},
            {"y", myType.Y}
        };
        return GraphSONUtil.ToTypedValue(nameof(TestClass), valueDict, MyType.GraphsonPrefix);
    }
}

internal class MyTypeReader : IGraphSONDeserializer
{
    public dynamic Objectify(JsonElement graphsonObject, GraphSONReader reader)
    {
        var x = reader.ToObject(graphsonObject.GetProperty("x"));
        var y = reader.ToObject(graphsonObject.GetProperty("y"));
        return new MyType(x, y);
    }
}
// end::myTypeSerialization[]
    
    public class IndexTests
    {
        [Fact(Skip="No Server under localhost")]
        public void SupportingGremlinNetIOTests()
        {
// tag::supportingGremlinNetIO[]
var graphsonReader = new GraphSON3Reader(
    new Dictionary<string, IGraphSONDeserializer> {{MyType.GraphsonType, new MyTypeReader()}});
var graphsonWriter = new GraphSON3Writer(
    new Dictionary<Type, IGraphSONSerializer> {{typeof(MyType), new MyClassWriter()}});

var gremlinClient = new GremlinClient(new GremlinServer("localhost", 8182), graphsonReader, graphsonWriter);
// end::supportingGremlinNetIO[]
        }
    }
}