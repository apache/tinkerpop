﻿#region License

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

using System.Linq;
using System.Text.Json;

namespace Gremlin.Net.Structure.IO.GraphSON
{
    internal class EdgeDeserializer : IGraphSONDeserializer
    {
        public dynamic Objectify(JsonElement graphsonObject, GraphSONReader reader)
        {
            var outVId = reader.ToObject(graphsonObject.GetProperty("outV"));
            string outVLabel = graphsonObject.TryGetProperty("outVLabel", out var outVLabelProperty)
                ? outVLabelProperty.GetString()!
                : Vertex.DefaultLabel;
            var outV = new Vertex(outVId, outVLabel);
            var inVId = reader.ToObject(graphsonObject.GetProperty("inV"));
            string inVLabel = graphsonObject.TryGetProperty("inVLabel", out var inVLabelProperty)
                ? inVLabelProperty.GetString()!
                : Vertex.DefaultLabel;
            var inV = new Vertex(inVId, inVLabel);
            var edgeId = reader.ToObject(graphsonObject.GetProperty("id"));
            string edgeLabel = graphsonObject.TryGetProperty("label", out var labelProperty)
                ? labelProperty.GetString()!
                : "edge";

            dynamic[]? properties = null;
            if (graphsonObject.TryGetProperty("properties", out var propertiesObject)
                && propertiesObject.ValueKind == JsonValueKind.Object)
            {
                properties = propertiesObject.EnumerateObject()
                    .Select(p => reader.ToObject(p.Value)!).ToArray();
            }

            return new Edge(edgeId, outV, edgeLabel, inV, properties);
        }
    }
}