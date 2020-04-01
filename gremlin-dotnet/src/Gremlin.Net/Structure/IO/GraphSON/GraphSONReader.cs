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
using System.Text.Json;

namespace Gremlin.Net.Structure.IO.GraphSON
{
    /// <summary>
    ///     Allows to deserialize GraphSON to objects.
    /// </summary>
    public abstract class GraphSONReader
    {
        /// <summary>
        /// Contains the <see cref="IGraphSONDeserializer" /> instances by their type identifier. 
        /// </summary>
        protected readonly Dictionary<string, IGraphSONDeserializer> Deserializers = new Dictionary
            <string, IGraphSONDeserializer>
            {
                {"g:Traverser", new TraverserReader()},
                {"g:Int32", new Int32Converter()},
                {"g:Int64", new Int64Converter()},
                {"g:Float", new FloatConverter()},
                {"g:Double", new DoubleConverter()},
                {"g:Direction", new DirectionDeserializer()},
                {"g:UUID", new UuidDeserializer()},
                {"g:Date", new DateDeserializer()},
                {"g:Timestamp", new DateDeserializer()},
                {"g:Vertex", new VertexDeserializer()},
                {"g:Edge", new EdgeDeserializer()},
                {"g:Property", new PropertyDeserializer()},
                {"g:VertexProperty", new VertexPropertyDeserializer()},
                {"g:Path", new PathDeserializer()},
                {"g:T", new TDeserializer()},

                //Extended
                {"gx:BigDecimal", new DecimalConverter()},
                {"gx:Duration", new DurationDeserializer()},
                {"gx:BigInteger", new BigIntegerDeserializer()},
                {"gx:Byte", new ByteConverter()},
                {"gx:ByteBuffer", new ByteBufferDeserializer()},
                {"gx:Char", new CharConverter()},
                {"gx:Int16", new Int16Converter() }
            };

        /// <summary>
        ///     Initializes a new instance of the <see cref="GraphSONReader" /> class.
        /// </summary>
        protected GraphSONReader()
        {
        }

        /// <summary>
        ///     Initializes a new instance of the <see cref="GraphSONReader" /> class.
        /// </summary>
        /// <param name="deserializerByGraphSONType">
        ///     <see cref="IGraphSONDeserializer" /> deserializers identified by their
        ///     GraphSON type.
        /// </param>
        protected GraphSONReader(IReadOnlyDictionary<string, IGraphSONDeserializer> deserializerByGraphSONType)
        {
            foreach (var deserializerAndGraphSONType in deserializerByGraphSONType)
                Deserializers[deserializerAndGraphSONType.Key] = deserializerAndGraphSONType.Value;
        }

        /// <summary>
        ///     Deserializes a GraphSON collection to an object.
        /// </summary>
        /// <param name="graphSonData">The GraphSON collection to deserialize.</param>
        /// <returns>The deserialized object.</returns>
        public virtual dynamic ToObject(IEnumerable<JsonElement> graphSonData)
        {
            return graphSonData.Select(graphson => ToObject(graphson));
        }

        /// <summary>
        ///     Deserializes GraphSON to an object.
        /// </summary>
        /// <param name="graphSon">The GraphSON to deserialize.</param>
        /// <returns>The deserialized object.</returns>
        public virtual dynamic ToObject(JsonElement graphSon)
        {
            switch (graphSon.ValueKind)
            {
                case JsonValueKind.Array:
                    return graphSon.EnumerateArray().Select(t => ToObject(t));
                case JsonValueKind.String:
                    return graphSon.GetString();
                case JsonValueKind.Null:
                    return null;
                case JsonValueKind.True:
                    return true;
                case JsonValueKind.False:
                    return false;
                case JsonValueKind.Object:
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(graphSon.ValueKind), graphSon.ValueKind,
                        $"JSON type not supported.");
            }

            if (graphSon.TryGetProperty(GraphSONTokens.TypeKey, out var graphSonTypeProperty))
            {
                return ReadValueOfType(graphSon, graphSonTypeProperty.GetString());
            }
            return ReadDictionary(graphSon);
            
        }

        private dynamic ReadValueOfType(JsonElement typedValue, string graphSONType)
        {
            if (!Deserializers.TryGetValue(graphSONType, out var deserializer))
            {
                throw new InvalidOperationException($"Deserializer for \"{graphSONType}\" not found");
            }
            return deserializer.Objectify(typedValue.GetProperty(GraphSONTokens.ValueKey), this);
        }
        
        private dynamic ReadDictionary(JsonElement jsonDict)
        {
            return jsonDict.EnumerateObject()
                .ToDictionary(property => property.Name, property => ToObject(property.Value));
        }
    }
}