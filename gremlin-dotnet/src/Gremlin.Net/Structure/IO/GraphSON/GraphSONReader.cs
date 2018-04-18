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
using Newtonsoft.Json.Linq;

namespace Gremlin.Net.Structure.IO.GraphSON
{
    /// <summary>
    ///     Allows to deserialize GraphSON to objects.
    /// </summary>
    public class GraphSONReader
    {
        private readonly Dictionary<string, IGraphSONDeserializer> _deserializerByGraphSONType = new Dictionary
            <string, IGraphSONDeserializer>
            {
                {"g:Traverser", new TraverserReader()},
                {"g:Int32", new Int32Converter()},
                {"g:Int64", new Int64Converter()},
                {"g:Float", new FloatConverter()},
                {"g:Double", new DoubleConverter()},
                {"g:UUID", new UuidDeserializer()},
                {"g:Date", new DateSerializer()},
                {"g:Timestamp", new DateSerializer()},
                {"g:Vertex", new VertexDeserializer()},
                {"g:Edge", new EdgeDeserializer()},
                {"g:Property", new PropertyDeserializer()},
                {"g:VertexProperty", new VertexPropertyDeserializer()},
                {"g:Path", new PathDeserializer()},

                //Extended
                {"gx:BigDecimal", new DecimalConverter()},
                {"gx:Duration", new TimeSpanConverter()}
            };

        /// <summary>
        ///     Initializes a new instance of the <see cref="GraphSONReader" /> class.
        /// </summary>
        public GraphSONReader()
        {
        }

        /// <summary>
        ///     Initializes a new instance of the <see cref="GraphSONReader" /> class.
        /// </summary>
        /// <param name="deserializerByGraphSONType">
        ///     <see cref="IGraphSONDeserializer" /> deserializers identified by their
        ///     GraphSON type.
        /// </param>
        public GraphSONReader(IReadOnlyDictionary<string, IGraphSONDeserializer> deserializerByGraphSONType)
        {
            foreach (var deserializerAndGraphSONType in deserializerByGraphSONType)
                _deserializerByGraphSONType[deserializerAndGraphSONType.Key] = deserializerAndGraphSONType.Value;
        }

        /// <summary>
        ///     Deserializes a GraphSON collection to an object.
        /// </summary>
        /// <param name="graphSonData">The GraphSON collection to deserialize.</param>
        /// <returns>The deserialized object.</returns>
        public dynamic ToObject(IEnumerable<JToken> graphSonData)
        {
            return graphSonData.Select(graphson => ToObject(graphson));
        }

        /// <summary>
        ///     Deserializes GraphSON to an object.
        /// </summary>
        /// <param name="jToken">The GraphSON to deserialize.</param>
        /// <returns>The deserialized object.</returns>
        public dynamic ToObject(JToken jToken)
        {
            if (jToken is JArray)
            {
                return jToken.Select(t => ToObject(t));
            }
            if (jToken is JValue jValue)
            {
                return jValue.Value;
            }
            if (!HasTypeKey(jToken))
            {
                return ReadDictionary(jToken);
            }
            return ReadTypedValue(jToken);
        }

        private bool HasTypeKey(JToken jToken)
        {
            var graphSONType = (string) jToken[GraphSONTokens.TypeKey];
            return graphSONType != null;
        }

        private dynamic ReadTypedValue(JToken typedValue)
        {
            var graphSONType = (string) typedValue[GraphSONTokens.TypeKey];
            if (!_deserializerByGraphSONType.TryGetValue(graphSONType, out var deserializer))
            {
                throw new InvalidOperationException($"Deserializer for \"{graphSONType}\" not found");
            }
            return deserializer.Objectify(typedValue[GraphSONTokens.ValueKey], this);
        }

        private dynamic ReadDictionary(JToken jtokenDict)
        {
            var dict = new Dictionary<string, dynamic>();
            foreach (var e in jtokenDict)
            {
                var property = e as JProperty;
                if (property == null)
                    throw new InvalidOperationException($"Cannot read graphson: {jtokenDict}");
                dict.Add(property.Name, ToObject(property.Value));
            }
            return dict;
        }
    }
}