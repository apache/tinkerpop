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
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Gremlin.Net.Driver.Messages;
using Gremlin.Net.Process.Traversal;
using Gremlin.Net.Process.Traversal.Strategy;
using Newtonsoft.Json;

namespace Gremlin.Net.Structure.IO.GraphSON
{
    /// <summary>
    ///     Allows to serialize objects to GraphSON.
    /// </summary>
    public class GraphSONWriter
    {
        private readonly Dictionary<Type, IGraphSONSerializer> _serializerByType = new Dictionary
            <Type, IGraphSONSerializer>
            {
                {typeof(ITraversal), new TraversalSerializer()},
                {typeof(Bytecode), new BytecodeSerializer()},
                {typeof(Binding), new BindingSerializer()},
                {typeof(RequestMessage), new RequestMessageSerializer()},
                {typeof(int), new Int32Converter()},
                {typeof(long), new Int64Converter()},
                {typeof(float), new FloatConverter()},
                {typeof(double), new DoubleConverter()},
                {typeof(decimal), new DecimalConverter()},
                {typeof(Guid), new UuidSerializer()},
                {typeof(DateTimeOffset), new DateSerializer()},
                {typeof(Type), new ClassSerializer()},
                {typeof(EnumWrapper), new EnumSerializer()},
                {typeof(TraversalPredicate), new TraversalPredicateSerializer()},
                {typeof(Vertex), new VertexSerializer()},
                {typeof(Edge), new EdgeSerializer()},
                {typeof(Property), new PropertySerializer()},
                {typeof(VertexProperty), new VertexPropertySerializer()},
                {typeof(AbstractTraversalStrategy), new TraversalStrategySerializer()}
            };

        /// <summary>
        ///     Initializes a new instance of the <see cref="GraphSONWriter" /> class.
        /// </summary>
        public GraphSONWriter()
        {
        }

        /// <summary>
        ///     Initializes a new instance of the <see cref="GraphSONWriter" /> class.
        /// </summary>
        /// <param name="customSerializerByType">
        ///     <see cref="IGraphSONSerializer" /> serializers identified by their
        ///     <see cref="Type" />.
        /// </param>
        public GraphSONWriter(IReadOnlyDictionary<Type, IGraphSONSerializer> customSerializerByType)
        {
            foreach (var serializerAndType in customSerializerByType)
                _serializerByType[serializerAndType.Key] = serializerAndType.Value;
        }

        /// <summary>
        ///     Serializes an object to GraphSON.
        /// </summary>
        /// <param name="objectData">The object to serialize.</param>
        /// <returns>The serialized GraphSON.</returns>
        public string WriteObject(dynamic objectData)
        {
            return JsonConvert.SerializeObject(ToDict(objectData));
        }

        internal dynamic ToDict(dynamic objectData)
        {
            var type = objectData.GetType();
            if (TryGetSerializerFor(out IGraphSONSerializer serializer, type))
                return serializer.Dictify(objectData, this);
            if (IsDictionaryType(type))
                return DictToGraphSONDict(objectData);
            if (IsCollectionType(type))
                return CollectionToGraphSONCollection(objectData);
            return objectData;
        }

        private bool TryGetSerializerFor(out IGraphSONSerializer serializer, Type type)
        {
            if (_serializerByType.ContainsKey(type))
            {
                serializer = _serializerByType[type];
                return true;
            }
            foreach (var supportedType in _serializerByType.Keys)
                if (supportedType.IsAssignableFrom(type))
                {
                    serializer = _serializerByType[supportedType];
                    return true;
                }
            serializer = null;
            return false;
        }

        private bool IsDictionaryType(Type type)
        {
            return type.IsConstructedGenericType && type.GetGenericTypeDefinition() == typeof(Dictionary<,>);
        }

        private Dictionary<string, dynamic> DictToGraphSONDict(dynamic dict)
        {
            var graphSONDict = new Dictionary<string, dynamic>();
            foreach (var keyValue in dict)
                graphSONDict.Add(ToDict(keyValue.Key), ToDict(keyValue.Value));
            return graphSONDict;
        }

        private bool IsCollectionType(Type type)
        {
            return type.GetInterfaces().Contains(typeof(ICollection));
        }

        private IEnumerable<dynamic> CollectionToGraphSONCollection(dynamic collection)
        {
            foreach (var e in collection)
                yield return ToDict(e);
        }
    }
}