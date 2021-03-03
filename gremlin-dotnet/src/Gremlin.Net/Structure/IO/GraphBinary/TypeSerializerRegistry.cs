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
using Gremlin.Net.Process.Traversal.Strategy;
using Gremlin.Net.Structure.IO.GraphBinary.Types;

namespace Gremlin.Net.Structure.IO.GraphBinary
{
    /// <summary>
    /// Provides GraphBinary serializers for different types.
    /// </summary>
    public class TypeSerializerRegistry
    {
        private static readonly Dictionary<Type, ITypeSerializer> SerializerByType =
            new Dictionary<Type, ITypeSerializer>
            {
                {typeof(int), SingleTypeSerializers.IntSerializer},
                {typeof(long), SingleTypeSerializers.LongSerializer},
                {typeof(string), new StringSerializer()},
                {typeof(DateTimeOffset), DateTimeOffsetSerializer.DateSerializer},
                {typeof(GremlinType), new ClassSerializer()},
                {typeof(Type), new TypeSerializer()},
                {typeof(double), SingleTypeSerializers.DoubleSerializer},
                {typeof(float), SingleTypeSerializers.FloatSerializer},
                {typeof(Guid), new UuidSerializer()},
                {typeof(Edge), new EdgeSerializer()},
                {typeof(Path), new PathSerializer()},
                {typeof(Property), new PropertySerializer()},
                {typeof(Vertex), new VertexSerializer()},
                {typeof(VertexProperty), new VertexPropertySerializer()},
                {typeof(Barrier), EnumSerializers.BarrierSerializer},
                {typeof(Binding), new BindingSerializer()},
                {typeof(Bytecode), new BytecodeSerializer()},
                {typeof(ITraversal), new TraversalSerializer()},
                {typeof(Cardinality), EnumSerializers.CardinalitySerializer},
                {typeof(Column), EnumSerializers.ColumnSerializer},
                {typeof(Direction), EnumSerializers.DirectionSerializer},
                {typeof(Operator), EnumSerializers.OperatorSerializer},
                {typeof(Order), EnumSerializers.OrderSerializer},
                {typeof(Pick), EnumSerializers.PickSerializer},
                {typeof(Pop), EnumSerializers.PopSerializer},
                {typeof(ILambda), new LambdaSerializer()},
                {typeof(P), new PSerializer(DataType.P)},
                {typeof(Scope), EnumSerializers.ScopeSerializer},
                {typeof(T), EnumSerializers.TSerializer},
                {typeof(Traverser), new TraverserSerializer()},
                {typeof(decimal), new BigDecimalSerializer()},
                {typeof(BigInteger), new BigIntegerSerializer()},
                {typeof(byte), SingleTypeSerializers.ByteSerializer},
                {typeof(byte[]), new ByteBufferSerializer()},
                {typeof(short), SingleTypeSerializers.ShortSerializer},
                {typeof(bool), SingleTypeSerializers.BooleanSerializer},
                {typeof(TextP), new PSerializer(DataType.TextP)},
                {typeof(AbstractTraversalStrategy), new TraversalStrategySerializer()},
                {typeof(char), new CharSerializer()},
                {typeof(TimeSpan), new DurationSerializer()},
            };

        private static readonly Dictionary<DataType, ITypeSerializer> SerializerByDataType =
            new Dictionary<DataType, ITypeSerializer>
            {
                {DataType.Int, SingleTypeSerializers.IntSerializer},
                {DataType.Long, SingleTypeSerializers.LongSerializer},
                {DataType.String, new StringSerializer()},
                {DataType.Date, DateTimeOffsetSerializer.DateSerializer},
                {DataType.Timestamp, DateTimeOffsetSerializer.TimestampSerializer},
                {DataType.Class, new ClassSerializer()},
                {DataType.Double, SingleTypeSerializers.DoubleSerializer},
                {DataType.Float, SingleTypeSerializers.FloatSerializer},
                {DataType.List, new ListSerializer<object>()},
                {DataType.Map, new MapSerializer<object, object>()},
                {DataType.Set, new SetSerializer<HashSet<object>, object>()},
                {DataType.Uuid, new UuidSerializer()},
                {DataType.Edge, new EdgeSerializer()},
                {DataType.Path, new PathSerializer()},
                {DataType.Property, new PropertySerializer()},
                {DataType.Vertex, new VertexSerializer()},
                {DataType.VertexProperty, new VertexPropertySerializer()},
                {DataType.Barrier, EnumSerializers.BarrierSerializer},
                {DataType.Binding, new BindingSerializer()},
                {DataType.Bytecode, new BytecodeSerializer()},
                {DataType.Cardinality, EnumSerializers.CardinalitySerializer},
                {DataType.Column, EnumSerializers.ColumnSerializer},
                {DataType.Direction, EnumSerializers.DirectionSerializer},
                {DataType.Operator, EnumSerializers.OperatorSerializer},
                {DataType.Order, EnumSerializers.OrderSerializer},
                {DataType.Pick, EnumSerializers.PickSerializer},
                {DataType.Pop, EnumSerializers.PopSerializer},
                {DataType.Lambda, new LambdaSerializer()},
                {DataType.P, new PSerializer(DataType.P)},
                {DataType.Scope, EnumSerializers.ScopeSerializer},
                {DataType.T, EnumSerializers.TSerializer},
                {DataType.Traverser, new TraverserSerializer()},
                {DataType.BigDecimal, new BigDecimalSerializer()},
                {DataType.BigInteger, new BigIntegerSerializer()},
                {DataType.Byte, SingleTypeSerializers.ByteSerializer},
                {DataType.ByteBuffer, new ByteBufferSerializer()},
                {DataType.Short, SingleTypeSerializers.ShortSerializer},
                {DataType.Boolean, SingleTypeSerializers.BooleanSerializer},
                {DataType.TextP, new PSerializer(DataType.TextP)},
                {DataType.TraversalStrategy, new TraversalStrategySerializer()},
                {DataType.BulkSet, new BulkSetSerializer<List<object>>()},
                {DataType.Char, new CharSerializer()},
                {DataType.Duration, new DurationSerializer()},
            };

        /// <summary>
        /// Gets a serializer for the given type of the value to be serialized.
        /// </summary>
        /// <param name="valueType">Type of the value to be serialized.</param>
        /// <returns>A serializer for the provided type.</returns>
        /// <exception cref="InvalidOperationException">Thrown when no serializer can be found for the type.</exception>
        public ITypeSerializer GetSerializerFor(Type valueType)
        {
            if (SerializerByType.ContainsKey(valueType))
            {
                return SerializerByType[valueType];
            }
            
            if (IsDictionaryType(valueType))
            {
                var dictKeyType = valueType.GetGenericArguments()[0];
                var dictValueType = valueType.GetGenericArguments()[1];
                var serializerType = typeof(MapSerializer<,>).MakeGenericType(dictKeyType, dictValueType);
                var serializer = (ITypeSerializer) Activator.CreateInstance(serializerType);
                SerializerByType[valueType] = serializer;
                return serializer;
            }

            if (IsSetType(valueType))
            {
                var memberType = valueType.GetGenericArguments()[0];
                var serializerType = typeof(SetSerializer<,>).MakeGenericType(valueType, memberType);
                var serializer = (ITypeSerializer) Activator.CreateInstance(serializerType);
                SerializerByType[valueType] = serializer;
                return serializer;
            }

            if (valueType.IsArray)
            {
                var memberType = valueType.GetElementType();
                var serializerType = typeof(ArraySerializer<>).MakeGenericType(memberType);
                var serializer = (ITypeSerializer) Activator.CreateInstance(serializerType);
                SerializerByType[valueType] = serializer;
                return serializer;
            }

            if (IsListType(valueType))
            {
                var memberType = valueType.GetGenericArguments()[0];
                var serializerType = typeof(ListSerializer<>).MakeGenericType(memberType);
                var serializer = (ITypeSerializer) Activator.CreateInstance(serializerType);
                SerializerByType[valueType] = serializer;
                return serializer;
            }

            foreach (var supportedType in SerializerByType.Keys)
            {
                if (supportedType.IsAssignableFrom(valueType))
                {
                    var serializer = SerializerByType[supportedType];
                    SerializerByType[valueType] = serializer;
                    return serializer;
                }
            }

            throw new InvalidOperationException($"No serializer found for type ${valueType}.");
        }

        private static bool IsDictionaryType(Type type)
        {
            return type.IsConstructedGenericType && type.GetGenericTypeDefinition() == typeof(Dictionary<,>);
        }
        
        private static bool IsSetType(Type type)
        {
            return type.GetInterfaces().Any(implementedInterface => implementedInterface.IsConstructedGenericType &&
                                                                    implementedInterface.GetGenericTypeDefinition() ==
                                                                    typeof(ISet<>));
        }

        private static bool IsListType(Type type)
        {
            return type.GetInterfaces().Any(implementedInterface => implementedInterface.IsConstructedGenericType &&
                                                                    implementedInterface.GetGenericTypeDefinition() ==
                                                                    typeof(IList<>));
        }
        
        /// <summary>
        /// Gets a serializer for the given GraphBinary type.
        /// </summary>
        /// <param name="type">The GraphBinary type that should be (de)serialized.</param>
        /// <returns>A serializer for the provided GraphBinary type.</returns>
        public ITypeSerializer GetSerializerFor(DataType type)
        {
            return SerializerByDataType[type];
        }
    }
}