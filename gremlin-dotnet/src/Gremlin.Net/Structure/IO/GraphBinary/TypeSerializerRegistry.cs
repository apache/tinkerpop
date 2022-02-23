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
        private readonly Dictionary<Type, ITypeSerializer> _serializerByType =
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
                {typeof(Merge), EnumSerializers.MergeSerializer},
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

        private readonly Dictionary<DataType, ITypeSerializer> _serializerByDataType =
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
                {DataType.Merge, EnumSerializers.MergeSerializer},
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

        private readonly Dictionary<string, CustomTypeSerializer> _serializerByCustomTypeName =
            new Dictionary<string, CustomTypeSerializer>();

        private TypeSerializerRegistry(List<CustomTypeRegistryEntry> customTypeEntries)
        {
            foreach (var entry in customTypeEntries)
            {
                _serializerByType[entry.Type] = entry.TypeSerializer;
                _serializerByCustomTypeName[entry.CustomTypeName] = entry.TypeSerializer;
            }
        }

        /// <summary>
        /// Provides a default <see cref="TypeSerializerRegistry"/> instance.
        /// </summary>
        public static readonly TypeSerializerRegistry Instance = Build().Create();

        /// <summary>
        /// Builds a <see cref="TypeSerializerRegistry"/>.
        /// </summary>
        public static Builder Build() => new Builder();

        /// <summary>
        /// Gets a serializer for the given type of the value to be serialized.
        /// </summary>
        /// <param name="valueType">Type of the value to be serialized.</param>
        /// <returns>A serializer for the provided type.</returns>
        /// <exception cref="InvalidOperationException">Thrown when no serializer can be found for the type.</exception>
        public ITypeSerializer GetSerializerFor(Type valueType)
        {
            if (_serializerByType.ContainsKey(valueType))
            {
                return _serializerByType[valueType];
            }
            
            if (IsDictionaryType(valueType, out var dictKeyType, out var dictValueType))
            {
                var serializerType = typeof(MapSerializer<,>).MakeGenericType(dictKeyType, dictValueType);
                var serializer = (ITypeSerializer) Activator.CreateInstance(serializerType);
                _serializerByType[valueType] = serializer;
                return serializer;
            }

            if (IsSetType(valueType))
            {
                var memberType = valueType.GetGenericArguments()[0];
                var serializerType = typeof(SetSerializer<,>).MakeGenericType(valueType, memberType);
                var serializer = (ITypeSerializer) Activator.CreateInstance(serializerType);
                _serializerByType[valueType] = serializer;
                return serializer;
            }

            if (valueType.IsArray)
            {
                var memberType = valueType.GetElementType();
                var serializerType = typeof(ArraySerializer<>).MakeGenericType(memberType);
                var serializer = (ITypeSerializer) Activator.CreateInstance(serializerType);
                _serializerByType[valueType] = serializer;
                return serializer;
            }

            if (IsListType(valueType))
            {
                var memberType = valueType.GetGenericArguments()[0];
                var serializerType = typeof(ListSerializer<>).MakeGenericType(memberType);
                var serializer = (ITypeSerializer) Activator.CreateInstance(serializerType);
                _serializerByType[valueType] = serializer;
                return serializer;
            }

            foreach (var supportedType in _serializerByType.Keys)
            {
                if (supportedType.IsAssignableFrom(valueType))
                {
                    var serializer = _serializerByType[supportedType];
                    _serializerByType[valueType] = serializer;
                    return serializer;
                }
            }

            throw new InvalidOperationException($"No serializer found for type ${valueType}.");
        }

        private static bool IsDictionaryType(Type type, out Type keyType, out Type valueType)
        {
            var maybeInterfaceType = type
                .GetInterfaces()
                .FirstOrDefault(implementedInterfaceType => implementedInterfaceType.IsConstructedGenericType && implementedInterfaceType.GetGenericTypeDefinition() == typeof(IDictionary<,>));

            if (maybeInterfaceType is { } interfaceType)
            {
                keyType = interfaceType.GetGenericArguments()[0];
                valueType = interfaceType.GetGenericArguments()[1];

                return true;
            }

            keyType = null;
            valueType = null;

            return false;
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
            if (type == DataType.Custom)
                throw new ArgumentException("Custom type serializers can not be retrieved using this method");
            
            return _serializerByDataType[type];
        }
        
        /// <summary>
        /// Gets a serializer for the given custom type name.
        /// </summary>
        /// <param name="typeName">The custom type name.</param>
        /// <returns>A serializer for the provided custom type name.</returns>
        public CustomTypeSerializer GetSerializerForCustomType(string typeName)
        {
            return _serializerByCustomTypeName[typeName];
        }
        
        /// <summary>
        /// The builder of a <see cref="TypeSerializerRegistry"/>.
        /// </summary>
        public class Builder
        {
            private readonly List<CustomTypeRegistryEntry> _list = new List<CustomTypeRegistryEntry>();
        
            /// <summary>
            /// Creates the <see cref="TypeSerializerRegistry"/>.
            /// </summary>
            public TypeSerializerRegistry Create() => new TypeSerializerRegistry(_list);

            /// <summary>
            /// Adds a serializer for a custom type.
            /// </summary>
            /// <param name="type">The custom type supported by the serializer.</param>
            /// <param name="serializer">The serializer for the custom type.</param>
            public Builder AddCustomType(Type type, CustomTypeSerializer serializer)
            {
                if (serializer == null) throw new ArgumentNullException(nameof(serializer));
                if (serializer.TypeName == null)
                    throw new ArgumentException("serializer custom type name can not be null");
                
                _list.Add(new CustomTypeRegistryEntry(type, serializer));
                return this;
            }
        }

        private class CustomTypeRegistryEntry
        {
            public Type Type { get; }
            public CustomTypeSerializer TypeSerializer { get; }

            public CustomTypeRegistryEntry(Type type, CustomTypeSerializer typeSerializer)
            {
                Type = type;
                TypeSerializer = typeSerializer;
            }
            public string CustomTypeName => TypeSerializer.TypeName;
        }
    }
}