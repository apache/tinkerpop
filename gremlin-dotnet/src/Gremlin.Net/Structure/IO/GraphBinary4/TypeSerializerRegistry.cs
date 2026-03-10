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
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Numerics;
using Gremlin.Net.Process.Traversal;
using Gremlin.Net.Structure.IO.GraphBinary4.Types;

namespace Gremlin.Net.Structure.IO.GraphBinary4
{
    /// <summary>
    /// Provides GraphBinary 4.0 serializers for different types.
    /// </summary>
    public class TypeSerializerRegistry
    {
        private readonly Dictionary<Type, ITypeSerializer> _serializerByType =
            new Dictionary<Type, ITypeSerializer>
            {
                {typeof(int), SingleTypeSerializers.IntSerializer},
                {typeof(long), SingleTypeSerializers.LongSerializer},
                {typeof(string), new StringSerializer()},
                {typeof(DateTimeOffset), new DateTimeSerializer()},
                {typeof(double), SingleTypeSerializers.DoubleSerializer},
                {typeof(float), SingleTypeSerializers.FloatSerializer},
                {typeof(Guid), new UuidSerializer()},
                {typeof(Edge), new EdgeSerializer()},
                {typeof(Path), new PathSerializer()},
                {typeof(Property), new PropertySerializer()},
                {typeof(Vertex), new VertexSerializer()},
                {typeof(VertexProperty), new VertexPropertySerializer()},
                {typeof(Direction), EnumSerializers.DirectionSerializer},
                {typeof(Merge), EnumSerializers.MergeSerializer},
                {typeof(T), EnumSerializers.TSerializer},
                {typeof(decimal), new BigDecimalSerializer()},
                {typeof(BigInteger), new BigIntegerSerializer()},
                {typeof(sbyte), SingleTypeSerializers.ByteSerializer},
                {typeof(byte[]), new BinarySerializer()},
                {typeof(short), SingleTypeSerializers.ShortSerializer},
                {typeof(bool), SingleTypeSerializers.BooleanSerializer},
                {typeof(char), new CharSerializer()},
                {typeof(TimeSpan), new DurationSerializer()},
                {typeof(Marker), SingleTypeSerializers.MarkerSerializer},
            };

        private readonly Dictionary<DataType, ITypeSerializer> _serializerByDataType =
            new Dictionary<DataType, ITypeSerializer>
            {
                {DataType.Int, SingleTypeSerializers.IntSerializer},
                {DataType.Long, SingleTypeSerializers.LongSerializer},
                {DataType.String, new StringSerializer()},
                {DataType.DateTime, new DateTimeSerializer()},
                {DataType.Double, SingleTypeSerializers.DoubleSerializer},
                {DataType.Float, SingleTypeSerializers.FloatSerializer},
                {DataType.List, new ListSerializer<object>()},
                {DataType.Map, new MapSerializer<object, object>()},
                {DataType.Set, new SetSerializer<HashSet<object?>, object>()},
                {DataType.Uuid, new UuidSerializer()},
                {DataType.Edge, new EdgeSerializer()},
                {DataType.Path, new PathSerializer()},
                {DataType.Property, new PropertySerializer()},
                {DataType.Vertex, new VertexSerializer()},
                {DataType.VertexProperty, new VertexPropertySerializer()},
                {DataType.Direction, EnumSerializers.DirectionSerializer},
                {DataType.Merge, EnumSerializers.MergeSerializer},
                {DataType.T, EnumSerializers.TSerializer},
                {DataType.BigDecimal, new BigDecimalSerializer()},
                {DataType.BigInteger, new BigIntegerSerializer()},
                {DataType.Byte, SingleTypeSerializers.ByteSerializer},
                {DataType.Binary, new BinarySerializer()},
                {DataType.Short, SingleTypeSerializers.ShortSerializer},
                {DataType.Boolean, SingleTypeSerializers.BooleanSerializer},
                {DataType.Char, new CharSerializer()},
                {DataType.Duration, new DurationSerializer()},
                {DataType.Marker, SingleTypeSerializers.MarkerSerializer},
            };

        /// <summary>
        /// Provides a default <see cref="TypeSerializerRegistry"/> instance.
        /// </summary>
        public static readonly TypeSerializerRegistry Instance = new TypeSerializerRegistry();

        /// <summary>
        /// Gets a serializer for the given type of the value to be serialized.
        /// </summary>
        /// <param name="valueType">Type of the value to be serialized.</param>
        /// <returns>A serializer for the provided type.</returns>
        /// <exception cref="InvalidOperationException">Thrown when no serializer can be found for the type.</exception>
        public ITypeSerializer GetSerializerFor(Type valueType)
        {
            if (_serializerByType.TryGetValue(valueType, out var serializerForType))
            {
                return serializerForType;
            }

            if (IsDictionaryType(valueType, out var dictKeyType, out var dictValueType))
            {
                var serializerType = typeof(MapSerializer<,>).MakeGenericType(dictKeyType, dictValueType);
                var serializer = (ITypeSerializer?) Activator.CreateInstance(serializerType);
                _serializerByType[valueType] = serializer ??
                                               throw new IOException(
                                                   $"Cannot create a serializer for the dictionary type {valueType}.");
                return serializer;
            }

            if (IsSetType(valueType))
            {
                var memberType = valueType.GetGenericArguments()[0];
                var serializerType = typeof(SetSerializer<,>).MakeGenericType(valueType, memberType);
                var serializer = (ITypeSerializer?) Activator.CreateInstance(serializerType);
                _serializerByType[valueType] = serializer ??
                                               throw new IOException(
                                                   $"Cannot create a serializer for the set type {valueType}.");
                return serializer;
            }

            if (valueType.IsArray)
            {
                var memberType = valueType.GetElementType();
                var serializerType = typeof(ArraySerializer<>).MakeGenericType(memberType!);
                var serializer = (ITypeSerializer?) Activator.CreateInstance(serializerType);
                _serializerByType[valueType] = serializer ??
                                               throw new IOException(
                                                   $"Cannot create a serializer for the array type {valueType}.");
                return serializer;
            }

            if (IsListType(valueType))
            {
                var memberType = valueType.GetGenericArguments()[0];
                var serializerType = typeof(ListSerializer<>).MakeGenericType(memberType);
                var serializer = (ITypeSerializer?) Activator.CreateInstance(serializerType);
                _serializerByType[valueType] = serializer ??
                                               throw new IOException(
                                                   $"Cannot create a serializer for the list type {valueType}.");
                return serializer;
            }

            foreach (var supportedType in new List<Type>(_serializerByType.Keys))
            {
                if (supportedType.IsAssignableFrom(valueType))
                {
                    var serializer = _serializerByType[supportedType];
                    _serializerByType[valueType] = serializer;
                    return serializer;
                }
            }

            throw new InvalidOperationException($"No serializer found for type '{valueType}'.");
        }

        /// <summary>
        /// Gets a serializer for the given GraphBinary 4.0 data type code.
        /// </summary>
        /// <param name="dataType">The GraphBinary 4.0 data type.</param>
        /// <returns>A serializer for the provided data type.</returns>
        public ITypeSerializer GetSerializerFor(DataType dataType)
        {
            return _serializerByDataType[dataType];
        }

        private static bool IsDictionaryType(Type type, [NotNullWhen(returnValue: true)] out Type? keyType,
            [NotNullWhen(returnValue: true)] out Type? valueType)
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

    }
}
