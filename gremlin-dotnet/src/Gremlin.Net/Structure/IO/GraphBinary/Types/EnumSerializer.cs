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
using System.IO;
using System.Threading.Tasks;
using Gremlin.Net.Process.Traversal;

namespace Gremlin.Net.Structure.IO.GraphBinary.Types
{
    /// <summary>
    /// Provides serializers for enum types.
    /// </summary>
    public static class EnumSerializers
    {
        /// <summary>
        /// A serializer for <see cref="Barrier"/> values.
        /// </summary>
        public static readonly EnumSerializer<Barrier> BarrierSerializer =
            new EnumSerializer<Barrier>(DataType.Barrier, Barrier.GetByValue);

        /// <summary>
        /// A serializer for <see cref="Cardinality"/> values.
        /// </summary>
        public static readonly EnumSerializer<Cardinality> CardinalitySerializer =
            new EnumSerializer<Cardinality>(DataType.Cardinality, Cardinality.GetByValue);

        /// <summary>
        /// A serializer for <see cref="Column"/> values.
        /// </summary>
        public static readonly EnumSerializer<Column> ColumnSerializer =
            new EnumSerializer<Column>(DataType.Column, Column.GetByValue);

        /// <summary>
        /// A serializer for <see cref="Direction"/> values.
        /// </summary>
        public static readonly EnumSerializer<Direction> DirectionSerializer =
            new EnumSerializer<Direction>(DataType.Direction, Direction.GetByValue);

        /// <summary>
        /// A serializer for <see cref="Merge"/> values.
        /// </summary>
        public static readonly EnumSerializer<Merge> MergeSerializer =
            new EnumSerializer<Merge>(DataType.Merge, Merge.GetByValue);

        /// <summary>
        /// A serializer for <see cref="Operator"/> values.
        /// </summary>
        public static readonly EnumSerializer<Operator> OperatorSerializer =
            new EnumSerializer<Operator>(DataType.Operator, Operator.GetByValue);

        /// <summary>
        /// A serializer for <see cref="Order"/> values.
        /// </summary>
        public static readonly EnumSerializer<Order> OrderSerializer =
            new EnumSerializer<Order>(DataType.Order, Order.GetByValue);

        /// <summary>
        /// A serializer for <see cref="Pick"/> values.
        /// </summary>
        public static readonly EnumSerializer<Pick> PickSerializer =
            new EnumSerializer<Pick>(DataType.Pick, Pick.GetByValue);

        /// <summary>
        /// A serializer for <see cref="Pop"/> values.
        /// </summary>
        public static readonly EnumSerializer<Pop> PopSerializer =
            new EnumSerializer<Pop>(DataType.Pop, Pop.GetByValue);

        /// <summary>
        /// A serializer for <see cref="Scope"/> values.
        /// </summary>
        public static readonly EnumSerializer<Scope> ScopeSerializer =
            new EnumSerializer<Scope>(DataType.Scope, Scope.GetByValue);

        /// <summary>
        /// A serializer for <see cref="T"/> values.
        /// </summary>
        public static readonly EnumSerializer<T> TSerializer =
            new EnumSerializer<T>(DataType.T, T.GetByValue);
    }

    /// <summary>
    /// Generalized serializer for enum types.
    /// </summary>
    /// <typeparam name="TEnum">The type of the enum to serialize.</typeparam>
    public class EnumSerializer<TEnum> : SimpleTypeSerializer<TEnum>
        where TEnum : EnumWrapper
    {
        private readonly Func<string, TEnum> _readFunc;

        internal EnumSerializer(DataType dataType, Func<string, TEnum> readFunc) : base(dataType)
        {
            _readFunc = readFunc;
        }

        /// <inheritdoc />
        protected override async Task WriteValueAsync(TEnum value, Stream stream, GraphBinaryWriter writer)
        {
            await writer.WriteAsync(value.EnumValue, stream).ConfigureAwait(false);
        }

        /// <inheritdoc />
        protected override async Task<TEnum> ReadValueAsync(Stream stream, GraphBinaryReader reader)
        {
            var enumValue = (string) await reader.ReadAsync(stream).ConfigureAwait(false);
            return _readFunc.Invoke(enumValue);
        }
    }
}