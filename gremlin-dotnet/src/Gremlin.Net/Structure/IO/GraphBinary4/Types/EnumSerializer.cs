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
using System.Threading;
using System.Threading.Tasks;
using Gremlin.Net.Process.Traversal;

namespace Gremlin.Net.Structure.IO.GraphBinary4.Types
{
    /// <summary>
    /// Provides serializers for enum types supported in GraphBinary 4.0.
    /// </summary>
    /// <remarks>
    /// GraphBinary 4.0 only supports Direction, T, and Merge enum types.
    /// The following types were removed in v4: Barrier, Cardinality, Column, DT, GType,
    /// Operator, Order, Pick, Pop, Scope.
    /// </remarks>
    public static class EnumSerializers
    {
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
        protected override async Task WriteValueAsync(TEnum value, Stream stream, GraphBinaryWriter writer,
            CancellationToken cancellationToken = default)
        {
            await writer.WriteAsync(value.EnumValue, stream, cancellationToken).ConfigureAwait(false);
        }

        /// <inheritdoc />
        protected override async Task<TEnum> ReadValueAsync(Stream stream, GraphBinaryReader reader,
            CancellationToken cancellationToken = default)
        {
            // This should probably be `reader.ReadNonNullableValueAsync<string>(stream, cancellationToken)` instead,
            // but it's the same in other GLVs and changing this would be a breaking change for the GraphBinary format.
            var enumValue = (string?) await reader.ReadAsync(stream, cancellationToken).ConfigureAwait(false);
            if (enumValue == null) throw new IOException($"Read null as a value for {DataType}");
            return _readFunc.Invoke(enumValue);
        }
    }
}