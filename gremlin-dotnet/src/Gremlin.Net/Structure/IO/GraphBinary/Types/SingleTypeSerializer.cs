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

namespace Gremlin.Net.Structure.IO.GraphBinary.Types
{
    /// <summary>
    /// Provides serializers for types that can be represented as a single value and that can be read and write in a
    /// single operation.
    /// </summary>
    public static class SingleTypeSerializers
    {
        /// <summary>
        /// A serializer for <see cref="int"/> values.
        /// </summary>
        public static readonly SingleTypeSerializer<int> IntSerializer = new SingleTypeSerializer<int>(DataType.Int,
            (value, stream, cancellationToken) => stream.WriteIntAsync(value, cancellationToken),
            (stream, cancellationToken) => stream.ReadIntAsync(cancellationToken));

        /// <summary>
        /// A serializer for <see cref="long"/> values.
        /// </summary>
        public static readonly SingleTypeSerializer<long> LongSerializer = new SingleTypeSerializer<long>(DataType.Long,
            (value, stream, cancellationToken) => stream.WriteLongAsync(value, cancellationToken),
            (stream, cancellationToken) => stream.ReadLongAsync(cancellationToken));

        /// <summary>
        /// A serializer for <see cref="double"/> values.
        /// </summary>
        public static readonly SingleTypeSerializer<double> DoubleSerializer =
            new SingleTypeSerializer<double>(DataType.Double,
                (value, stream, cancellationToken) => stream.WriteDoubleAsync(value, cancellationToken),
                (stream, cancellationToken) => stream.ReadDoubleAsync(cancellationToken));

        /// <summary>
        /// A serializer for <see cref="float"/> values.
        /// </summary>
        public static readonly SingleTypeSerializer<float> FloatSerializer =
            new SingleTypeSerializer<float>(DataType.Float,
                (value, stream, cancellationToken) => stream.WriteFloatAsync(value, cancellationToken),
                (stream, cancellationToken) => stream.ReadFloatAsync(cancellationToken));

        /// <summary>
        /// A serializer for <see cref="short"/> values.
        /// </summary>
        public static readonly SingleTypeSerializer<short> ShortSerializer =
            new SingleTypeSerializer<short>(DataType.Short,
                (value, stream, cancellationToken) => stream.WriteShortAsync(value, cancellationToken),
                (stream, cancellationToken) => stream.ReadShortAsync(cancellationToken));

        /// <summary>
        /// A serializer for <see cref="bool"/> values.
        /// </summary>
        public static readonly SingleTypeSerializer<bool> BooleanSerializer =
            new SingleTypeSerializer<bool>(DataType.Boolean,
                (value, stream, cancellationToken) => stream.WriteBoolAsync(value, cancellationToken),
                (stream, cancellationToken) => stream.ReadBoolAsync(cancellationToken));

        /// <summary>
        /// A serializer for <see cref="sbyte"/> values.
        /// </summary>
        public static readonly SingleTypeSerializer<sbyte> ByteSerializer = new SingleTypeSerializer<sbyte>(DataType.Byte,
            (value, stream, cancellationToken) => stream.WriteSByteAsync(value, cancellationToken),
            (stream, cancellationToken) => stream.ReadSByteAsync(cancellationToken));
    }
    
    /// <summary>
    /// Represents a serializer for types that can be represented as a single value and that can be read and write in a
    /// single operation.
    /// </summary>
    public class SingleTypeSerializer<T> : SimpleTypeSerializer<T>
    {
        private readonly Func<T, Stream, CancellationToken, Task> _writeFunc;
        private readonly Func<Stream, CancellationToken, Task<T>> _readFunc;

        internal SingleTypeSerializer(DataType dataType, Func<T, Stream, CancellationToken, Task> writeFunc,
            Func<Stream, CancellationToken, Task<T>> readFunc)
            : base(dataType)
        {
            _writeFunc = writeFunc;
            _readFunc = readFunc;
        }

        /// <inheritdoc />
        protected override async Task WriteValueAsync(T value, Stream stream, GraphBinaryWriter writer,
            CancellationToken cancellationToken = default)
        {
            await _writeFunc.Invoke(value, stream, cancellationToken).ConfigureAwait(false);
        }

        /// <inheritdoc />
        protected override async Task<T> ReadValueAsync(Stream stream, GraphBinaryReader reader,
            CancellationToken cancellationToken = default)
        {
            return await _readFunc.Invoke(stream, cancellationToken).ConfigureAwait(false);
        }
    }
}