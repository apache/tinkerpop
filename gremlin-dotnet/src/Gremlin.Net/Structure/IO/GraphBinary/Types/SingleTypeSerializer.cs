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
            (value, stream) => stream.WriteIntAsync(value), stream => stream.ReadIntAsync());
        
        /// <summary>
        /// A serializer for <see cref="long"/> values.
        /// </summary>
        public static readonly SingleTypeSerializer<long> LongSerializer = new SingleTypeSerializer<long>(DataType.Long,
            (value, stream) => stream.WriteLongAsync(value), stream => stream.ReadLongAsync());

        /// <summary>
        /// A serializer for <see cref="double"/> values.
        /// </summary>
        public static readonly SingleTypeSerializer<double> DoubleSerializer =
            new SingleTypeSerializer<double>(DataType.Double, (value, stream) => stream.WriteDoubleAsync(value),
                stream => stream.ReadDoubleAsync());

        /// <summary>
        /// A serializer for <see cref="float"/> values.
        /// </summary>
        public static readonly SingleTypeSerializer<float> FloatSerializer =
            new SingleTypeSerializer<float>(DataType.Float, (value, stream) => stream.WriteFloatAsync(value),
                stream => stream.ReadFloatAsync());

        /// <summary>
        /// A serializer for <see cref="short"/> values.
        /// </summary>
        public static readonly SingleTypeSerializer<short> ShortSerializer =
            new SingleTypeSerializer<short>(DataType.Short, (value, stream) => stream.WriteShortAsync(value),
                stream => stream.ReadShortAsync());

        /// <summary>
        /// A serializer for <see cref="bool"/> values.
        /// </summary>
        public static readonly SingleTypeSerializer<bool> BooleanSerializer =
            new SingleTypeSerializer<bool>(DataType.Boolean, (value, stream) => stream.WriteBoolAsync(value),
                stream => stream.ReadBoolAsync());

        /// <summary>
        /// A serializer for <see cref="byte"/> values.
        /// </summary>
        public static readonly SingleTypeSerializer<byte> ByteSerializer = new SingleTypeSerializer<byte>(DataType.Byte,
            (value, stream) => stream.WriteByteAsync(value), stream => stream.ReadByteAsync());
    }
    
    /// <summary>
    /// Represents a serializer for types that can be represented as a single value and that can be read and write in a
    /// single operation.
    /// </summary>
    public class SingleTypeSerializer<T> : SimpleTypeSerializer<T>
    {
        private readonly Func<T, Stream, Task> _writeFunc;
        private readonly Func<Stream, Task<T>> _readFunc;

        internal SingleTypeSerializer(DataType dataType, Func<T, Stream, Task> writeFunc, Func<Stream, Task<T>> readFunc)
            : base(dataType)
        {
            _writeFunc = writeFunc;
            _readFunc = readFunc;
        }

        /// <inheritdoc />
        protected override async Task WriteValueAsync(T value, Stream stream, GraphBinaryWriter writer)
        {
            await _writeFunc.Invoke(value, stream).ConfigureAwait(false);
        }

        /// <inheritdoc />
        protected override async Task<T> ReadValueAsync(Stream stream, GraphBinaryReader reader)
        {
            return await _readFunc.Invoke(stream).ConfigureAwait(false);
        }
    }
}