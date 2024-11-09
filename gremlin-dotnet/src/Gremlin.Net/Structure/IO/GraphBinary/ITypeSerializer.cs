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

using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Gremlin.Net.Structure.IO.GraphBinary
{
    /// <summary>
    /// Represents a serializer for a certain type.
    /// </summary>
    public interface ITypeSerializer
    {
        /// <summary>
        /// Gets the <see cref="DataType"/> that supported by this serializer.
        /// </summary>
        DataType DataType { get; }

        /// <summary>
        /// Writes the type code, information and value to a stream.
        /// </summary>
        /// <param name="value">The value to write.</param>
        /// <param name="stream">The stream to write to.</param>
        /// <param name="writer">A <see cref="GraphBinaryWriter"/> that can be used to write nested values.</param>
        /// <param name="cancellationToken">The token to cancel the operation. The default value is None.</param>
        /// <returns>A task that represents the asynchronous write operation.</returns>
        Task WriteAsync(object value, Stream stream, GraphBinaryWriter writer,
            CancellationToken cancellationToken = default);

        /// <summary>
        /// Writes the nullable value to a stream, composed by the value flag and the sequence of bytes.
        /// </summary>
        /// <param name="value">The value to write.</param>
        /// <param name="stream">The stream to write to.</param>
        /// <param name="writer">A <see cref="GraphBinaryWriter"/> that can be used to write nested values.</param>
        /// <param name="cancellationToken">The token to cancel the operation. The default value is None.</param>
        /// <returns>A task that represents the asynchronous write operation.</returns>
        Task WriteNullableValueAsync(object value, Stream stream, GraphBinaryWriter writer,
            CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Writes the non-nullable value to a stream, composed by the value flag and the sequence of bytes.
        /// </summary>
        /// <param name="value">The value to write.</param>
        /// <param name="stream">The stream to write to.</param>
        /// <param name="writer">A <see cref="GraphBinaryWriter"/> that can be used to write nested values.</param>
        /// <param name="cancellationToken">The token to cancel the operation. The default value is None.</param>
        /// <returns>A task that represents the asynchronous write operation.</returns>
        Task WriteNonNullableValueAsync(object value, Stream stream, GraphBinaryWriter writer,
            CancellationToken cancellationToken = default);

        /// <summary>
        /// Reads the type information and value from the stream.
        /// </summary>
        /// <param name="stream">The GraphBinary data to parse.</param>
        /// <param name="reader">A <see cref="GraphBinaryReader"/> that can be used to read nested values.</param>
        /// <param name="cancellationToken">The token to cancel the operation. The default value is None.</param>
        /// <returns>The read value.</returns>
        Task<object?> ReadAsync(Stream stream, GraphBinaryReader reader, CancellationToken cancellationToken = default);

        /// <summary>
        /// Reads the value from the stream (not the type information).
        /// </summary>
        /// <param name="stream">The GraphBinary data to parse.</param>
        /// <param name="reader">A <see cref="GraphBinaryReader"/> that can be used to read nested values.</param>
        /// <param name="cancellationToken">The token to cancel the operation. The default value is None.</param>
        /// <returns>The read value.</returns>
        Task<object?> ReadNullableValueAsync(Stream stream, GraphBinaryReader reader,
            CancellationToken cancellationToken = default);

        /// <summary>
        /// Reads the value from the stream (not the type information).
        /// </summary>
        /// <param name="stream">The GraphBinary data to parse.</param>
        /// <param name="reader">A <see cref="GraphBinaryReader"/> that can be used to read nested values.</param>
        /// <param name="cancellationToken">The token to cancel the operation. The default value is None.</param>
        /// <returns>The read value.</returns>
        Task<object> ReadNonNullableValueAsync(Stream stream, GraphBinaryReader reader,
            CancellationToken cancellationToken = default);
    }
}