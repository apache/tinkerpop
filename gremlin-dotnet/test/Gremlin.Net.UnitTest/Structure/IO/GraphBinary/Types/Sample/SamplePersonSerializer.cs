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
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Gremlin.Net.Structure.IO.GraphBinary;
using Gremlin.Net.Structure.IO.GraphBinary.Types;

namespace Gremlin.Net.UnitTest.Structure.IO.GraphBinary.Types.Sample
{
    public class SamplePersonSerializer : CustomTypeSerializer
    {
        private readonly byte[] _typeInfoBytes = { 0, 0, 0, 0 };

        public override async Task WriteAsync(object value, Stream stream, GraphBinaryWriter writer,
            CancellationToken cancellationToken = default)
        {
            // Write {custom type info}, {value_flag} and {value}
            await stream.WriteAsync(_typeInfoBytes, cancellationToken).ConfigureAwait(false);
            await WriteNullableValueAsync(value, stream, writer, cancellationToken);
        }

        public override async Task WriteNullableValueAsync(object value, Stream stream, GraphBinaryWriter writer,
            CancellationToken cancellationToken = default)
        {
            if (value == null)
            {
                await writer.WriteValueFlagNullAsync(stream, cancellationToken).ConfigureAwait(false);
                return;
            }

            await writer.WriteValueFlagNoneAsync(stream, cancellationToken).ConfigureAwait(false);

            var samplePerson = (SamplePerson)value;
            var name = samplePerson.Name;
            
            // value_length = name_byte_length + name_bytes + long
            await stream.WriteIntAsync(4 + Encoding.UTF8.GetBytes(name).Length + 8, cancellationToken)
                .ConfigureAwait(false);

            await writer.WriteNonNullableValueAsync(name, stream, cancellationToken).ConfigureAwait(false);
            await writer.WriteNonNullableValueAsync(samplePerson.BirthDate, stream, cancellationToken)
                .ConfigureAwait(false);
        }
        
        public override async Task WriteNonNullableValueAsync(object value, Stream stream, GraphBinaryWriter writer,
            CancellationToken cancellationToken = default)
        {
            if (value == null)
            {
                throw new IOException("Unexpected null value when nullable is false");
            }

            var samplePerson = (SamplePerson)value;
            var name = samplePerson.Name;
            
            // value_length = name_byte_length + name_bytes + long
            await stream.WriteIntAsync(4 + Encoding.UTF8.GetBytes(name).Length + 8, cancellationToken)
                .ConfigureAwait(false);

            await writer.WriteNonNullableValueAsync(name, stream, cancellationToken).ConfigureAwait(false);
            await writer.WriteNonNullableValueAsync(samplePerson.BirthDate, stream, cancellationToken)
                .ConfigureAwait(false);
        }

        public override async Task<object?> ReadAsync(Stream stream, GraphBinaryReader reader,
            CancellationToken cancellationToken = default)
        {
            // {custom type info}, {value_flag} and {value}
            // No custom_type_info
            if (await stream.ReadIntAsync(cancellationToken).ConfigureAwait(false) != 0)
            {
                throw new IOException("{custom_type_info} should not be provided for this custom type");
            }

            return await ReadNullableValueAsync(stream, reader, cancellationToken).ConfigureAwait(false);
        }

        public override async Task<object?> ReadNullableValueAsync(Stream stream, GraphBinaryReader reader,
            CancellationToken cancellationToken = default)
        {
            var valueFlag = await stream.ReadByteAsync(cancellationToken).ConfigureAwait(false);
            if ((valueFlag & 1) == 1)
            {
                return null;
            }
            
            // Read the byte length of the value bytes
            var valueLength = await stream.ReadIntAsync(cancellationToken).ConfigureAwait(false);

            if (valueLength <= 0)
            {
                throw new IOException($"Unexpected value length: {valueLength}");
            }

            if (valueLength > stream.Length)
            {
                throw new IOException($"Not enough readable bytes: {valueLength} (expected: {stream.Length})");
            }

            var name = (string)await reader.ReadNonNullableValueAsync<string>(stream, cancellationToken)
                .ConfigureAwait(false);
            var birthDate =
                (DateTimeOffset)await reader.ReadNonNullableValueAsync<DateTimeOffset>(stream, cancellationToken)
                    .ConfigureAwait(false);

            return new SamplePerson(name, birthDate);
        }
        
        public override async Task<object> ReadNonNullableValueAsync(Stream stream, GraphBinaryReader reader,
            CancellationToken cancellationToken = default)
        {
            // Read the byte length of the value bytes
            var valueLength = await stream.ReadIntAsync(cancellationToken).ConfigureAwait(false);

            if (valueLength <= 0)
            {
                throw new IOException($"Unexpected value length: {valueLength}");
            }

            if (valueLength > stream.Length)
            {
                throw new IOException($"Not enough readable bytes: {valueLength} (expected: {stream.Length})");
            }

            var name = (string)await reader.ReadNonNullableValueAsync<string>(stream, cancellationToken)
                .ConfigureAwait(false);
            var birthDate =
                (DateTimeOffset)await reader.ReadNonNullableValueAsync<DateTimeOffset>(stream, cancellationToken)
                    .ConfigureAwait(false);

            return new SamplePerson(name, birthDate);
        }

        public override string TypeName => "sampleProvider.SamplePerson";
    }
}