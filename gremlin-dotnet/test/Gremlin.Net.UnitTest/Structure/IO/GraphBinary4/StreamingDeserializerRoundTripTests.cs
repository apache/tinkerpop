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
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Gremlin.Net.Driver;
using Gremlin.Net.Driver.Exceptions;
using Gremlin.Net.Process.Traversal;
using Gremlin.Net.Structure.IO.GraphBinary4;
using Xunit;

namespace Gremlin.Net.UnitTest.Structure.IO.GraphBinary4
{
    /// <summary>
    ///     Round-trip tests for streaming deserialization with randomized inputs.
    /// </summary>
    public class StreamingDeserializerRoundTripTests
    {
        /// <summary>
        ///     Streaming deserialization round-trip (non-bulked mode).
        ///
        ///     For any random list of primitive values, serializing them into a valid
        ///     GraphBinary 4.0 non-bulked response stream and deserializing via
        ///     ReadStreamingAsync yields the same values in the same order.
        /// </summary>
        [Theory]
        [InlineData(0)]
        [InlineData(1)]
        [InlineData(42)]
        [InlineData(-1)]
        [InlineData(12345)]
        [InlineData(-99999)]
        [InlineData(int.MaxValue)]
        [InlineData(int.MinValue)]
        [InlineData(7777)]
        [InlineData(314159)]
        public async Task NonBulkedRoundTrip(int seed)
        {
            Assert.True(await NonBulkedRoundTripAsync(seed));
        }

        private static async Task<bool> NonBulkedRoundTripAsync(int seed)
        {
            var rng = new System.Random(seed);
            var count = rng.Next(0, 20);
            var values = GenerateRandomPrimitives(rng, count);

            using var stream = new MemoryStream();
            var writer = new GraphBinaryWriter();

            await WriteNonBulkedResponse(stream, writer, values);
            stream.Position = 0;

            var reader = new GraphBinaryReader();
            var serializer = new ResponseMessageSerializer();

            var results = new List<object>();
            await foreach (var item in serializer.ReadStreamingAsync(stream, reader))
            {
                results.Add(item);
            }

            if (values.Count != results.Count) return false;

            for (var i = 0; i < values.Count; i++)
            {
                if (!values[i].Value.Equals(results[i])) return false;
            }

            return true;
        }

        /// <summary>
        ///     Streaming deserialization round-trip (bulked mode).
        ///
        ///     For any random list of primitive values with random bulk counts,
        ///     serializing them into a valid GraphBinary 4.0 bulked response stream
        ///     and deserializing via ReadStreamingAsync yields Traverser objects
        ///     wrapping the original values with the correct bulk counts.
        /// </summary>
        [Theory]
        [InlineData(0)]
        [InlineData(1)]
        [InlineData(42)]
        [InlineData(-1)]
        [InlineData(12345)]
        [InlineData(-99999)]
        [InlineData(int.MaxValue)]
        [InlineData(int.MinValue)]
        [InlineData(7777)]
        [InlineData(314159)]
        public async Task BulkedRoundTrip(int seed)
        {
            Assert.True(await BulkedRoundTripAsync(seed));
        }

        private static async Task<bool> BulkedRoundTripAsync(int seed)
        {
            var rng = new System.Random(seed);
            var count = rng.Next(0, 20);
            var values = GenerateRandomPrimitives(rng, count);
            var bulks = Enumerable.Range(0, count)
                .Select(_ => (long)(rng.Next(1, 1000)))
                .ToList();

            var entries = values.Zip(bulks, (v, b) => (v, b)).ToList();

            using var stream = new MemoryStream();
            var writer = new GraphBinaryWriter();

            await WriteBulkedResponse(stream, writer, entries);
            stream.Position = 0;

            var reader = new GraphBinaryReader();
            var serializer = new ResponseMessageSerializer();

            var results = new List<object>();
            await foreach (var item in serializer.ReadStreamingAsync(stream, reader))
            {
                results.Add(item);
            }

            if (entries.Count != results.Count) return false;

            for (var i = 0; i < entries.Count; i++)
            {
                if (results[i] is not Traverser traverser) return false;
                if (!entries[i].v.Value.Equals((object)traverser.Object)) return false;
                if (entries[i].b != traverser.Bulk) return false;
            }

            return true;
        }

        /// <summary>
        ///     Version byte validation.
        ///
        ///     For any byte value in the range 0x00–0x7F (MSB not set),
        ///     constructing a stream starting with that byte and attempting to
        ///     iterate ReadStreamingAsync throws IOException.
        /// </summary>
        [Theory]
        [InlineData(0x00)]
        [InlineData(0x01)]
        [InlineData(0x7F)]
        [InlineData(0x40)]
        [InlineData(0x3F)]
        [InlineData(0x10)]
        [InlineData(0x55)]
        [InlineData(0x6A)]
        [InlineData(0x20)]
        [InlineData(0x0F)]
        public async Task InvalidVersionByteThrowsIOException(byte invalidByte)
        {
            Assert.True(await InvalidVersionByteThrowsIOExceptionAsync(invalidByte));
        }

        private static async Task<bool> InvalidVersionByteThrowsIOExceptionAsync(byte versionByte)
        {
            using var stream = new MemoryStream();

            // Write the invalid version byte
            await stream.WriteByteAsync(versionByte);
            // Write enough trailing data to avoid EOF errors before the version check
            await stream.WriteByteAsync(0x00); // bulked flag
            // Marker
            await stream.WriteByteAsync(0xFD);
            await stream.WriteByteAsync(0x00);
            await stream.WriteByteAsync(0x00);
            // Status footer
            await stream.WriteIntAsync(200);
            await stream.WriteByteAsync(0x01);
            await stream.WriteByteAsync(0x01);

            stream.Position = 0;
            var reader = new GraphBinaryReader();
            var serializer = new ResponseMessageSerializer();

            try
            {
                await foreach (var _ in serializer.ReadStreamingAsync(stream, reader))
                {
                    // Should not yield any items
                }

                return false; // Should have thrown
            }
            catch (IOException)
            {
                return true; // Expected
            }
        }

        /// <summary>
        ///     Verifies that version byte 0x81 (MSB set) is accepted as valid.
        /// </summary>
        [Fact]
        public async Task ValidVersionByte0x81Succeeds()
        {
            using var stream = new MemoryStream();

            // Version byte 0x81 (MSB set, valid)
            await stream.WriteByteAsync(0x81);
            // Bulked = false
            await stream.WriteByteAsync(0x00);
            // Marker immediately (no results)
            await stream.WriteByteAsync(0xFD);
            await stream.WriteByteAsync(0x00);
            await stream.WriteByteAsync(0x00);
            // Status footer: 200, null message, null exception
            await stream.WriteIntAsync(200);
            await stream.WriteByteAsync(0x01);
            await stream.WriteByteAsync(0x01);

            stream.Position = 0;
            var reader = new GraphBinaryReader();
            var serializer = new ResponseMessageSerializer();

            var results = new List<object>();
            await foreach (var item in serializer.ReadStreamingAsync(stream, reader))
            {
                results.Add(item);
            }

            // Should succeed with no results (empty result set with valid version byte)
            Assert.Empty(results);
        }

        /// <summary>
        ///     Error status code propagation.
        ///
        ///     For any random non-200 status code (1–199, 201–599), random nullable
        ///     status message, and random nullable exception string, writing a valid
        ///     response stream with an error footer and iterating ReadStreamingAsync
        ///     throws a ResponseException with matching StatusCode, Message, and
        ///     ServerException fields.
        /// </summary>
        [Theory]
        [InlineData(0)]
        [InlineData(1)]
        [InlineData(42)]
        [InlineData(-1)]
        [InlineData(12345)]
        [InlineData(-99999)]
        [InlineData(int.MaxValue)]
        [InlineData(int.MinValue)]
        [InlineData(7777)]
        [InlineData(314159)]
        public async Task ErrorStatusCodePropagation(int seed)
        {
            Assert.True(await ErrorStatusCodePropagationAsync(seed));
        }

        private static async Task<bool> ErrorStatusCodePropagationAsync(int seed)
        {
            var rng = new System.Random(seed);

            // Generate a non-200 status code in range 1–599
            int statusCode;
            do
            {
                statusCode = rng.Next(1, 600);
            } while (statusCode == 200);

            // Generate nullable status message
            var hasMessage = rng.Next(2) == 1;
            var statusMessage = hasMessage ? GenerateRandomString(rng) : null;

            // Generate nullable exception string
            var hasException = rng.Next(2) == 1;
            var exceptionString = hasException ? GenerateRandomString(rng) : null;

            using var stream = new MemoryStream();

            // Write a valid response with error footer (no result data)
            await WriteErrorResponse(stream, statusCode, statusMessage, exceptionString);
            stream.Position = 0;

            var reader = new GraphBinaryReader();
            var serializer = new ResponseMessageSerializer();

            try
            {
                await foreach (var _ in serializer.ReadStreamingAsync(stream, reader))
                {
                    // Should not yield any items before throwing
                }

                return false; // Should have thrown ResponseException
            }
            catch (ResponseException ex)
            {
                // Verify StatusCode matches
                if (ex.StatusCode != statusCode) return false;

                // Verify Message matches: statusMessage if provided, otherwise default
                var expectedMessage = statusMessage ?? $"Server error: {statusCode}";
                if (ex.Message != expectedMessage) return false;

                // Verify ServerException matches
                if (ex.ServerException != exceptionString) return false;

                return true;
            }
        }

        /// <summary>
        ///     ToListAsync collects all results in order.
        ///
        ///     For any random list of int values, writing them to a channel,
        ///     constructing a ResultSet backed by that channel, and calling
        ///     ToListAsync returns a list matching the input in order and length.
        /// </summary>
        [Theory]
        [InlineData(0)]
        [InlineData(1)]
        [InlineData(42)]
        [InlineData(-1)]
        [InlineData(12345)]
        [InlineData(-99999)]
        [InlineData(int.MaxValue)]
        [InlineData(int.MinValue)]
        [InlineData(7777)]
        [InlineData(314159)]
        public async Task ToListAsyncCollectsAllResultsInOrder(int seed)
        {
            Assert.True(await ToListAsyncCollectsAllResultsInOrderAsync(seed));
        }

        private static async Task<bool> ToListAsyncCollectsAllResultsInOrderAsync(int seed)
        {
            var rng = new System.Random(seed);
            var count = rng.Next(0, 50);
            var inputValues = new List<int>(count);
            for (var i = 0; i < count; i++)
            {
                inputValues.Add(rng.Next(int.MinValue, int.MaxValue));
            }

            // Create a channel and write all values, then complete it
            var channel = Channel.CreateUnbounded<object>(
                new UnboundedChannelOptions { SingleWriter = true });
            foreach (var value in inputValues)
            {
                await channel.Writer.WriteAsync(value);
            }
            channel.Writer.Complete();

            // Create a ResultSet<int> backed by the channel
            using var disposeCts = new CancellationTokenSource();
            var backgroundTask = Task.CompletedTask;
            var resultSet = new ResultSet<int>(channel.Reader, disposeCts, backgroundTask);

            // Call ToListAsync and verify
            var result = await resultSet.ToListAsync();

            if (result.Count != inputValues.Count) return false;

            for (var i = 0; i < inputValues.Count; i++)
            {
                if (result[i] != inputValues[i]) return false;
            }

            return true;
        }

        /// <summary>
        ///     EOF detection in stream reads.
        ///
        ///     For any random byte array of length 0 to 7, attempting to read
        ///     int/long/short values from a stream with insufficient data throws
        ///     IOException or EndOfStreamException.
        /// </summary>
        [Theory]
        [InlineData(0)]
        [InlineData(1)]
        [InlineData(42)]
        [InlineData(-1)]
        [InlineData(12345)]
        [InlineData(-99999)]
        [InlineData(int.MaxValue)]
        [InlineData(int.MinValue)]
        [InlineData(7777)]
        [InlineData(314159)]
        public async Task EofDetectionInStreamReads(int seed)
        {
            Assert.True(await EofDetectionInStreamReadsAsync(seed));
        }

        private static async Task<bool> EofDetectionInStreamReadsAsync(int seed)
        {
            var rng = new System.Random(seed);
            var length = rng.Next(0, 8); // 0 to 7 bytes
            var data = new byte[length];
            rng.NextBytes(data);

            // ReadShortAsync needs 2 bytes — test with arrays of length 0-1
            if (length < 2)
            {
                if (!await ThrowsIOExceptionAsync(() =>
                    {
                        var stream = new MemoryStream(data);
                        return stream.ReadShortAsync().AsTask();
                    }))
                    return false;
            }

            // ReadIntAsync needs 4 bytes — test with arrays of length 0-3
            if (length < 4)
            {
                if (!await ThrowsIOExceptionAsync(() =>
                    {
                        var stream = new MemoryStream(data);
                        return stream.ReadIntAsync().AsTask();
                    }))
                    return false;
            }

            // ReadLongAsync needs 8 bytes — test with arrays of length 0-7
            if (length < 8)
            {
                if (!await ThrowsIOExceptionAsync(() =>
                    {
                        var stream = new MemoryStream(data);
                        return stream.ReadLongAsync().AsTask();
                    }))
                    return false;
            }

            // ReadByteAsync needs 1 byte — test with empty array
            if (length == 0)
            {
                if (!await ThrowsIOExceptionAsync(() =>
                    {
                        var stream = new MemoryStream(data);
                        return stream.ReadByteAsync().AsTask();
                    }))
                    return false;
            }

            return true;
        }

        /// <summary>
        ///     Helper that returns true if the given async action throws IOException
        ///     or EndOfStreamException (which is a subclass of IOException).
        /// </summary>
        private static async Task<bool> ThrowsIOExceptionAsync(Func<Task> action)
        {
            try
            {
                await action();
                return false; // Should have thrown
            }
            catch (IOException)
            {
                return true; // IOException or EndOfStreamException (subclass)
            }
        }

        /// <summary>
        ///     Writes a complete GraphBinary 4.0 response stream with an error status footer.
        ///     Contains no result data (marker immediately after bulked flag).
        /// </summary>
        private static async Task WriteErrorResponse(Stream stream, int statusCode,
            string? statusMessage, string? exceptionString)
        {
            // Version byte (0x84)
            await stream.WriteByteAsync(GraphBinaryWriter.VersionByte);
            // Bulked = false
            await stream.WriteByteAsync(0x00);

            // Marker immediately (no results): type_code(0xFD) + value_flag(0x00) + value(0x00)
            await stream.WriteByteAsync(0xFD);
            await stream.WriteByteAsync(0x00);
            await stream.WriteByteAsync(0x00);

            // Status footer: non-nullable int status code
            await stream.WriteIntAsync(statusCode);

            // Nullable status message
            await WriteNullableStringAsync(stream, statusMessage);

            // Nullable exception string
            await WriteNullableStringAsync(stream, exceptionString);
        }

        /// <summary>
        ///     Writes a nullable string in GraphBinary format:
        ///     value_flag(0x01) for null, or value_flag(0x00) + int32(length) + UTF-8 bytes.
        /// </summary>
        private static async Task WriteNullableStringAsync(Stream stream, string? value)
        {
            if (value == null)
            {
                await stream.WriteByteAsync(0x01); // null flag
            }
            else
            {
                await stream.WriteByteAsync(0x00); // not null flag
                var bytes = System.Text.Encoding.UTF8.GetBytes(value);
                await stream.WriteIntAsync(bytes.Length);
                if (bytes.Length > 0)
                {
                    await stream.WriteAsync(bytes);
                }
            }
        }

        /// <summary>
        ///     Writes a complete GraphBinary 4.0 response stream for non-bulked mode.
        /// </summary>
        private static async Task WriteNonBulkedResponse(Stream stream, GraphBinaryWriter writer,
            IReadOnlyList<PrimitiveValueHolder> values)
        {
            // Version byte (0x84)
            await stream.WriteByteAsync(GraphBinaryWriter.VersionByte);
            // Bulked = false
            await stream.WriteByteAsync(0x00);

            // Write each value as fully-qualified typed value
            foreach (var pv in values)
            {
                await writer.WriteAsync(pv.Value, stream);
            }

            // Marker: type_code(0xFD) + value_flag(0x00) + value(0x00)
            await stream.WriteByteAsync(0xFD);
            await stream.WriteByteAsync(0x00);
            await stream.WriteByteAsync(0x00);

            // Status footer: status code 200, null message, null exception
            await stream.WriteIntAsync(200);
            await stream.WriteByteAsync(0x01); // null message
            await stream.WriteByteAsync(0x01); // null exception
        }

        /// <summary>
        ///     Writes a complete GraphBinary 4.0 response stream for bulked mode.
        /// </summary>
        private static async Task WriteBulkedResponse(Stream stream, GraphBinaryWriter writer,
            IReadOnlyList<(PrimitiveValueHolder Value, long Bulk)> entries)
        {
            // Version byte (0x84)
            await stream.WriteByteAsync(GraphBinaryWriter.VersionByte);
            // Bulked = true
            await stream.WriteByteAsync(0x01);

            // Write each value followed by its bulk count as fully-qualified typed values
            foreach (var (pv, bulk) in entries)
            {
                await writer.WriteAsync(pv.Value, stream);
                await writer.WriteAsync(bulk, stream);
            }

            // Marker: type_code(0xFD) + value_flag(0x00) + value(0x00)
            await stream.WriteByteAsync(0xFD);
            await stream.WriteByteAsync(0x00);
            await stream.WriteByteAsync(0x00);

            // Status footer: status code 200, null message, null exception
            await stream.WriteIntAsync(200);
            await stream.WriteByteAsync(0x01); // null message
            await stream.WriteByteAsync(0x01); // null exception
        }

        /// <summary>
        ///     A wrapper for a single primitive value used in test generation.
        /// </summary>
        private class PrimitiveValueHolder
        {
            public object Value { get; }

            public PrimitiveValueHolder(object value)
            {
                Value = value;
            }

            public override string ToString() => $"{Value?.GetType().Name}({Value})";
        }

        /// <summary>
        ///     Generates a list of random primitive values using the given RNG.
        /// </summary>
        private static List<PrimitiveValueHolder> GenerateRandomPrimitives(System.Random rng, int count)
        {
            var result = new List<PrimitiveValueHolder>(count);
            for (var i = 0; i < count; i++)
            {
                result.Add(GenerateOnePrimitive(rng));
            }
            return result;
        }

        /// <summary>
        ///     Generates a single random primitive value from the set:
        ///     int, long, string (non-null), double (finite), float (finite), bool.
        /// </summary>
        private static PrimitiveValueHolder GenerateOnePrimitive(System.Random rng)
        {
            var typeChoice = rng.Next(6);
            return typeChoice switch
            {
                0 => new PrimitiveValueHolder(rng.Next(int.MinValue, int.MaxValue)),
                1 => new PrimitiveValueHolder((long)rng.Next(int.MinValue, int.MaxValue) * rng.Next(1, 100)),
                2 => new PrimitiveValueHolder(GenerateRandomString(rng)),
                3 => new PrimitiveValueHolder(GenerateFiniteDouble(rng)),
                4 => new PrimitiveValueHolder(GenerateFiniteFloat(rng)),
                5 => new PrimitiveValueHolder(rng.Next(2) == 0),
                _ => throw new InvalidOperationException()
            };
        }

        /// <summary>
        ///     Generates a random non-null string of printable ASCII characters.
        /// </summary>
        private static string GenerateRandomString(System.Random rng)
        {
            var length = rng.Next(0, 50);
            var chars = new char[length];
            for (var i = 0; i < length; i++)
            {
                chars[i] = (char)rng.Next(32, 127); // printable ASCII
            }
            return new string(chars);
        }

        /// <summary>
        ///     Generates a finite double value (no NaN or Infinity).
        /// </summary>
        private static double GenerateFiniteDouble(System.Random rng)
        {
            return (rng.NextDouble() - 0.5) * 2.0 * rng.Next(1, 10000);
        }

        /// <summary>
        ///     Generates a finite float value (no NaN or Infinity).
        /// </summary>
        private static float GenerateFiniteFloat(System.Random rng)
        {
            return (float)((rng.NextDouble() - 0.5) * 2.0 * rng.Next(1, 10000));
        }
    }
}
