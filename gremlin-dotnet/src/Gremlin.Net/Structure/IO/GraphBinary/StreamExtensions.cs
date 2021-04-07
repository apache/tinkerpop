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

namespace Gremlin.Net.Structure.IO.GraphBinary
{
    internal static class StreamExtensions
    {
        public static async Task WriteByteAsync(this Stream stream, byte value)
        {
            await stream.WriteAsync(new[] {value}, 0, 1).ConfigureAwait(false);
        }
        
        public static async Task<byte> ReadByteAsync(this Stream stream)
        {
            var readBuffer = new byte[1];
            await stream.ReadAsync(readBuffer, 0, 1);
            return readBuffer[0];
        }

        public static async Task WriteIntAsync(this Stream stream, int value)
        {
            var bytes = BitConverter.GetBytes(value);
            await stream.WriteAsync(new[] {bytes[3], bytes[2], bytes[1], bytes[0]}, 0, 4).ConfigureAwait(false);
        }
        
        public static async Task<int> ReadIntAsync(this Stream stream)
        {
            var bytes = new byte[4];
            await stream.ReadAsync(bytes, 0, 4).ConfigureAwait(false);
            return BitConverter.ToInt32(new []{bytes[3], bytes[2], bytes[1], bytes[0]}, 0);
        }
        
        public static async Task WriteLongAsync(this Stream stream, long value)
        {
            var bytes = BitConverter.GetBytes(value);
            await stream
                .WriteAsync(new[] {bytes[7], bytes[6], bytes[5], bytes[4], bytes[3], bytes[2], bytes[1], bytes[0]}, 0,
                    8).ConfigureAwait(false);
        }
        
        public static async Task<long> ReadLongAsync(this Stream stream)
        {
            var bytes = new byte[8];
            await stream.ReadAsync(bytes, 0, 8).ConfigureAwait(false);
            return BitConverter.ToInt64(
                new[] {bytes[7], bytes[6], bytes[5], bytes[4], bytes[3], bytes[2], bytes[1], bytes[0]}, 0);
        }
        
        public static async Task WriteFloatAsync(this Stream stream, float value)
        {
            var bytes = BitConverter.GetBytes(value);
            await stream.WriteAsync(new[] {bytes[3], bytes[2], bytes[1], bytes[0]}, 0, 4).ConfigureAwait(false);
        }
        
        public static async Task<float> ReadFloatAsync(this Stream stream)
        {
            var bytes = new byte[4];
            await stream.ReadAsync(bytes, 0, 4).ConfigureAwait(false);
            return BitConverter.ToSingle(new []{bytes[3], bytes[2], bytes[1], bytes[0]}, 0);
        }
        
        public static async Task WriteDoubleAsync(this Stream stream, double value)
        {
            var bytes = BitConverter.GetBytes(value);
            await stream
                .WriteAsync(new[] {bytes[7], bytes[6], bytes[5], bytes[4], bytes[3], bytes[2], bytes[1], bytes[0]}, 0,
                    8).ConfigureAwait(false);
        }
        
        public static async Task<double> ReadDoubleAsync(this Stream stream)
        {
            var bytes = new byte[8];
            await stream.ReadAsync(bytes, 0, 8).ConfigureAwait(false);
            return BitConverter.ToDouble(
                new[] {bytes[7], bytes[6], bytes[5], bytes[4], bytes[3], bytes[2], bytes[1], bytes[0]}, 0);
        }
        
        public static async Task WriteShortAsync(this Stream stream, short value)
        {
            var bytes = BitConverter.GetBytes(value);
            await stream.WriteAsync(new[] {bytes[1], bytes[0]}, 0, 2).ConfigureAwait(false);
        }
        
        public static async Task<short> ReadShortAsync(this Stream stream)
        {
            var bytes = new byte[2];
            await stream.ReadAsync(bytes, 0, 2).ConfigureAwait(false);
            return BitConverter.ToInt16(new []{bytes[1], bytes[0]}, 0);
        }
        
        public static async Task WriteBoolAsync(this Stream stream, bool value)
        {
            await stream.WriteByteAsync((byte) (value ? 1 : 0)).ConfigureAwait(false);
        }
        
        public static async Task<bool> ReadBoolAsync(this Stream stream)
        {
            var b = await stream.ReadByteAsync().ConfigureAwait(false);
            switch (b)
            {
                case 1:
                    return true;
                case 0:
                    return false;
                default:
                    throw new IOException($"Cannot read byte {b} as a boolean.");
            }
        }
        
        public static async Task WriteAsync(this Stream stream, byte[] value)
        {
            await stream.WriteAsync(value, 0, value.Length).ConfigureAwait(false);
        }

        public static async Task ReadAsync(this Stream stream, byte[] buffer)
        {
            await stream.ReadAsync(buffer, 0, buffer.Length).ConfigureAwait(false);
        }
    }
}