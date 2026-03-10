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

namespace Gremlin.Net.Structure.IO.GraphBinary4
{
    /// <summary>
    /// Represents a special marker type for stream boundaries in GraphBinary 4.0 serialization.
    /// </summary>
    /// <remarks>
    /// Markers are used to indicate stream boundaries in GraphBinary 4.0 responses.
    /// The <see cref="EndOfStream"/> marker indicates the end of result data in a response,
    /// signaling that the footer (status code, message, exception) follows.
    /// </remarks>
    public sealed class Marker
    {
        /// <summary>
        /// Marker indicating the end of the result stream.
        /// </summary>
        /// <remarks>
        /// In GraphBinary 4.0 response format, this marker (type code 0xFD with value 0x00)
        /// separates the result data from the response footer.
        /// </remarks>
        public static readonly Marker EndOfStream = new Marker(0);

        /// <summary>
        /// Gets the byte value of this marker as used on the wire.
        /// </summary>
        public byte Value { get; }

        private Marker(byte value)
        {
            Value = value;
        }

        /// <summary>
        /// Returns the <see cref="Marker"/> instance for the given byte value.
        /// </summary>
        /// <param name="value">The marker byte value.</param>
        /// <returns>The corresponding <see cref="Marker"/> instance.</returns>
        /// <exception cref="ArgumentException">Thrown if the value is not a known marker.</exception>
        public static Marker Of(byte value)
        {
            if (value != 0)
            {
                throw new ArgumentException($"Unknown marker value: {value}", nameof(value));
            }
            return EndOfStream;
        }

        /// <inheritdoc />
        public override string ToString() => $"Marker({Value})";

        /// <inheritdoc />
        public override bool Equals(object? obj)
        {
            if (obj is null) return false;
            if (ReferenceEquals(this, obj)) return true;
            return obj is Marker other && Value == other.Value;
        }

        /// <inheritdoc />
        public override int GetHashCode() => Value.GetHashCode();
    }
}
