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
 *   http://www.apache.org/licenses/LICENSE-2.0
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

namespace Gremlin.Net.Driver
{
    /// <summary>
    ///     The compression algorithm requested for server responses. The server currently
    ///     supports <see cref="Deflate"/> only; additional members are reserved for when the
    ///     server adds support for them (server-side first).
    /// </summary>
    public enum CompressionType
    {
        /// <summary>
        ///     No compression.
        /// </summary>
        None,

        /// <summary>
        ///     Deflate compression.
        /// </summary>
        Deflate
    }

    /// <summary>
    ///     Configures response compression. A <see cref="CompressionType"/> is implicitly
    ///     convertible (<see cref="CompressionType.Deflate"/> = <see cref="Deflate"/>,
    ///     <see cref="CompressionType.None"/> = <see cref="None"/>).
    /// </summary>
    public readonly struct Compression : IEquatable<Compression>
    {
        /// <summary>
        ///     Gets the configured compression algorithm.
        /// </summary>
        public CompressionType Type { get; }

        /// <summary>
        ///     Initializes a new instance of the <see cref="Compression"/> struct.
        /// </summary>
        /// <param name="type">The compression algorithm.</param>
        public Compression(CompressionType type)
        {
            Type = type;
        }

        /// <summary>
        ///     No compression.
        /// </summary>
        public static Compression None => new Compression(CompressionType.None);

        /// <summary>
        ///     Deflate compression.
        /// </summary>
        public static Compression Deflate => new Compression(CompressionType.Deflate);

        /// <summary>
        ///     Gets whether compression is enabled (i.e. the algorithm is not <see cref="CompressionType.None"/>).
        /// </summary>
        public bool Enabled => Type != CompressionType.None;

        /// <summary>
        ///     Implicitly converts a <see cref="CompressionType"/> to a <see cref="Compression"/>.
        /// </summary>
        /// <param name="type">The compression algorithm.</param>
        public static implicit operator Compression(CompressionType type) =>
            new Compression(type);

        /// <inheritdoc />
        public bool Equals(Compression other) => Type == other.Type;

        /// <inheritdoc />
        public override bool Equals(object? obj) => obj is Compression other && Equals(other);

        /// <inheritdoc />
        public override int GetHashCode() => (int)Type;

        /// <summary>Determines whether two <see cref="Compression"/> values are equal.</summary>
        public static bool operator ==(Compression left, Compression right) => left.Equals(right);

        /// <summary>Determines whether two <see cref="Compression"/> values are not equal.</summary>
        public static bool operator !=(Compression left, Compression right) => !left.Equals(right);

        /// <inheritdoc />
        public override string ToString() => Type.ToString();
    }
}
