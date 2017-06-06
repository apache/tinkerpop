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

namespace Gremlin.Net.Process.Traversal
{
    /// <summary>
    ///     Associates a variable with a value.
    /// </summary>
    public class Binding : IEquatable<Binding>
    {
        /// <summary>
        ///     Initializes a new instance of the <see cref="Binding" /> class.
        /// </summary>
        /// <param name="key">The key that identifies the <see cref="Binding" />.</param>
        /// <param name="value">The value of the <see cref="Binding" />.</param>
        public Binding(string key, object value)
        {
            Key = key;
            Value = value;
        }

        /// <summary>
        ///     Gets the key that identifies the <see cref="Binding" />.
        /// </summary>
        public string Key { get; }

        /// <summary>
        ///     Gets the value of the <see cref="Binding" />.
        /// </summary>
        public object Value { get; }

        /// <inheritdoc />
        public bool Equals(Binding other)
        {
            if (other == null)
                return false;
            return Key == other.Key && Value.Equals(other.Value);
        }

        /// <inheritdoc />
        public override bool Equals(object other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            if (other.GetType() != GetType()) return false;
            return Equals(other as Binding);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked
            {
                return ((Key?.GetHashCode() ?? 0) * 397) ^ (Value?.GetHashCode() ?? 0);
            }
        }
    }
}