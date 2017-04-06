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

namespace Gremlin.Net.Structure
{
    /// <summary>
    ///     A <see cref="Property" /> denotes a key/value pair associated with an <see cref="Edge" />.
    /// </summary>
    public class Property : IEquatable<Property>
    {
        /// <summary>
        ///     Initializes a new instance of the <see cref="Property" /> class.
        /// </summary>
        /// <param name="key">The key of the property.</param>
        /// <param name="value">The value of the property.</param>
        /// <param name="element">The element that the property is associated with.</param>
        public Property(string key, dynamic value, Element element)
        {
            Key = key;
            Value = value;
            Element = element;
        }

        /// <summary>
        ///     Gets the key of the property.
        /// </summary>
        public string Key { get; }

        /// <summary>
        ///     Gets the value of the property.
        /// </summary>
        public dynamic Value { get; }

        /// <summary>
        ///     Gets the element that this property is associated with.
        /// </summary>
        public Element Element { get; }

        /// <inheritdoc />
        public bool Equals(Property other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return string.Equals(Key, other.Key) && Equals(Value, other.Value) && Equals(Element, other.Element);
        }

        /// <inheritdoc />
        public override string ToString()
        {
            return $"p[{Key}->{Value}]";
        }

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((Property) obj);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = Key?.GetHashCode() ?? 0;
                hashCode = (hashCode * 397) ^ (Value != null ? Value.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (Element?.GetHashCode() ?? 0);
                return hashCode;
            }
        }
    }
}