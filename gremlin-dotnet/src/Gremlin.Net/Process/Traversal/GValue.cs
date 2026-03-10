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
    ///     Non-generic interface for GValue to allow type-agnostic access in GremlinLang.
    /// </summary>
    public interface IGValue
    {
        /// <summary>
        ///     Gets the parameter name.
        /// </summary>
        string Name { get; }

        /// <summary>
        ///     Gets the parameter value as an object.
        /// </summary>
        object? ObjectValue { get; }
    }

    /// <summary>
    ///     A named parameter wrapper that associates a user-defined name with a value.
    ///     GremlinLang renders the name in the gremlin string and stores the name-to-value
    ///     mapping in the parameters dictionary. Replaces the legacy Binding/Bindings mechanism.
    /// </summary>
    /// <typeparam name="T">The type of the parameter value.</typeparam>
    public class GValue<T> : IGValue, IEquatable<GValue<T>>
    {
        /// <summary>
        ///     Initializes a new instance of the <see cref="GValue{T}" /> class.
        /// </summary>
        /// <param name="name">The parameter name. Must be a valid identifier, not null, and not start with underscore.</param>
        /// <param name="value">The parameter value.</param>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="name" /> is null.</exception>
        /// <exception cref="ArgumentException">Thrown when <paramref name="name" /> is not a valid identifier.</exception>
        public GValue(string name, T value)
        {
            if (name == null)
                throw new ArgumentNullException(nameof(name), "The parameter name cannot be null.");

            if (name.Length == 0)
                throw new ArgumentException($"Invalid parameter name [{name}].");

            if (name[0] == '_')
                throw new ArgumentException($"Invalid GValue name {name}. Should not start with _.");

            if (!char.IsLetter(name[0]))
                throw new ArgumentException($"Invalid parameter name [{name}].");

            for (int i = 1; i < name.Length; i++)
            {
                if (!char.IsLetterOrDigit(name[i]))
                    throw new ArgumentException($"Invalid parameter name [{name}].");
            }

            Name = name;
            Value = value;
        }

        /// <summary>
        ///     Gets the parameter name.
        /// </summary>
        public string Name { get; }

        /// <summary>
        ///     Gets the parameter value.
        /// </summary>
        public T Value { get; }

        /// <inheritdoc />
        public object? ObjectValue => Value;

        /// <inheritdoc />
        public bool Equals(GValue<T>? other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Name == other.Name && Equals(Value, other.Value);
        }

        /// <inheritdoc />
        public override bool Equals(object? obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((GValue<T>)obj);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked
            {
                return (Name.GetHashCode() * 397) ^ (Value != null ? Value.GetHashCode() : 0);
            }
        }

        /// <inheritdoc />
        public override string ToString()
        {
            return $"GValue({Name}, {Value})";
        }
    }
}
