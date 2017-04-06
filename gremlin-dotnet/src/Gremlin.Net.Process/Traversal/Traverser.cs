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

namespace Gremlin.Net.Process.Traversal
{
    /// <summary>
    ///     A traverser represents the current state of an object flowing through a <see cref="ITraversal" />.
    /// </summary>
    public class Traverser
    {
        /// <summary>
        ///     Initializes a new instance of the <see cref="Traverser" /> class.
        /// </summary>
        /// <param name="obj">The object of the traverser.</param>
        /// <param name="bulk">The number of traversers represented in this traverser.</param>
        public Traverser(dynamic obj, long bulk = 1)
        {
            Object = obj;
            Bulk = bulk;
        }

        /// <summary>
        ///     Gets the object of this traverser.
        /// </summary>
        public dynamic Object { get; }

        /// <summary>
        ///     Gets the number of traversers represented in this traverser.
        /// </summary>
        public long Bulk { get; internal set; }

        /// <inheritdoc />
        public bool Equals(Traverser other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Equals(Object, other.Object);
        }

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((Traverser) obj);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            return Object != null ? Object.GetHashCode() : 0;
        }
    }
}