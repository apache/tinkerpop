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
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Gremlin.Net.Process.Traversal;

namespace Gremlin.Net.Structure
{
    /// <summary>
    ///     A Path denotes a particular walk through a graph as defined by a <see cref="ITraversal" />.
    /// </summary>
    /// <remarks>
    ///     In abstraction, any Path implementation maintains two lists: a list of sets of labels and a list of objects.
    ///     The list of labels are the labels of the steps traversed. The list of objects are the objects traversed.
    /// </remarks>
    public class Path : IReadOnlyList<object>, IEquatable<Path>
    {
        /// <summary>
        ///     Initializes a new instance of the <see cref="Path" /> class.
        /// </summary>
        /// <param name="labels">The labels associated with the path</param>
        /// <param name="objects">The objects in the <see cref="Path" />.</param>
        public Path(IList<ISet<string>> labels, IList<object> objects)
        {
            Labels = labels;
            Objects = objects;
        }

        /// <summary>
        ///     Gets an ordered list of the labels associated with the <see cref="Path" />.
        /// </summary>
        public IList<ISet<string>> Labels { get; }

        /// <summary>
        ///     Gets an ordered list of the objects in the <see cref="Path" />.
        /// </summary>
        public IList<object> Objects { get; }

        /// <summary>
        ///     Gets the object associated with the particular label of the path.
        /// </summary>
        /// <remarks>If the path has multiple labels of the type, then get a collection of those objects.</remarks>
        /// <param name="label">The label of the path</param>
        /// <returns>The object associated with the label of the path</returns>
        /// <exception cref="KeyNotFoundException">Thrown if the path does not contain the label.</exception>
        public object this[string label]
        {
            get
            {
                var objFound = TryGetValue(label, out object obj);
                if (!objFound)
                    throw new KeyNotFoundException($"The step with label {label} does not exist");
                return obj;
            }
        }

        /// <inheritdoc />
        public bool Equals(Path other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return ObjectsEqual(other.Objects) && LabelsEqual(other.Labels);
        }

        /// <summary>
        ///     Get the object associated with the specified index into the path.
        /// </summary>
        /// <param name="index">The index of the path</param>
        /// <returns>The object associated with the index of the path</returns>
        public dynamic this[int index] => Objects[index];

        /// <summary>
        ///     Gets the number of steps in the path.
        /// </summary>
        public int Count => Objects.Count;

        /// <inheritdoc />
        public IEnumerator<object> GetEnumerator()
        {
            return ((IReadOnlyList<object>) Objects).GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return ((IReadOnlyList<object>) Objects).GetEnumerator();
        }

        /// <inheritdoc />
        public override string ToString()
        {
            return $"[{string.Join(", ", Objects)}]";
        }

        /// <summary>
        ///     Returns true if the path has the specified label, else return false.
        /// </summary>
        /// <param name="key">The label to search for.</param>
        /// <returns>True if the label exists in the path.</returns>
        public bool ContainsKey(string key)
        {
            return Labels.Any(objLabels => objLabels.Contains(key));
        }

        /// <summary>
        ///     Tries to get the object associated with the particular label of the path.
        /// </summary>
        /// <remarks>If the path has multiple labels of the type, then get a collection of those objects.</remarks>
        /// <param name="label">The label of the path.</param>
        /// <param name="value">The object associated with the label of the path.</param>
        /// <returns>True, if an object was found for the label.</returns>
        public bool TryGetValue(string label, out object value)
        {
            value = null;
            for (var i = 0; i < Labels.Count; i++)
            {
                if (!Labels[i].Contains(label)) continue;
                if (value == null)
                    value = Objects[i];
                else if (value.GetType() == typeof(List<object>))
                    ((List<object>) value).Add(Objects[i]);
                else
                    value = new List<object> {value, Objects[i]};
            }
            return value != null;
        }

        private bool ObjectsEqual(ICollection<object> otherObjects)
        {
            if (Objects == null)
                return otherObjects == null;
            return Objects.SequenceEqual(otherObjects);
        }

        private bool LabelsEqual(ICollection<ISet<string>> otherLabels)
        {
            if (Labels == null)
                return otherLabels == null;
            if (Labels.Count != otherLabels.Count)
                return false;
            using (var enumOther = otherLabels.GetEnumerator())
            using (var enumThis = Labels.GetEnumerator())
            {
                while (enumOther.MoveNext() && enumThis.MoveNext())
                {
                    if (!enumOther.Current.SequenceEqual(enumThis.Current))
                    {
                        return false;
                    }
                }
            }
            return true;
        }

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((Path) obj);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = 19;
                if (Labels != null)
                    hashCode = Labels.Where(objLabels => objLabels != null)
                        .Aggregate(hashCode,
                            (current1, objLabels) => objLabels.Aggregate(current1,
                                (current, label) => current * 31 + label.GetHashCode()));
                if (Objects != null)
                    hashCode = Objects.Aggregate(hashCode, (current, obj) => current * 31 + obj.GetHashCode());
                return hashCode;
            }
        }
    }
}