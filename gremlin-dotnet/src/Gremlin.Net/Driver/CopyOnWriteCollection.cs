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

namespace Gremlin.Net.Driver
{
    internal class CopyOnWriteCollection<T>
    {
        private static readonly T[] EmptyArray = new T[0];
        
        private volatile T[] _array = EmptyArray;
        private readonly object _writeLock = new object();

        public int Count => _array.Length;

        public void AddRange(T[] items)
        {
            if (items == null || items.Length == 0)
                return;
            lock (_writeLock)
            {
                var newArray = new T[_array.Length + items.Length];
                _array.CopyTo(newArray, 0);
                Array.Copy(items, 0, newArray, _array.Length, items.Length);
                _array = newArray;
            }
        }

        public bool TryRemove(T item)
        {
            lock (_writeLock)
            {
                var index = Array.IndexOf(_array, item);
                if (index < 0) return false;
                if (_array.Length == 1 && index == 0)
                {
                    _array = EmptyArray;
                    return true;
                }

                var newArray = new T[_array.Length - 1];
                    
                if (index != 0)
                    Array.Copy(_array, 0, newArray, 0, index);
                if (index < _array.Length - 1)
                    Array.Copy(_array, index + 1, newArray, index, _array.Length - index - 1);
                _array = newArray;
                return true;
            }
        }

        public T[] RemoveAndGetAll()
        {
            lock (_writeLock)
            {
                var old = _array;
                _array = EmptyArray;
                return old;
            }
        }

        public T[] Snapshot => _array;
    }
}