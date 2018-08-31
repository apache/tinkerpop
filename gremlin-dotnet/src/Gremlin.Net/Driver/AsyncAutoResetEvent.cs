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
using System.Threading.Tasks;

// The implementation of this class is inspired by this blog post from Stephen Toub:
// https://blogs.msdn.microsoft.com/pfxteam/2012/02/11/building-async-coordination-primitives-part-2-asyncautoresetevent/

namespace Gremlin.Net.Driver
{
    /// <summary>
    ///     An async version of the AutoResetEvent.
    /// </summary>
    public class AsyncAutoResetEvent
    {
        private static readonly Task<bool> CompletedTask = Task.FromResult(true);
        private readonly List<TaskCompletionSource<bool>> _waitingTasks = new List<TaskCompletionSource<bool>>();
        private bool _isSignaled;
        
        /// <summary>
        ///     Asynchronously waits for this event to be set or until a timeout occurs.
        /// </summary>
        /// <param name="timeout">A <see cref="TimeSpan"/> that represents the number of milliseconds to wait.</param>
        /// <returns>true if the current instance received a signal before timing out; otherwise, false.</returns>
        public async Task<bool> WaitOneAsync(TimeSpan timeout)
        {
            var tcs = new TaskCompletionSource<bool>();
            var waitTask = WaitForSignalAsync(tcs);
            if (waitTask.IsCompleted) return waitTask.Result;
            
            await Task.WhenAny(waitTask, Task.Delay(timeout)).ConfigureAwait(false);
            if (waitTask.IsCompleted) return waitTask.Result;
            
            StopWaiting(tcs);

            return waitTask.Result;
        }

        private Task<bool> WaitForSignalAsync(TaskCompletionSource<bool> tcs)
        {
            lock (_waitingTasks)
            {
                if (_isSignaled)
                {
                    _isSignaled = false;
                    return CompletedTask;
                }
                _waitingTasks.Add(tcs);
            }
            return tcs.Task;
        }

        private void StopWaiting(TaskCompletionSource<bool> tcs)
        {
            lock (_waitingTasks)
            {
                _waitingTasks.Remove(tcs);
                tcs.SetResult(false);
            }
        }
        
        /// <summary>
        ///     Sets the event.
        /// </summary>
        public void Set()
        {
            TaskCompletionSource<bool> toRelease = null;
            lock (_waitingTasks)
            {
                if (_waitingTasks.Count == 0)
                {
                    _isSignaled = true;
                }
                else
                {
                    toRelease = _waitingTasks[0];
                    _waitingTasks.RemoveAt(0);
                }
            }

            toRelease?.SetResult(true);
        }
    }
}