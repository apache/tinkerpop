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
using System.Runtime.ExceptionServices;
using System.Threading.Tasks;

namespace Gremlin.Net.Process
{
    /// <summary>
    /// Contains useful methods that can be reused across the project. 
    /// </summary>
    internal static class Utils
    {
        /// <summary>
        /// Waits the completion of the provided Task.
        /// When an AggregateException is thrown, the inner exception is thrown.
        /// </summary>
        public static void WaitUnwrap(this Task task)
        {
            try
            {
                task.Wait();
            }
            catch (AggregateException ex)
            {
                if (ex.InnerExceptions.Count == 1)
                {
                    ExceptionDispatchInfo.Capture(ex.InnerException).Throw();   
                }
                throw;
            }
        }
    }
}