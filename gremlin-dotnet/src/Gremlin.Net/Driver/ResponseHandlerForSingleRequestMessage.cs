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
using Gremlin.Net.Driver.Messages;

namespace Gremlin.Net.Driver
{
    internal class ResponseHandlerForSingleRequestMessage<T> : IResponseHandlerForSingleRequestMessage
    {
        public Task<ResultSet<T>> Result => _tcs.Task;

        private readonly TaskCompletionSource<ResultSet<T>> _tcs =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        
        private readonly List<T> _result = new();

        public void HandleReceived(ResponseMessage<List<object>> received)
        {
            foreach (var d in received.Result.Data!)
            {
                _result.Add((T) d);
            }
        }

        public void Finalize(Dictionary<string, object> statusAttributes)
        {
            var resultSet =
                new ResultSet<T>(_result, statusAttributes);
            _tcs.TrySetResult(resultSet);
        }

        public void HandleFailure(Exception objException)
        {
            _tcs.TrySetException(objException);
        }

        public void Cancel()
        {
            _tcs.SetCanceled();
        }
    }
}