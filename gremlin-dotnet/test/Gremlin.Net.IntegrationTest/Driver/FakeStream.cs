using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Gremlin.Net.Structure.IO.GraphBinary;

namespace Gremlin.Net.IntegrationTest.Driver
{
    internal class FakeStream : Stream
    {
        private List<object> _results;

        public FakeStream(List<object> results)
        {
            _results = results;
        }

        public override bool CanRead => throw new NotImplementedException();

        public override bool CanSeek => throw new NotImplementedException();

        public override bool CanWrite => true;

        public override long Length => throw new NotImplementedException();

        public override long Position { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

        public override void Flush()
        {
            throw new NotImplementedException();
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            throw new NotImplementedException();
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotImplementedException();
        }

        public override void SetLength(long value)
        {
            throw new NotImplementedException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotImplementedException();
        }

        public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            var serializer = new GraphBinaryMessageSerializer();

            var m = await serializer.DeserializeMessageAsync(buffer.ToArray());

            // buffer.Span.ToArray().Select(a => a.ToString())
            _results.AddRange(m.Result.Data);
        }
    }
}
