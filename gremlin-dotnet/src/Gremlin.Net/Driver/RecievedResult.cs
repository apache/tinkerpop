namespace Gremlin.Net.Driver
{
    internal sealed class RecievedMessageResult
    {
        private RecievedMessageResult(byte[] data, bool connectionClosed)
        {
            Data = data;
            ConnectionClosed = connectionClosed;
        }

        public static RecievedMessageResult Success(byte[] data) => new RecievedMessageResult(data, false);
        public static RecievedMessageResult Failed() => new RecievedMessageResult(null, true);

        public byte[] Data { get; }
        public bool ConnectionClosed { get; }
    }


}