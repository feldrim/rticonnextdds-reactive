using System.Collections.Concurrent;

namespace RTI.RxDDS
{
    internal class FixedSizedQueue<T> : ConcurrentQueue<T>
    {
        public FixedSizedQueue(long size)
        {
            Size = size;
        }

        public long Size { get; }

        public bool Shift(T obj, out T outObj)
        {
            Enqueue(obj);
            lock (this)
            {
                if (Count > Size &&
                    TryDequeue(out outObj))
                    return true;

                outObj = default(T);
                return false;
            }
        }
    }
}