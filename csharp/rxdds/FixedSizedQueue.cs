using System.Collections.Concurrent;

namespace RTI.RxDDS
{
    internal class FixedSizedQueue<T> : ConcurrentQueue<T>
    {
        public long Size { get; private set; }

        public FixedSizedQueue(long size)
        {
            Size = size;
        }

        public bool Shift(T obj, out T outObj)
        {
            base.Enqueue(obj);
            lock (this)
            {
                if ((base.Count > Size) &&
                    (base.TryDequeue(out outObj)))
                {
                    return true;
                }
                else
                {
                    outObj = default(T);
                    return false;
                }
            }
        }
    };
}