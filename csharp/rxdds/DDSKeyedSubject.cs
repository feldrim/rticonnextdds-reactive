using System;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Reactive.Subjects;

namespace RTI.RxDDS
{
    public class DDSKeyedSubject<TKey, T> : IGroupedObservable<TKey, T>, IObserver<T>
    {
        public TKey Key
        {
            get
            {
                return this.key;
            }
        }

        public DDSKeyedSubject(TKey key, IScheduler scheduler = null)
        {
            this.key = key;
            this.scheduler = scheduler;
            this.sub = new Subject<T>();
        }
        public void OnNext(T value)
        {
            //scheduler.Schedule(() => sub.OnNext(value));
            sub.OnNext(value);
        }
        public void OnCompleted()
        {
            //scheduler.Schedule(() => sub.OnCompleted());
            sub.OnCompleted();
        }
        public void OnError(Exception ex)
        {
            //scheduler.Schedule(() => sub.OnError(ex));
            sub.OnError(ex);
        }
        public IDisposable Subscribe(IObserver<T> observer)
        {
            return sub.Subscribe(observer);
        }

        private TKey key;
        private ISubject<T, T> sub;
        private IScheduler scheduler;
    };
}