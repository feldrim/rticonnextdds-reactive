using System;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Reactive.Subjects;

namespace RTI.RxDDS
{
    public class DDSKeyedSubject<TKey, T> : IGroupedObservable<TKey, T>, IObserver<T>
    {
        private IScheduler _scheduler;
        private readonly ISubject<T, T> _sub;

        public DDSKeyedSubject(TKey key, IScheduler scheduler = null)
        {
            Key = key;
            _scheduler = scheduler;
            _sub = new Subject<T>();
        }

        public TKey Key { get; }

        public IDisposable Subscribe(IObserver<T> observer)
        {
            return _sub.Subscribe(observer);
        }

        public void OnNext(T value)
        {
            //_scheduler.Schedule(() => sub.OnNext(value));
            _sub.OnNext(value);
        }

        public void OnCompleted()
        {
            //_scheduler.Schedule(() => sub.OnCompleted());
            _sub.OnCompleted();
        }

        public void OnError(Exception ex)
        {
            //_scheduler.Schedule(() => sub.OnError(ex));
            _sub.OnError(ex);
        }
    }
}