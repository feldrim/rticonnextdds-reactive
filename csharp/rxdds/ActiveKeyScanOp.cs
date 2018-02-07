using System;
using System.Collections.Generic;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Reactive.Subjects;

namespace RTI.RxDDS
{
    internal class ActiveKeyScanOp<TKey, T, TSeedType>
        : IObserver<IGroupedObservable<TKey, T>>,
            IObservable<IList<IGroupedObservable<TKey, T>>>
    {
        private readonly Func<TSeedType, IList<IGroupedObservable<TKey, T>>, TSeedType> _aggregator;
        private readonly object _guard;
        private readonly IObservable<IGroupedObservable<TKey, T>> _source;
        private readonly IList<IGroupedObservable<TKey, T>> _streamList;
        private readonly Subject<IList<IGroupedObservable<TKey, T>>> _subject;

        private bool _completed;
        private TSeedType _seed;

        public ActiveKeyScanOp(IObservable<IGroupedObservable<TKey, T>> source,
            TSeedType seed,
            Func<TSeedType, IList<IGroupedObservable<TKey, T>>, TSeedType> aggregator)
        {
            _aggregator = aggregator;
            _seed = seed;
            _source = source;
            _subject = new Subject<IList<IGroupedObservable<TKey, T>>>();
            _guard = new object();
            _streamList = new List<IGroupedObservable<TKey, T>>();
            _completed = false;
        }

        public virtual IDisposable Subscribe(IObserver<IList<IGroupedObservable<TKey, T>>> observer)
        {
            return
                new CompositeDisposable
                {
                    _subject.Subscribe(observer),
                    _source.Subscribe(this)
                };
        }

        public void OnNext(IGroupedObservable<TKey, T> value)
        {
            AddStream(value);
            value.Subscribe(data =>
                {
                    /* OnNext = No-op */
                },
                ex => DropStream(value),
                () => DropStream(value));
        }

        public void OnCompleted()
        {
            lock (_guard)
            {
                _completed = true;
                if (_streamList.Count == 0) _subject.OnCompleted();
            }
        }

        public void OnError(Exception error)
        {
            _subject.OnError(error);
        }

        private void AddStream(IGroupedObservable<TKey, T> value)
        {
            IList<IGroupedObservable<TKey, T>> list = null;
            lock (_guard)
            {
                try
                {
                    _streamList.Add(value);
                    list = new List<IGroupedObservable<TKey, T>>(_streamList);
                    _seed = _aggregator(_seed, list);
                }
                catch (Exception ex)
                {
                    _subject.OnError(ex);
                    return;
                }

                /* Ensure Rx contract: Not to invoke observers concurrently.
           Therefore, OnNext call is inside guard. */
                _subject.OnNext(list);
            }
        }

        private void DropStream(IGroupedObservable<TKey, T> value)
        {
            lock (_guard)
            {
                IList<IGroupedObservable<TKey, T>> list;
                try
                {
                    _streamList.Remove(value);
                    list = new List<IGroupedObservable<TKey, T>>(_streamList);
                    _seed = _aggregator(_seed, list);
                }
                catch (Exception ex)
                {
                    _subject.OnError(ex);
                    return;
                }

                /* Ensure Rx contract: Not to invoke observers concurrently.
           Therefore, OnNext call is inside guard. */
                _subject.OnNext(list);
                if (_completed) _subject.OnCompleted();
            }
        }
    }
}