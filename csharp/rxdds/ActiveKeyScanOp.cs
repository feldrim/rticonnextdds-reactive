using System;
using System.Collections.Generic;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Reactive.Subjects;

namespace RTI.RxDDS
{
    class ActiveKeyScanOp<Key, T, SeedType>
        :  IObserver<IGroupedObservable<Key, T>> ,
            IObservable<IList<IGroupedObservable<Key, T>>>
    {
        public ActiveKeyScanOp(IObservable<IGroupedObservable<Key, T>> source,
            SeedType seed,
            Func<SeedType, IList<IGroupedObservable<Key, T>>, SeedType> aggregator)
        {
            this.aggregator = aggregator;
            this.seed = seed;
            this.source = source;
            subject = new Subject<IList<IGroupedObservable<Key, T>>>();
            guard = new Object();
            streamList = new List<IGroupedObservable<Key, T>>();
            completed = false;
        }

        public void OnNext(IGroupedObservable<Key, T> value)
        {
            AddStream(value);
            value.Subscribe(data => { /* OnNext = No-op */},
                (Exception ex) => DropStream(value),
                () => DropStream(value));
        }

        public void OnCompleted()
        {
            lock(guard) {
                completed = true;
                if (streamList.Count == 0)
                {
                    subject.OnCompleted();
                }
            }
        }

        public void OnError(Exception error)
        {
            subject.OnError(error);
        }

        public virtual IDisposable Subscribe(IObserver<IList<IGroupedObservable<Key, T>>> observer)
        {
            return
                new CompositeDisposable()
                {  
                    subject.Subscribe(observer),
                    source.Subscribe(this)
                };
        }

        private void AddStream(IGroupedObservable<Key, T> value)
        {
            IList<IGroupedObservable<Key, T>> list = null;
            lock (guard)
            {
                try
                {
                    streamList.Add(value);
                    list = new List<IGroupedObservable<Key, T>>(streamList);
                    this.seed = aggregator(this.seed, list);
                }
                catch (Exception ex)
                {
                    this.subject.OnError(ex);
                    return;
                }
                /* Ensure Rx contract: Not to invoke observers concurrently.
           Therefore, OnNext call is inside guard. */
                subject.OnNext(list);
            }
        }

        private void DropStream(IGroupedObservable<Key, T> value)
        {
            IList<IGroupedObservable<Key, T>> list = null;
            lock (guard)
            {
                try
                {
                    streamList.Remove(value);
                    list = new List<IGroupedObservable<Key, T>>(streamList);
                    seed = aggregator(this.seed, list);
                }
                catch(Exception ex)
                {
                    this.subject.OnError(ex);
                    return;
                }
                /* Ensure Rx contract: Not to invoke observers concurrently.
           Therefore, OnNext call is inside guard. */
                subject.OnNext(list);
                if (completed == true)
                {
                    subject.OnCompleted();
                }
            }
        }

        private bool completed;
        private Object guard;
        private SeedType seed;
        private IList<IGroupedObservable<Key, T>> streamList;
        private Func<SeedType, IList<IGroupedObservable<Key, T>>, SeedType> aggregator;
        private IObservable<IGroupedObservable<Key, T>> source;
        private Subject<IList<IGroupedObservable<Key, T>>> subject;
    };
}