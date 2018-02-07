using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Reactive;
using System.Reactive.Concurrency;
using System.Reactive.Linq;

namespace RTI.RxDDS
{
    [SuppressMessage("ReSharper", "NotResolvedInText")]
    public static class ObservableExtensions
    {
        public static IDisposable Subscribe<TSource>(
            this IObservable<TSource> source,
            DDS.TypedDataWriter<TSource> dw)
        {
            DDS.InstanceHandle_t instance_handle = DDS.InstanceHandle_t.HANDLE_NIL;
            return source.Subscribe(
                Observer.Create<TSource>(o => dw.write(o, ref instance_handle)));
        }

        public static IDisposable Subscribe<TSource>(
            this IObservable<TSource> source,
            DDS.TypedDataWriter<TSource> dw,
            Action onCompleted)
        {
            DDS.InstanceHandle_t instance_handle = DDS.InstanceHandle_t.HANDLE_NIL;
            return source.Subscribe(
                Observer.Create<TSource>(o => dw.write(o, ref instance_handle), onCompleted));
        }

        public static IObservable<TSource> DisposeAtEnd<TSource>(
            this IObservable<TSource> source,
            DDS.TypedDataWriter<TSource> dw,
            TSource instance)
        {
            DDS.InstanceHandle_t instance_handle = DDS.InstanceHandle_t.HANDLE_NIL;
            return source.Do(o => dw.write(o, ref instance_handle),
                ex => dw.dispose(instance, ref instance_handle),
                () => dw.dispose(instance, ref instance_handle));
        }

        public static IObservable<TSource> Shift<TSource>(
            this IObservable<TSource> source,
            long count,
            IScheduler scheduler)
        {
            return Observable.Create<TSource>(observer =>
            {
                var limited_queue = new FixedSizedQueue<TSource>(count);
                return source.Subscribe(value =>
                    {
                        if (limited_queue.Shift(value, out var outObj))
                            observer.OnNext(outObj);
                    },
                    observer.OnError,
                    /* Immediate abort semantics of Rx requires us to propagate
             errors immediately (i.e., without draining the queue).
          (Exception ex) => 
          {
            limited_queue
              .Subscribe(Observer
                         .Create<TSource>(data => observer.OnNext(data),
                                          () => observer.OnError(ex)), 
                                          scheduler);
          }*/
                    () => { limited_queue.ToObservable(scheduler).Subscribe(observer); });
            });
        }

        public static IObservable<TSource> Shift<TSource>(
            this IObservable<TSource> source,
            long count)
        {
            return Shift(source, count, Scheduler.Immediate);
        }

        public static IObservable<TAccumulate> RollingAggregate<TSource, TAccumulate>(
            this IObservable<TSource> source,
            TAccumulate seed,
            Func<TAccumulate, TSource, TAccumulate> accumulator)
        {
            return source.Scan(seed, accumulator);
        }

        public static IObservable<TAccumulate> WindowAggregate<TSource, TAccumulate>(
            this IObservable<TSource> source,
            long windowSize,
            TAccumulate seed,
            Func<TAccumulate, TSource, TSource, long, TAccumulate> accumulator,
            Func<TAccumulate, TSource, long, long, TAccumulate> windowDrainer)
        {
            if (accumulator == null)
                throw new ArgumentNullException("WindowAggregate: accumulator can't be null");
            if (windowDrainer == null)
                throw new ArgumentNullException("WindowAggregate: windowDrainer can't be null");

            return Observable.Create<TAccumulate>(observer =>
            {
                var limitedQueue = new FixedSizedQueue<TSource>(windowSize);

                return source.Subscribe(
                    value =>
                    {
                        try
                        {
                            seed = limitedQueue.Shift(value, out var outObj)
                                ? accumulator(seed, value, outObj, limitedQueue.Count)
                                : windowDrainer(seed, value, limitedQueue.Count, limitedQueue.Count - 1);
                            observer.OnNext(seed);
                        }
                        catch (Exception ex)
                        {
                            observer.OnError(ex);
                        }
                    },
                    observer.OnError,
                    () =>
                    {
                        try
                        {
                            long count = limitedQueue.Count;
                            while (limitedQueue.TryDequeue(out var outObj))
                            {
                                count--;
                                seed = windowDrainer(seed, outObj, count, count + 1);
                                observer.OnNext(seed);
                            }

                            if (count != 0)
                                observer.OnError(
                                    new ApplicationException(
                                        "WindowAggregte: OnCompleted: Unable to deque all elements"));
                            else
                                observer.OnCompleted();
                        }
                        catch (Exception ex)
                        {
                            observer.OnError(ex);
                        }
                    });
            });
        }

        public static IObservable<TAccumulate> TimeWindowAggregate<TSource, TAccumulate>(
            this IObservable<TSource> source,
            TimeSpan timespan,
            TAccumulate seed,
            Func<TAccumulate, TSource, IList<TSource>, long, TAccumulate> accumulator)
        {
            return TimeWindowAggregate(source, timespan, seed, accumulator, null, Scheduler.Immediate);
        }

        public static IObservable<TAccumulate> TimeWindowAggregate<TSource, TAccumulate>(
            this IObservable<TSource> source,
            TimeSpan timespan,
            TAccumulate seed,
            Func<TAccumulate, TSource, IList<TSource>, long, TAccumulate> accumulator,
            IScheduler scheduler)
        {
            return TimeWindowAggregate(source, timespan, seed, accumulator, null, scheduler);
        }

        public static IObservable<TAccumulate> TimeWindowAggregate<TSource, TAccumulate>(
            this IObservable<TSource> source,
            TimeSpan timespan,
            TAccumulate seed,
            Func<TAccumulate, TSource, IList<TSource>, long, TAccumulate> accumulator,
            Func<TSource, DateTimeOffset> timeKeeper)
        {
            return TimeWindowAggregate(source, timespan, seed, accumulator, timeKeeper, Scheduler.Immediate);
        }

        public static IObservable<TAccumulate> TimeWindowAggregate<TSource, TAccumulate>(
            this IObservable<TSource> source,
            TimeSpan timespan,
            TAccumulate seed,
            Func<TAccumulate, TSource, IList<TSource>, long, TAccumulate> accumulator,
            Func<TSource, DateTimeOffset> timeKeeper,
            IScheduler scheduler)
        {
            if (accumulator == null)
                throw new ArgumentNullException("TimeWindowAggregate: accumulator can't be null");

            return Observable.Create<TAccumulate>(observer =>
            {
                var timestampList = new List<TimeStamp<TSource>>();
                IList<TSource> expiredList = new List<TSource>();

                return source.Subscribe(value =>
                    {
                        try
                        {
                            var now = timeKeeper?.Invoke(value) ?? Scheduler.Now;

                            var exCount = 0;
                            expiredList.Clear();
                            foreach (var item in timestampList)
                                if (now - item.Timestamp > timespan)
                                {
                                    exCount++;
                                    expiredList.Add(item.Data);
                                }
                                else
                                {
                                    break;
                                }

                            timestampList.RemoveRange(0, exCount);
                            timestampList.Add(new TimeStamp<TSource>
                            {
                                Timestamp = now,
                                Data = value
                            });
                            seed = accumulator(seed, value, expiredList, timestampList.Count);
                            observer.OnNext(seed);
                        }
                        catch (Exception ex)
                        {
                            observer.OnError(ex);
                        }
                    },
                    observer.OnError,
                    observer.OnCompleted);
            });
        }

        public static IObservable<TSource> Do<TSource>(
            this IObservable<TSource> source,
            Action<TSource, long> action)
        {
            return Observable.Create<TSource>(observer =>
            {
                long count = 1;
                return source.Do(data => action(data, count++))
                    .Subscribe(observer);
            });
        }

        public static IObservable<TSource> DoIf<TSource>
        (this IObservable<TSource> source,
            Func<bool> conditional,
            Action<TSource> action)
        {
            return !conditional()
                ? source
                : Observable.Create<TSource>(observer => source.Do(action).Subscribe(observer));
        }

        public static IObservable<TSource> DoIf<TSource>
        (this IObservable<TSource> source,
            Func<bool> conditional,
            Action<TSource> action, Action onCompleted)
        {
            if (!conditional())
                return source;
            return Observable.Create<TSource>(observer => source
                .Do(action, onCompleted)
                .Subscribe(observer));
        }


        public static IDisposable OnDataAvailable<TSource>(
            this IObservable<TSource> source,
            Action<TSource> onNext,
            Action onCompleted)
        {
            return source.Subscribe(onNext, onCompleted);
        }

        public static IDisposable OnDataAvailable<TSource>(
            this IObservable<TSource> source,
            Action<TSource> onNext)
        {
            return source.Subscribe(onNext);
        }

        public static IDisposable OnDataAvailable<TSource>(
            this IObservable<TSource> source,
            DDS.TypedDataWriter<TSource> dw)
        {
            return source.Subscribe(dw);
        }

        public static IEnumerable<U> Scan<T, U>(this IEnumerable<T> input, Func<U, T, U> next, U state)
        {
            yield return state;
            foreach (var item in input)
            {
                state = next(state, item);
                yield return state;
            }
        }

        public static IObservable<IList<IGroupedObservable<Key, T>>> ActiveKeyScan<Key, T, Seed>(
            this IObservable<IGroupedObservable<Key, T>> source,
            Seed seed,
            Func<Seed, IList<IGroupedObservable<Key, T>>, Seed> aggregator)
        {
            return new ActiveKeyScanOp<Key, T, Seed>(source, seed, aggregator);
        }

        public static IObservable<T> Once<T>(this IObservable<T> source, T first, IScheduler scheduler)
        {
            return Observable.Create<T>(observer =>
            {
                scheduler.Schedule(() => observer.OnNext(first));
                return source.Subscribe(observer);
            });
        }

        public static IObservable<T> Once<T>(this IObservable<T> source, T first)
        {
            return Once(source, first, Scheduler.Immediate);
        }

        public static IObservable<T> Once<T>(this IObservable<T> source, Func<T> onceAction, IScheduler scheduler)
        {
            return Observable.Create<T>(observer =>
            {
                scheduler.Schedule(() => observer.OnNext(onceAction()));
                return source.Subscribe(observer);
            });
        }

        public static IObservable<T> Once<T>(this IObservable<T> source, Func<T> onceAction)
        {
            return Once(source, onceAction, Scheduler.Immediate);
        }

        internal struct TimeStamp<T>
        {
            public DateTimeOffset Timestamp;
            public T Data;
        }
    }
}