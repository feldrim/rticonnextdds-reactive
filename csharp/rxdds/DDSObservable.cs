using System;
using System.Collections.Generic;
using System.Reactive.Concurrency;
using System.Reactive.Linq;

namespace RTI.RxDDS
{
    public static class DDSObservable
    {
        public static IObservable<T> FromTopicWaitSet<T>(DDS.DomainParticipant participant,
            string topicName,
            DDS.Duration_t timeout)
            where T : class , DDS.ICopyable<T>, new()
        {
            string type_name = null;
            var ddsObservable = new ObservableTopicWaitSet<T>(participant, topicName, type_name, timeout);
            return ddsObservable;
        }

        public static IObservable<IGroupedObservable<TKey, T>>
            FromKeyedTopicWaitSet<TKey, T>(DDS.DomainParticipant participant,
                string topicName,
                string typeName,
                Func<T, TKey> keySelector,
                IEqualityComparer<TKey> keyComparer,
                DDS.Duration_t timeout)
            where T : class , DDS.ICopyable<T>, new()
        {
            var ddsObservable =
                new ObservableKeyedTopicWaitSet<TKey, T>(participant, topicName, typeName, keySelector, keyComparer, timeout);
            return ddsObservable;
        }

        public static IObservable<IGroupedObservable<TKey, T>>
            FromKeyedTopicWaitSet<TKey, T>(DDS.DomainParticipant participant,
                string topicName,
                Func<T, TKey> keySelector,
                IEqualityComparer<TKey> keyComparer,
                DDS.Duration_t timeout)
            where T : class , DDS.ICopyable<T>, new()
        {
            string typeName = null;
            var ddsObservable =
                new ObservableKeyedTopicWaitSet<TKey, T>(participant, topicName, typeName, keySelector, keyComparer, timeout);
            return ddsObservable;
        }

        public static IObservable<IGroupedObservable<TKey, T>>
            FromKeyedTopicWaitSet<TKey, T>(DDS.DomainParticipant participant,
                string topicName,
                string typeName,
                Func<T, TKey> keySelector,
                DDS.Duration_t timeout)
            where T : class , DDS.ICopyable<T>, new()
        {
            var ddsObservable =
                new ObservableKeyedTopicWaitSet<TKey, T>(participant, topicName, typeName, keySelector,
                    EqualityComparer<TKey>.Default, timeout);
            return ddsObservable;
        }

        public static IObservable<IGroupedObservable<TKey, T>>
            FromKeyedTopicWaitSet<TKey, T>(DDS.DomainParticipant participant,
                string topicName,
                Func<T, TKey> keySelector,
                DDS.Duration_t timeout)
            where T : class , DDS.ICopyable<T>, new()
        {
            string typeName = null;
            var ddsObservable =
                new ObservableKeyedTopicWaitSet<TKey, T>(participant, topicName, typeName, keySelector,
                    EqualityComparer<TKey>.Default, timeout);
            return ddsObservable;
        }

        public static IObservable<IGroupedObservable<TKey, T>>
            FromKeyedTopicWaitSet<TKey, T>(DDS.DomainParticipant participant,
                string topicName,
                Func<T, TKey> keySelector,
                Dictionary<TKey, DDSKeyedSubject<TKey, T>> subjectDict,
                DDS.Duration_t timeout)
            where T : class , DDS.ICopyable<T>, new()
        {
            string typeName = null;
            var ddsObservable =
                new ObservableKeyedTopicWaitSet<TKey, T>(participant, topicName, typeName, keySelector,
                    EqualityComparer<TKey>.Default, subjectDict, timeout);
            return ddsObservable;
        }

        public static IObservable<T> FromTopic<T>(DDS.DomainParticipant participant,
            string topicName,
            IScheduler subscribeOnScheduler = null)
            where T : class , DDS.ICopyable<T>, new()
        {
            var ddsObservable = new ObservableTopic<T>(participant, topicName, null, Scheduler.Immediate);
            if (subscribeOnScheduler == null)
                return ddsObservable;
            else
                return ddsObservable.SubscribeOn(subscribeOnScheduler);
        }

        public static IObservable<T> FromTopic<T>(DDS.DomainParticipant participant,
            string topicName,
            string typeName,
            IScheduler subscribeOnScheduler = null)
            where T : class , DDS.ICopyable<T>, new()
        {
            var ddsObservable = new ObservableTopic<T>(participant, topicName, typeName, Scheduler.Immediate);
            if (subscribeOnScheduler == null)
                return ddsObservable;
            else
                return ddsObservable.SubscribeOn(subscribeOnScheduler);
        }

        public static IObservable<IGroupedObservable<TKey, T>>
            FromKeyedTopic<TKey, T>(DDS.DomainParticipant participant,
                string topicName,
                Func<T, TKey> keySelector,
                IScheduler subscribeOnScheduler = null)
            where T : class , DDS.ICopyable<T>, new()
        {
            var ddsObservable =
                new ObservableKeyedTopic<TKey, T>(participant, topicName, null, keySelector, EqualityComparer<TKey>.Default, Scheduler.Immediate);
            if (subscribeOnScheduler == null)
                return ddsObservable;
            else
                return ddsObservable.SubscribeOn(subscribeOnScheduler);
        }

        public static IObservable<IGroupedObservable<TKey, T>>
            FromKeyedTopic<TKey, T>(DDS.DomainParticipant participant,
                string topicName,
                string typeName,
                Func<T, TKey> keySelector,
                IScheduler subscribeOnScheduler = null)
            where T : class , DDS.ICopyable<T>, new()
        {
            var ddsObservable =
                new ObservableKeyedTopic<TKey, T>(participant, topicName, typeName, keySelector, EqualityComparer<TKey>.Default, Scheduler.Immediate);
            if (subscribeOnScheduler == null)
                return ddsObservable;
            else
                return ddsObservable.SubscribeOn(subscribeOnScheduler);
        }

        public static IObservable<IGroupedObservable<TKey, T>>
            FromKeyedTopic<TKey, T>(DDS.DomainParticipant participant,
                string topicName,
                Func<T, TKey> keySelector,
                IEqualityComparer<TKey> keyComparer,
                IScheduler subscribeOnScheduler = null)
            where T : class , DDS.ICopyable<T>, new()
        {
            var ddsObservable =
                new ObservableKeyedTopic<TKey, T>(participant, topicName, null, keySelector, keyComparer, Scheduler.Immediate);
            if (subscribeOnScheduler == null)
                return ddsObservable;
            else
                return ddsObservable.SubscribeOn(subscribeOnScheduler);
        }
        public static IObservable<IGroupedObservable<TKey, T>>
            FromKeyedTopic<TKey, T>(DDS.DomainParticipant participant,
                string topicName,
                string typeName,
                Func<T, TKey> keySelector,
                IEqualityComparer<TKey> keyComparer,
                IScheduler subscribeOnScheduler = null)
            where T : class , DDS.ICopyable<T>, new()
        {
            var ddsObservable =
                new ObservableKeyedTopic<TKey, T>(participant, topicName, typeName, keySelector, keyComparer, Scheduler.Immediate);
            if (subscribeOnScheduler == null)
                return ddsObservable;
            else
                return ddsObservable.SubscribeOn(subscribeOnScheduler);
        }
    };
}