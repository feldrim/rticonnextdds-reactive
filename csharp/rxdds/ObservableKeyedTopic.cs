using System;
using System.Collections.Generic;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Reactive.Subjects;

namespace RTI.RxDDS
{
    class ObservableKeyedTopic<TKey, T> : IObservable<IGroupedObservable<TKey, T>>
        where T : class , DDS.ICopyable<T>, new()
    {
        public ObservableKeyedTopic(DDS.DomainParticipant participant,
            string topicName,
            string typeName,
            Func<T, TKey> keySelector,
            IEqualityComparer<TKey> comparer,
            IScheduler scheduler)
        {
            init(participant,
                topicName,
                typeName,
                keySelector,
                comparer,
                new Dictionary<TKey, DDSKeyedSubject<TKey, T>>(comparer),
                scheduler);

            externalSubDict = false;
        }

        public ObservableKeyedTopic(DDS.DomainParticipant participant,
            string topicName,
            string typeName,
            Func<T, TKey> keySelector,
            IEqualityComparer<TKey> comparer,
            Dictionary<TKey, DDSKeyedSubject<TKey, T>> subDict,
            IScheduler scheduler)
        {
            init(participant,
                topicName,
                typeName,
                keySelector,
                comparer,
                subDict,
                scheduler);

            externalSubDict = true;
        }

        private void init(DDS.DomainParticipant participant,
            string topicName,
            string typeName,
            Func<T, TKey> keySelector,
            IEqualityComparer<TKey> comparer,
            Dictionary<TKey, DDSKeyedSubject<TKey, T>> subDict,
            IScheduler scheduler)
        {
            mutex = new Object();

            if (scheduler == null)
                this.scheduler = Scheduler.Immediate;
            else
                this.scheduler = scheduler;

            if (typeName == null)
                this.typeName = typeof(T).ToString();
            else
                this.typeName = typeName;

            this.participant = participant;
            this.topicName = topicName;      
            this.keySelector = keySelector;
            this.comparer = comparer;
            this.keyedSubjectDict = subDict;
            this.handleKeyDict = new Dictionary<DDS.InstanceHandle_t, TKey>(new InstanceHandleComparer());

            if (this.scheduler == null ||
                this.typeName == null ||
                this.participant == null ||
                this.topicName == null ||
                this.keySelector == null ||
                this.comparer == null)
                throw new ApplicationException("Invalid Params");
        }

        public void Dispose()
        {
            listener.Dispose();
        }

        private void initializeDataReader(DDS.DomainParticipant participant)
        {
            DDS.Subscriber subscriber = participant.create_subscriber(
                DDS.DomainParticipant.SUBSCRIBER_QOS_DEFAULT,
                null,
                DDS.StatusMask.STATUS_MASK_NONE);
            if (subscriber == null)
            {
                throw new ApplicationException("create_subscriber error");
            }

            DDS.Topic topic = participant.create_topic(
                topicName,
                typeName,
                DDS.DomainParticipant.TOPIC_QOS_DEFAULT,
                null,
                DDS.StatusMask.STATUS_MASK_NONE);
            if (topic == null)
            {
                throw new ApplicationException("create_topic error");
            }

            listener = new InstanceDataReaderListener(
                groupSubject, keyedSubjectDict, keySelector, comparer, handleKeyDict, scheduler, externalSubDict);

            DDS.DataReaderQos r_qos = new DDS.DataReaderQos();
            participant.get_default_datareader_qos(r_qos);
            //Console.WriteLine("LIB CODE DR QOS: " + r_qos.history.kind);
            //Console.WriteLine("LIB CODE DR QOS: " + r_qos.reliability.kind);

            DDS.DataReader reader = subscriber.create_datareader(
                topic,
                r_qos,//DDS.Subscriber.DATAREADER_QOS_DEFAULT,
                listener,
                DDS.StatusMask.STATUS_MASK_ALL);
            if (reader == null)
            {
                listener = null;
                throw new ApplicationException("create_datareader error");
            }
        }

        private void initSubject()
        {
            lock (mutex)
            {
                if (groupSubject == null)
                {
                    groupSubject = new Subject<IGroupedObservable<TKey, T>>();
                    initializeDataReader(participant);
                }
            }
        }

        public IDisposable Subscribe(IObserver<IGroupedObservable<TKey, T>> observer)
        {
            initSubject();
            return groupSubject.Subscribe(observer);
        }

        public IDisposable Subscribe(Action<IGroupedObservable<TKey, T>> action)
        {
            initSubject();
            return groupSubject.Subscribe(action);
        }

        private class InstanceDataReaderListener : DataReaderListenerAdapter
        {
            public InstanceDataReaderListener(IObserver<IGroupedObservable<TKey, T>> observer,
                Dictionary<TKey, DDSKeyedSubject<TKey, T>> dict,
                Func<T, TKey> keySelector,
                IEqualityComparer<TKey> comparer,
                Dictionary<DDS.InstanceHandle_t, TKey> handleKeyDict,
                IScheduler sched,
                bool externalSubDict)
            {
                this.externalSubDict = externalSubDict;
                this.observer = observer;
                this.scheduler = sched;
                this.keyedSubDict = dict;
                this.keySelector = keySelector;
                this.comparer = comparer;
                this.handleKeyDict = handleKeyDict;
                this.dataSeq = new DDS.UserRefSequence<T>();
                this.infoSeq = new DDS.SampleInfoSeq();
            }

            public override void on_data_available(DDS.DataReader reader)
            {
                try
                {
                    DDS.TypedDataReader<T> dataReader =
                        (DDS.TypedDataReader<T>)reader;

                    dataReader.take(
                        dataSeq,
                        infoSeq,
                        DDS.ResourceLimitsQosPolicy.LENGTH_UNLIMITED,
                        DDS.SampleStateKind.ANY_SAMPLE_STATE,
                        DDS.ViewStateKind.ANY_VIEW_STATE,
                        DDS.InstanceStateKind.ANY_INSTANCE_STATE);

                    System.Int32 dataLength = dataSeq.length;
                    for (int i = 0; i < dataLength; ++i)
                    {
                        DDS.SampleInfo info = infoSeq.get_at(i);
                        if (info.valid_data)
                        {
                            T data = new T();
                            data.copy_from(dataSeq.get_at(i));
                            TKey key = keySelector(data);
                            DDSKeyedSubject<TKey, T> keyedSubject;

                            if (!keyedSubDict.ContainsKey(key))
                            {
                                keyedSubject = new DDSKeyedSubject<TKey, T>(key, scheduler);
                                keyedSubDict.Add(key, keyedSubject);
                                handleKeyDict.Add(info.instance_handle, key);
                                observer.OnNext(keyedSubject);
                            }
                            else
                            {
                                keyedSubject = keyedSubDict[key];
                                if (externalSubDict)
                                {
                                    if (!handleKeyDict.ContainsKey(info.instance_handle))
                                    {
                                        handleKeyDict.Add(info.instance_handle, key);
                                        observer.OnNext(keyedSubject);
                                    }
                                }
                            }
                            keyedSubject.OnNext(data);
                        }
                        else if (info.instance_state == DDS.InstanceStateKind.NOT_ALIVE_DISPOSED_INSTANCE_STATE)
                        {
                            if (handleKeyDict.ContainsKey(info.instance_handle))
                            {
                                TKey key = handleKeyDict[info.instance_handle];
                                if (keyedSubDict.ContainsKey(key))
                                {
                                    DDSKeyedSubject<TKey, T> keyedSub = keyedSubDict[key];
                                    keyedSubDict.Remove(key);
                                    handleKeyDict.Remove(info.instance_handle);
                                    keyedSub.OnCompleted();
                                    /* FIXME: If the instance comes alive again, it will break the Rx contract */
                                }
                                else
                                    Console.WriteLine("InstanceDataReaderListener invariant broken: keyedSubDict does not contain key");
                            }
                            else
                                Console.WriteLine("InstanceDataReaderListener invariant broken: handleKeyDict does not contain info.instance_handle");
                        }
                    }

                    dataReader.return_loan(dataSeq, infoSeq);
                }
                catch (DDS.Retcode_NoData)
                {
                    return;
                }
                catch (DDS.Exception ex)
                {
                    observer.OnError(ex);
                    Console.WriteLine("ObservableKeyedTopic: InstanceDataReaderListener: take error {0}", ex);
                }
            }

            private bool externalSubDict;
            private DDS.UserRefSequence<T> dataSeq;
            private DDS.SampleInfoSeq infoSeq;
            private IObserver<IGroupedObservable<TKey, T>> observer;
            private Dictionary<TKey, DDSKeyedSubject<TKey, T>> keyedSubDict;
            private Func<T, TKey> keySelector;
            private IEqualityComparer<TKey> comparer;
            private Dictionary<DDS.InstanceHandle_t, TKey> handleKeyDict;
            private IScheduler scheduler;
        }

        private bool externalSubDict;
        private Object mutex;
        private DDS.DomainParticipant participant;
        private string topicName;
        private string typeName;
        private InstanceDataReaderListener listener;
        private ISubject<IGroupedObservable<TKey, T>,
            IGroupedObservable<TKey, T>> groupSubject;
        private Dictionary<TKey, DDSKeyedSubject<TKey, T>> keyedSubjectDict;
        private Func<T, TKey> keySelector;
        private IEqualityComparer<TKey> comparer;
        private Dictionary<DDS.InstanceHandle_t, TKey> handleKeyDict;
        private IScheduler scheduler;
    };
}