using System;
using System.Collections.Generic;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Reactive.Subjects;

namespace RTI.RxDDS
{
    class ObservableKeyedTopicWaitSet<TKey, T> : IObservable<IGroupedObservable<TKey, T>>
        where T : class , DDS.ICopyable<T>, new()
    {
        public ObservableKeyedTopicWaitSet(DDS.DomainParticipant participant,
            string topicName,
            string typeName,
            Func<T, TKey> keySelector,
            IEqualityComparer<TKey> comparer,
            DDS.Duration_t tmout)
        {
            init(participant,
                topicName,
                typeName,
                keySelector,
                comparer,
                new Dictionary<TKey, DDSKeyedSubject<TKey, T>>(comparer),
                tmout);

            externalSubDict = false;
        }

        public ObservableKeyedTopicWaitSet(DDS.DomainParticipant participant,
            string topicName,
            string typeName,
            Func<T, TKey> keySelector,
            IEqualityComparer<TKey> comparer,
            Dictionary<TKey, DDSKeyedSubject<TKey, T>> keySubDict,
            DDS.Duration_t tmout)
        {
            init(participant,
                topicName,
                typeName,
                keySelector,
                comparer,
                keySubDict,
                tmout);

            externalSubDict = true;
        }

        public void Dispose()
        {

        }

        private void init(DDS.DomainParticipant participant,
            string topicName,
            string typeName,
            Func<T, TKey> keySelector,
            IEqualityComparer<TKey> comparer,
            Dictionary<TKey, DDSKeyedSubject<TKey, T>> keySubDict,
            DDS.Duration_t tmout)
        {
            mutex = new Object();

            this.scheduler = new EventLoopScheduler();
            this.timeout = tmout;

            if (typeName == null)
                this.typeName = typeof(T).ToString();
            else
                this.typeName = typeName;

            this.participant = participant;
            this.topicName = topicName;
            this.keySelector = keySelector;
            this.comparer = comparer;
            this.keyedSubjectDict = keySubDict;
            this.handleKeyDict = new Dictionary<DDS.InstanceHandle_t, TKey>(new InstanceHandleComparer());

            if (this.scheduler == null ||
                this.typeName == null ||
                this.participant == null ||
                this.topicName == null ||
                this.keySelector == null ||
                this.keyedSubjectDict == null ||
                this.comparer == null)
            {
                throw new ArgumentNullException("ObservableTopic: Null parameters detected");
            }
        }

        private void initializeDataReader(DDS.DomainParticipant participant)
        {
            DDS.Subscriber subscriber = participant.create_subscriber(
                DDS.DomainParticipant.SUBSCRIBER_QOS_DEFAULT,
                null /* listener */,
                DDS.StatusMask.STATUS_MASK_NONE);
            if (subscriber == null)
            {
                throw new ApplicationException("create_subscriber error");
            }

            DDS.Topic topic = participant.create_topic(
                topicName,
                typeName,
                DDS.DomainParticipant.TOPIC_QOS_DEFAULT,
                null /* listener */,
                DDS.StatusMask.STATUS_MASK_NONE);
            if (topic == null)
            {
                throw new ApplicationException("create_topic error");
            }

            /* To customize the data reader QoS, use 
           the configuration file USER_QOS_PROFILES.xml */
            reader = subscriber.create_datareader(
                topic,
                DDS.Subscriber.DATAREADER_QOS_DEFAULT,
                null,
                DDS.StatusMask.STATUS_MASK_ALL);

            if (reader == null)
            {
                throw new ApplicationException("create_datareader error");
            }

            status_condition = reader.get_statuscondition();

            try
            {
                int mask =
                    (int)DDS.StatusKind.DATA_AVAILABLE_STATUS |
                    (int)DDS.StatusKind.SUBSCRIPTION_MATCHED_STATUS |
                    (int)DDS.StatusKind.LIVELINESS_CHANGED_STATUS |
                    (int)DDS.StatusKind.SAMPLE_LOST_STATUS |
                    (int)DDS.StatusKind.SAMPLE_REJECTED_STATUS;

                status_condition.set_enabled_statuses((DDS.StatusMask)mask);
            }
            catch (DDS.Exception e)
            {
                throw new ApplicationException("set_enabled_statuses error {0}", e);
            }

            waitset = new DDS.WaitSet();

            try
            {
                waitset.attach_condition(status_condition);
            }
            catch (DDS.Exception e)
            {
                throw new ApplicationException("attach_condition error {0}", e);
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
                    scheduler.Schedule(_ => { receiveData(); });
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

        public void Subscribe()
        {
            initSubject();
        }

        private void receiveData()
        {
            DDS.ConditionSeq active_conditions = new DDS.ConditionSeq();
            while (true)
            {
                try
                {
                    waitset.wait(active_conditions, timeout);
                    for (int c = 0; c < active_conditions.length; ++c)
                    {
                        if (active_conditions.get_at(c) == status_condition)
                        {
                            DDS.StatusMask triggeredmask =
                                reader.get_status_changes();

                            if ((triggeredmask &
                                 (DDS.StatusMask)
                                 DDS.StatusKind.DATA_AVAILABLE_STATUS) != 0)
                            {
                                try
                                {
                                    DDS.TypedDataReader<T> dataReader
                                        = (DDS.TypedDataReader<T>)reader;

                                    dataReader.take(
                                        dataSeq,
                                        infoSeq,
                                        DDS.ResourceLimitsQosPolicy.LENGTH_UNLIMITED,
                                        DDS.SampleStateKind.ANY_SAMPLE_STATE,
                                        DDS.ViewStateKind.ANY_VIEW_STATE,
                                        DDS.InstanceStateKind.ANY_INSTANCE_STATE);

                                    System.Int32 dataLength = dataSeq.length;
                                    //Console.WriteLine("Received {0}", dataLength);
                                    for (int i = 0; i < dataLength; ++i)
                                    {
                                        DDS.SampleInfo info = infoSeq.get_at(i);
                                        if (info.valid_data)
                                        {
                                            T data = new T();
                                            data.copy_from(dataSeq.get_at(i));
                                            TKey key = keySelector(data);
                                            DDSKeyedSubject<TKey, T> keyedSubject;

                                            if (!keyedSubjectDict.ContainsKey(key))
                                            {
                                                keyedSubject = new DDSKeyedSubject<TKey, T>(key, scheduler);
                                                keyedSubjectDict.Add(key, keyedSubject);
                                                handleKeyDict.Add(info.instance_handle, key);
                                                groupSubject.OnNext(keyedSubject);
                                            }
                                            else
                                            {
                                                keyedSubject = keyedSubjectDict[key];
                                                if (externalSubDict)
                                                {
                                                    if (!handleKeyDict.ContainsKey(info.instance_handle))
                                                    {
                                                        handleKeyDict.Add(info.instance_handle, key);
                                                        groupSubject.OnNext(keyedSubject);
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
                                                if (keyedSubjectDict.ContainsKey(key))
                                                {
                                                    DDSKeyedSubject<TKey, T> keyedSub = keyedSubjectDict[key];
                                                    keyedSubjectDict.Remove(key);
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
                                    subject.OnCompleted();
                                    return;
                                }
                                catch (Exception ex)
                                {
                                    subject.OnError(ex);
                                    Console.WriteLine("ObservableTopicWaitSet: take error {0}", ex);
                                }
                            }
                            else
                            {
                                StatusKindPrinter.print((int)triggeredmask);
                                if ((triggeredmask &
                                     (DDS.StatusMask)
                                     DDS.StatusKind.SUBSCRIPTION_MATCHED_STATUS) != 0)
                                {
                                    DDS.SubscriptionMatchedStatus status = new DDS.SubscriptionMatchedStatus();
                                    reader.get_subscription_matched_status(ref status);
                                    Console.WriteLine("Subscription matched. current_count = {0}", status.current_count);
                                }
                                if ((triggeredmask &
                                     (DDS.StatusMask)
                                     DDS.StatusKind.LIVELINESS_CHANGED_STATUS) != 0)
                                {
                                    DDS.LivelinessChangedStatus status = new DDS.LivelinessChangedStatus();
                                    reader.get_liveliness_changed_status(ref status);
                                    Console.WriteLine("Liveliness changed. alive_count = {0}", status.alive_count);
                                }
                                if ((triggeredmask &
                                     (DDS.StatusMask)
                                     DDS.StatusKind.SAMPLE_LOST_STATUS) != 0)
                                {
                                    DDS.SampleLostStatus status = new DDS.SampleLostStatus();
                                    reader.get_sample_lost_status(ref status);
                                    Console.WriteLine("Sample lost. Reason = {0}", status.last_reason.ToString());
                                }
                                if ((triggeredmask &
                                     (DDS.StatusMask)
                                     DDS.StatusKind.SAMPLE_REJECTED_STATUS) != 0)
                                {
                                    DDS.SampleRejectedStatus status = new DDS.SampleRejectedStatus();
                                    reader.get_sample_rejected_status(ref status);
                                    Console.WriteLine("Sample Rejected. Reason = {0}", status.last_reason.ToString());
                                }
                            }
                        }
                    }
                }
                catch (DDS.Retcode_Timeout)
                {
                    Console.WriteLine("wait timed out");
                    continue;
                }
            }
        }

        public IDisposable Subscribe(IObserver<T> observer)
        {
            lock (mutex)
            {
                if (subject == null)
                {
                    subject = new Subject<T>();
                    initializeDataReader(participant);
                    scheduler.Schedule(_ => { receiveData(); });
                }
            }

            return subject.Subscribe(observer);
        }

        private bool externalSubDict;
        private Object mutex;
        private DDS.DomainParticipant participant;
        private DDS.DataReader reader;
        private DDS.StatusCondition status_condition;
        private DDS.WaitSet waitset;
        private DDS.Duration_t timeout;
        private IScheduler scheduler;
        private string topicName;
        private string typeName;
        private Func<T, TKey> keySelector;
        private IEqualityComparer<TKey> comparer;
        private ISubject<T, T> subject;
        private Dictionary<TKey, DDSKeyedSubject<TKey, T>> keyedSubjectDict;
        private ISubject<IGroupedObservable<TKey, T>,
            IGroupedObservable<TKey, T>> groupSubject;
        private Dictionary<DDS.InstanceHandle_t, TKey> handleKeyDict;
        private DDS.UserRefSequence<T> dataSeq = new DDS.UserRefSequence<T>();
        private DDS.SampleInfoSeq infoSeq = new DDS.SampleInfoSeq();
    };
}