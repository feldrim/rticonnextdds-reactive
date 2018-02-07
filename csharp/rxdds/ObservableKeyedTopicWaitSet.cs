using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Reactive.Subjects;

namespace RTI.RxDDS
{
    [SuppressMessage("ReSharper", "NotResolvedInText")]
    internal class ObservableKeyedTopicWaitSet<TKey, T> : IObservable<IGroupedObservable<TKey, T>>
        where T : class, DDS.ICopyable<T>, new()
    {
        private readonly DDS.UserRefSequence<T> _dataSeq = new DDS.UserRefSequence<T>();

        private readonly bool _externalSubDict;
        private readonly DDS.SampleInfoSeq _infoSeq = new DDS.SampleInfoSeq();
        private IEqualityComparer<TKey> _comparer;

        private ISubject<IGroupedObservable<TKey, T>,
            IGroupedObservable<TKey, T>> _groupSubject;

        private Dictionary<DDS.InstanceHandle_t, TKey> _handleKeyDict;
        private Dictionary<TKey, DDSKeyedSubject<TKey, T>> _keyedSubjectDict;
        private Func<T, TKey> _keySelector;
        private object _mutex;
        private DDS.DomainParticipant _participant;
        private DDS.DataReader _reader;
        private IScheduler _scheduler;
        private DDS.StatusCondition _statusCondition;
        private ISubject<T, T> _subject;
        private DDS.Duration_t _timeout;
        private string _topicName;
        private string _typeName;
        private DDS.WaitSet _waitset;

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

            _externalSubDict = false;
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

            _externalSubDict = true;
        }

        public IDisposable Subscribe(IObserver<IGroupedObservable<TKey, T>> observer)
        {
            initSubject();
            lock (_mutex)
            {
                return _groupSubject.Subscribe(observer);
            }
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
            _mutex = new object();

            _scheduler = new EventLoopScheduler();
            _timeout = tmout;

            _typeName = typeName ?? typeof(T).ToString();

            _participant = participant;
            _topicName = topicName;
            _keySelector = keySelector;
            _comparer = comparer;
            _keyedSubjectDict = keySubDict;
            _handleKeyDict = new Dictionary<DDS.InstanceHandle_t, TKey>(new InstanceHandleComparer());

            if (_scheduler == null ||
                _typeName == null ||
                _participant == null ||
                _topicName == null ||
                _keySelector == null ||
                _keyedSubjectDict == null ||
                _comparer == null)
                throw new ArgumentNullException("ObservableTopic: Null parameters detected");
        }

        private void initializeDataReader(DDS.DomainParticipant participant)
        {
            DDS.Subscriber subscriber = participant.create_subscriber(
                DDS.DomainParticipant.SUBSCRIBER_QOS_DEFAULT,
                null /* listener */,
                DDS.StatusMask.STATUS_MASK_NONE);
            if (subscriber == null) throw new ApplicationException("create_subscriber error");

            DDS.Topic topic = participant.create_topic(
                _topicName,
                _typeName,
                DDS.DomainParticipant.TOPIC_QOS_DEFAULT,
                null /* listener */,
                DDS.StatusMask.STATUS_MASK_NONE);
            if (topic == null) throw new ApplicationException("create_topic error");

            /* To customize the data reader QoS, use 
           the configuration file USER_QOS_PROFILES.xml */
            _reader = subscriber.create_datareader(
                topic,
                DDS.Subscriber.DATAREADER_QOS_DEFAULT,
                null,
                DDS.StatusMask.STATUS_MASK_ALL);

            if (_reader == null) throw new ApplicationException("create_datareader error");

            _statusCondition = _reader.get_statuscondition();

            try
            {
                var mask =
                    (int) DDS.StatusKind.DATA_AVAILABLE_STATUS |
                    (int) DDS.StatusKind.SUBSCRIPTION_MATCHED_STATUS |
                    (int) DDS.StatusKind.LIVELINESS_CHANGED_STATUS |
                    (int) DDS.StatusKind.SAMPLE_LOST_STATUS |
                    (int) DDS.StatusKind.SAMPLE_REJECTED_STATUS;

                _statusCondition.set_enabled_statuses((DDS.StatusMask) mask);
            }
            catch (DDS.Exception e)
            {
                throw new ApplicationException($"set_enabled_statuses error {e}");
            }

            _waitset = new DDS.WaitSet();

            try
            {
                _waitset.attach_condition(_statusCondition);
            }
            catch (DDS.Exception e)
            {
                throw new ApplicationException("attach_condition error {0}", e);
            }
        }

        private void initSubject()
        {
            lock (_mutex)
            {
                if (_groupSubject != null) return;
                _groupSubject = new Subject<IGroupedObservable<TKey, T>>();
                initializeDataReader(_participant);
                _scheduler.Schedule(_ => { ReceiveData(); });
            }
        }

        public IDisposable Subscribe(Action<IGroupedObservable<TKey, T>> action)
        {
            initSubject();
            return _groupSubject.Subscribe(action);
        }

        public void Subscribe()
        {
            initSubject();
        }

        private void ReceiveData()
        {
            DDS.ConditionSeq activeConditions = new DDS.ConditionSeq();
            while (true)
                try
                {
                    _waitset.wait(activeConditions, _timeout);
                    for (var c = 0; c < activeConditions.length; ++c)
                        if (activeConditions.get_at(c) == _statusCondition)
                        {
                            DDS.StatusMask triggeredmask =
                                _reader.get_status_changes();

                            if ((triggeredmask &
                                 (DDS.StatusMask)
                                 DDS.StatusKind.DATA_AVAILABLE_STATUS) != 0)
                            {
                                try
                                {
                                    DDS.TypedDataReader<T> dataReader
                                        = (DDS.TypedDataReader<T>) _reader;

                                    dataReader.take(
                                        _dataSeq,
                                        _infoSeq,
                                        DDS.ResourceLimitsQosPolicy.LENGTH_UNLIMITED,
                                        DDS.SampleStateKind.ANY_SAMPLE_STATE,
                                        DDS.ViewStateKind.ANY_VIEW_STATE,
                                        DDS.InstanceStateKind.ANY_INSTANCE_STATE);

                                    int dataLength = _dataSeq.length;
                                    //Console.WriteLine("Received {0}", dataLength);
                                    for (var i = 0; i < dataLength; ++i)
                                    {
                                        DDS.SampleInfo info = _infoSeq.get_at(i);
                                        if (info.valid_data)
                                        {
                                            var data = new T();
                                            data.copy_from(_dataSeq.get_at(i));
                                            var key = _keySelector(data);
                                            DDSKeyedSubject<TKey, T> keyedSubject;

                                            if (!_keyedSubjectDict.ContainsKey(key))
                                            {
                                                keyedSubject = new DDSKeyedSubject<TKey, T>(key, _scheduler);
                                                _keyedSubjectDict.Add(key, keyedSubject);
                                                _handleKeyDict.Add(info.instance_handle, key);
                                                _groupSubject.OnNext(keyedSubject);
                                            }
                                            else
                                            {
                                                keyedSubject = _keyedSubjectDict[key];
                                                if (_externalSubDict)
                                                    if (!_handleKeyDict.ContainsKey(info.instance_handle))
                                                    {
                                                        _handleKeyDict.Add(info.instance_handle, key);
                                                        _groupSubject.OnNext(keyedSubject);
                                                    }
                                            }

                                            keyedSubject.OnNext(data);
                                        }
                                        else if (info.instance_state ==
                                                 DDS.InstanceStateKind.NOT_ALIVE_DISPOSED_INSTANCE_STATE)
                                        {
                                            if (_handleKeyDict.ContainsKey(info.instance_handle))
                                            {
                                                var key = _handleKeyDict[info.instance_handle];
                                                if (_keyedSubjectDict.ContainsKey(key))
                                                {
                                                    var keyedSub = _keyedSubjectDict[key];
                                                    _keyedSubjectDict.Remove(key);
                                                    _handleKeyDict.Remove(info.instance_handle);
                                                    keyedSub.OnCompleted();
                                                    /* FIXME: If the instance comes alive again, it will break the Rx contract */
                                                }
                                                else
                                                {
                                                    Console.WriteLine(
                                                        "InstanceDataReaderListener invariant broken: keyedSubDict does not contain key");
                                                }
                                            }
                                            else
                                            {
                                                Console.WriteLine(
                                                    "InstanceDataReaderListener invariant broken: handleKeyDict does not contain info.instance_handle");
                                            }
                                        }
                                    }

                                    dataReader.return_loan(_dataSeq, _infoSeq);
                                }
                                catch (DDS.Retcode_NoData)
                                {
                                    _subject.OnCompleted();
                                    return;
                                }
                                catch (Exception ex)
                                {
                                    _subject.OnError(ex);
                                    Console.WriteLine($"ObservableTopicWaitSet: take error {ex}");
                                }
                            }
                            else
                            {
                                StatusKindPrinter.print((int) triggeredmask);
                                if ((triggeredmask &
                                     (DDS.StatusMask)
                                     DDS.StatusKind.SUBSCRIPTION_MATCHED_STATUS) != 0)
                                {
                                    DDS.SubscriptionMatchedStatus status = new DDS.SubscriptionMatchedStatus();
                                    _reader.get_subscription_matched_status(ref status);
                                    Console.WriteLine($"Subscription matched. current_count = {status.current_count}");
                                }

                                if ((triggeredmask &
                                     (DDS.StatusMask)
                                     DDS.StatusKind.LIVELINESS_CHANGED_STATUS) != 0)
                                {
                                    DDS.LivelinessChangedStatus status = new DDS.LivelinessChangedStatus();
                                    _reader.get_liveliness_changed_status(ref status);
                                    Console.WriteLine($"Liveliness changed. alive_count = {status.alive_count}");
                                }

                                if ((triggeredmask &
                                     (DDS.StatusMask)
                                     DDS.StatusKind.SAMPLE_LOST_STATUS) != 0)
                                {
                                    DDS.SampleLostStatus status = new DDS.SampleLostStatus();
                                    _reader.get_sample_lost_status(ref status);
                                    Console.WriteLine($"Sample lost. Reason = {status.last_reason.ToString()}");
                                }

                                if ((triggeredmask &
                                     (DDS.StatusMask)
                                     DDS.StatusKind.SAMPLE_REJECTED_STATUS) != 0)
                                {
                                    DDS.SampleRejectedStatus status = new DDS.SampleRejectedStatus();
                                    _reader.get_sample_rejected_status(ref status);
                                    Console.WriteLine($"Sample Rejected. Reason = {status.last_reason.ToString()}");
                                }
                            }
                        }
                }
                catch (DDS.Retcode_Timeout)
                {
                    Console.WriteLine("wait timed out");
                }
        }

        public IDisposable Subscribe(IObserver<T> observer)
        {
            lock (_mutex)
            {
                if (_subject != null) return _subject.Subscribe(observer);
                _subject = new Subject<T>();
                initializeDataReader(_participant);
                _scheduler.Schedule(_ => { ReceiveData(); });
            }

            return _subject.Subscribe(observer);
        }
    }
}