using System;
using System.Collections.Generic;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Reactive.Subjects;

namespace RTI.RxDDS
{
    internal class ObservableKeyedTopic<TKey, T> : IObservable<IGroupedObservable<TKey, T>>
        where T : class, DDS.ICopyable<T>, new()
    {
        private IEqualityComparer<TKey> _comparer;

        private readonly bool _externalSubDict;

        private ISubject<IGroupedObservable<TKey, T>,
            IGroupedObservable<TKey, T>> _groupSubject;

        private Dictionary<DDS.InstanceHandle_t, TKey> _handleKeyDict;
        private Dictionary<TKey, DDSKeyedSubject<TKey, T>> _keyedSubjectDict;
        private Func<T, TKey> _keySelector;
        private InstanceDataReaderListener _listener;
        private object _mutex;
        private DDS.DomainParticipant _participant;
        private IScheduler _scheduler;
        private string _topicName;
        private string _typeName;

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

            _externalSubDict = false;
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

            _externalSubDict = true;
        }

        public IDisposable Subscribe(IObserver<IGroupedObservable<TKey, T>> observer)
        {
            initSubject();
            return _groupSubject.Subscribe(observer);
        }

        private void init(DDS.DomainParticipant participant,
            string topicName,
            string typeName,
            Func<T, TKey> keySelector,
            IEqualityComparer<TKey> comparer,
            Dictionary<TKey, DDSKeyedSubject<TKey, T>> subDict,
            IScheduler scheduler)
        {
            _mutex = new object();

            _scheduler = scheduler ?? Scheduler.Immediate;

            _typeName = typeName ?? typeof(T).ToString();

            _participant = participant;
            _topicName = topicName;
            _keySelector = keySelector;
            _comparer = comparer;
            _keyedSubjectDict = subDict;
            _handleKeyDict = new Dictionary<DDS.InstanceHandle_t, TKey>(new InstanceHandleComparer());

            if (_scheduler == null ||
                _typeName == null ||
                _participant == null ||
                _topicName == null ||
                _keySelector == null ||
                _comparer == null)
                throw new ApplicationException("Invalid Params");
        }

        public void Dispose()
        {
            _listener.Dispose();
        }

        private void initializeDataReader(DDS.DomainParticipant participant)
        {
            DDS.Subscriber subscriber = participant.create_subscriber(
                DDS.DomainParticipant.SUBSCRIBER_QOS_DEFAULT,
                null,
                DDS.StatusMask.STATUS_MASK_NONE);
            if (subscriber == null) throw new ApplicationException("create_subscriber error");

            DDS.Topic topic = participant.create_topic(
                _topicName,
                _typeName,
                DDS.DomainParticipant.TOPIC_QOS_DEFAULT,
                null,
                DDS.StatusMask.STATUS_MASK_NONE);
            if (topic == null) throw new ApplicationException("create_topic error");

            _listener = new InstanceDataReaderListener(
                _groupSubject, _keyedSubjectDict, _keySelector, _comparer, _handleKeyDict, _scheduler, _externalSubDict);

            DDS.DataReaderQos rQos = new DDS.DataReaderQos();
            participant.get_default_datareader_qos(rQos);
            //Console.WriteLine("LIB CODE DR QOS: " + r_qos.history.kind);
            //Console.WriteLine("LIB CODE DR QOS: " + r_qos.reliability.kind);

            DDS.DataReader reader = subscriber.create_datareader(
                topic,
                rQos, //DDS.Subscriber.DATAREADER_QOS_DEFAULT,
                _listener,
                DDS.StatusMask.STATUS_MASK_ALL);
            if (reader != null) return;
            _listener = null;
            throw new ApplicationException("create_datareader error");
        }

        private void initSubject()
        {
            lock (_mutex)
            {
                if (_groupSubject != null) return;
                _groupSubject = new Subject<IGroupedObservable<TKey, T>>();
                initializeDataReader(_participant);
            }
        }

        public IDisposable Subscribe(Action<IGroupedObservable<TKey, T>> action)
        {
            initSubject();
            return _groupSubject.Subscribe(action);
        }

        private class InstanceDataReaderListener : DataReaderListenerAdapter
        {
            private IEqualityComparer<TKey> _comparer;
            private readonly DDS.UserRefSequence<T> _dataSeq;

            private readonly bool _externalSubDict;
            private readonly Dictionary<DDS.InstanceHandle_t, TKey> _handleKeyDict;
            private readonly DDS.SampleInfoSeq _infoSeq;
            private readonly Dictionary<TKey, DDSKeyedSubject<TKey, T>> _keyedSubDict;
            private readonly Func<T, TKey> _keySelector;
            private readonly IObserver<IGroupedObservable<TKey, T>> _observer;
            private readonly IScheduler _scheduler;

            public InstanceDataReaderListener(IObserver<IGroupedObservable<TKey, T>> observer,
                Dictionary<TKey, DDSKeyedSubject<TKey, T>> dict,
                Func<T, TKey> keySelector,
                IEqualityComparer<TKey> comparer,
                Dictionary<DDS.InstanceHandle_t, TKey> handleKeyDict,
                IScheduler sched,
                bool externalSubDict)
            {
                _externalSubDict = externalSubDict;
                _observer = observer;
                _scheduler = sched;
                _keyedSubDict = dict;
                _keySelector = keySelector;
                _comparer = comparer;
                _handleKeyDict = handleKeyDict;
                _dataSeq = new DDS.UserRefSequence<T>();
                _infoSeq = new DDS.SampleInfoSeq();
            }

            public override void on_data_available(DDS.DataReader reader)
            {
                try
                {
                    DDS.TypedDataReader<T> dataReader =
                        (DDS.TypedDataReader<T>) reader;

                    dataReader.take(
                        _dataSeq,
                        _infoSeq,
                        DDS.ResourceLimitsQosPolicy.LENGTH_UNLIMITED,
                        DDS.SampleStateKind.ANY_SAMPLE_STATE,
                        DDS.ViewStateKind.ANY_VIEW_STATE,
                        DDS.InstanceStateKind.ANY_INSTANCE_STATE);

                    int dataLength = _dataSeq.length;
                    for (var i = 0; i < dataLength; ++i)
                    {
                        DDS.SampleInfo info = _infoSeq.get_at(i);
                        if (info.valid_data)
                        {
                            var data = new T();
                            data.copy_from(_dataSeq.get_at(i));
                            var key = _keySelector(data);
                            DDSKeyedSubject<TKey, T> keyedSubject;

                            if (!_keyedSubDict.ContainsKey(key))
                            {
                                keyedSubject = new DDSKeyedSubject<TKey, T>(key, _scheduler);
                                _keyedSubDict.Add(key, keyedSubject);
                                _handleKeyDict.Add(info.instance_handle, key);
                                _observer.OnNext(keyedSubject);
                            }
                            else
                            {
                                keyedSubject = _keyedSubDict[key];
                                if (_externalSubDict)
                                    if (!_handleKeyDict.ContainsKey(info.instance_handle))
                                    {
                                        _handleKeyDict.Add(info.instance_handle, key);
                                        _observer.OnNext(keyedSubject);
                                    }
                            }

                            keyedSubject.OnNext(data);
                        }
                        else if (info.instance_state == DDS.InstanceStateKind.NOT_ALIVE_DISPOSED_INSTANCE_STATE)
                        {
                            if (_handleKeyDict.ContainsKey(info.instance_handle))
                            {
                                var key = _handleKeyDict[info.instance_handle];
                                if (_keyedSubDict.ContainsKey(key))
                                {
                                    var keyedSub = _keyedSubDict[key];
                                    _keyedSubDict.Remove(key);
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
                }
                catch (DDS.Exception ex)
                {
                    _observer.OnError(ex);
                    Console.WriteLine("ObservableKeyedTopic: InstanceDataReaderListener: take error {0}", ex);
                }
            }
        }
    }
}