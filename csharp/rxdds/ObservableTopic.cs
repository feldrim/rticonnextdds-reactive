using System;
using System.Diagnostics.CodeAnalysis;
using System.Reactive.Concurrency;
using System.Reactive.Subjects;

namespace RTI.RxDDS
{
    [SuppressMessage("ReSharper", "NotResolvedInText")]
    internal class ObservableTopic<T> : IObservable<T> where T : class, DDS.ICopyable<T>, new()
    {
        private DataReaderListener _listener;

        private readonly object _mutex;
        private readonly DDS.DomainParticipant _participant;
        private readonly IScheduler _scheduler;
        private ISubject<T, T> _subject;
        private readonly string _topicName;
        private readonly string _typeName;

        public ObservableTopic(DDS.DomainParticipant participant,
            string topicName,
            string typeName,
            IScheduler subscribeOnScheduler)
        {
            _mutex = new object();

            _scheduler = _scheduler == null ? Scheduler.Immediate : subscribeOnScheduler;

            _typeName = typeName ?? typeof(T).ToString();

            _participant = participant;
            _topicName = topicName;

            if (_scheduler == null ||
                _typeName == null ||
                _participant == null ||
                _topicName == null)
                throw new ArgumentNullException("ObservableTopic: Null parameters detected");
        }

        public IDisposable Subscribe(IObserver<T> observer)
        {
            lock (_mutex)
            {
                if (_subject != null) return _subject.Subscribe(observer);
                _subject = new Subject<T>();
                initializeDataReader(_participant);
            }

            return _subject.Subscribe(observer);
        }

        public void Dispose()
        {
            _listener.Dispose();
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

            _listener = new DataReaderListener(_subject, _scheduler);

            /* To customize the data reader QoS, use 
           the configuration file USER_QOS_PROFILES.xml */
            DDS.DataReader reader = subscriber.create_datareader(
                topic,
                DDS.Subscriber.DATAREADER_QOS_DEFAULT,
                _listener,
                DDS.StatusMask.STATUS_MASK_ALL);

            if (reader != null) return;
            _listener = null;
            throw new ApplicationException("create_datareader error");
        }

        private class DataReaderListener : DataReaderListenerAdapter
        {
            private readonly DDS.UserRefSequence<T> _dataSeq;
            private readonly DDS.SampleInfoSeq _infoSeq;
            private IScheduler _scheduler;
            private readonly ISubject<T, T> _subject;

            public DataReaderListener(ISubject<T, T> subject, IScheduler scheduler)
            {
                _subject = subject;
                _scheduler = scheduler;
                _dataSeq = new DDS.UserRefSequence<T>();
                _infoSeq = new DDS.SampleInfoSeq();
            }

            public override void on_data_available(DDS.DataReader reader)
            {
                try
                {
                    DDS.TypedDataReader<T> dataReader = (DDS.TypedDataReader<T>) reader;

                    dataReader.take(
                        _dataSeq,
                        _infoSeq,
                        DDS.ResourceLimitsQosPolicy.LENGTH_UNLIMITED,
                        DDS.SampleStateKind.ANY_SAMPLE_STATE,
                        DDS.ViewStateKind.ANY_VIEW_STATE,
                        DDS.InstanceStateKind.ANY_INSTANCE_STATE);

                    int dataLength = _dataSeq.length;

                    for (var i = 0; i < dataLength; ++i)
                        if (_infoSeq.get_at(i).valid_data)
                        {
                            var temp = new T();
                            temp.copy_from(_dataSeq.get_at(i));
                            _subject.OnNext(temp);
                        }
                        else if (_infoSeq.get_at(i).instance_state ==
                                 DDS.InstanceStateKind.NOT_ALIVE_DISPOSED_INSTANCE_STATE)
                        {
                            /* FIXME: If the instance comes back online, it will break the Rx contract. */
                            //Console.WriteLine("OnCompleted CALLED FROM LIB CODE on tid "+System.Threading.Thread.CurrentThread.ManagedThreadId);
                            _subject.OnCompleted();
                        }

                    dataReader.return_loan(_dataSeq, _infoSeq);
                }
                catch (DDS.Retcode_NoData)
                {
                    _subject.OnCompleted();
                }
                catch (Exception ex)
                {
                    _subject.OnError(ex);
                    Console.WriteLine($"ObservableTopic: take error {ex}");
                }
            }
        }
    }
}