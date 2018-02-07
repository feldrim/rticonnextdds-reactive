using System;
using System.Reactive.Concurrency;
using System.Reactive.Subjects;

namespace RTI.RxDDS
{
    internal class ObservableTopicWaitSet<T> : IObservable<T> where T : class, DDS.ICopyable<T>, new()
    {
        private readonly DDS.UserRefSequence<T> _dataSeq = new DDS.UserRefSequence<T>();
        private readonly DDS.SampleInfoSeq _infoSeq = new DDS.SampleInfoSeq();

        private readonly object _mutex;
        private readonly DDS.DomainParticipant _participant;
        private DDS.DataReader _reader;
        private readonly IScheduler _scheduler;
        private DDS.StatusCondition _statusCondition;
        private ISubject<T, T> _subject;
        private readonly DDS.Duration_t _timeout;
        private readonly string _topicName;
        private readonly string _typeName;
        private DDS.WaitSet _waitset;

        public ObservableTopicWaitSet(DDS.DomainParticipant participant,
            string topicName,
            string typeName,
            DDS.Duration_t tmout)
        {
            _mutex = new object();

            _scheduler = new EventLoopScheduler();
            _timeout = tmout;

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
                _scheduler.Schedule(_ => { receiveData(); });
            }

            return _subject.Subscribe(observer);
        }

        public void Dispose()
        {
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
                throw new ApplicationException("set_enabled_statuses error {0}", e);
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

        private void receiveData()
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
                                        if (_infoSeq.get_at(i).valid_data)
                                        {
                                            var temp = new T();
                                            temp.copy_from(_dataSeq.get_at(i));
                                            _subject.OnNext(temp);
                                        }
                                        else if (_infoSeq.get_at(i).instance_state ==
                                                 DDS.InstanceStateKind.NOT_ALIVE_DISPOSED_INSTANCE_STATE)
                                        {
                                            /* FIXME: If the instance comes back online, 
                                         * it will break the Rx contract. */
                                            //Console.WriteLine("OnCompleted CALLED FROM LIB CODE on tid "+ 
                                            //System.Threading.Thread.CurrentThread.ManagedThreadId);
                                            _subject.OnCompleted();
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
    }
}