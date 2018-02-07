using System;
using System.Reactive.Concurrency;
using System.Reactive.Subjects;

namespace RTI.RxDDS
{
    class ObservableTopic<T> : IObservable<T> where T : class , DDS.ICopyable<T>, new()
    {
        public ObservableTopic(DDS.DomainParticipant participant,
            string topicName,
            string typeName,
            IScheduler subscribeOnScheduler)
        {
            mutex = new Object();

            if (scheduler == null)
                this.scheduler = Scheduler.Immediate;
            else
                this.scheduler = subscribeOnScheduler;

            if (typeName == null)
                this.typeName = typeof(T).ToString();
            else
                this.typeName = typeName;

            this.participant = participant;
            this.topicName = topicName;

            if (this.scheduler == null ||
                this.typeName == null ||
                this.participant == null ||
                this.topicName == null)
                throw new ArgumentNullException("ObservableTopic: Null parameters detected");
        }

        public void Dispose()
        {
            listener.Dispose();
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

            listener = new DataReaderListener(subject, scheduler);

            /* To customize the data reader QoS, use 
           the configuration file USER_QOS_PROFILES.xml */
            DDS.DataReader reader = subscriber.create_datareader(
                topic,
                DDS.Subscriber.DATAREADER_QOS_DEFAULT,
                listener,
                DDS.StatusMask.STATUS_MASK_ALL);

            if (reader == null)
            {
                listener = null;
                throw new ApplicationException("create_datareader error");
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
                }
            }

            return subject.Subscribe(observer);
        }

        private class DataReaderListener : DataReaderListenerAdapter
        {
            public DataReaderListener(ISubject<T, T> subject, IScheduler scheduler)
            {
                this.subject = subject;
                this.scheduler = scheduler;
                dataSeq = new DDS.UserRefSequence<T>();
                infoSeq = new DDS.SampleInfoSeq();
            }

            public override void on_data_available(DDS.DataReader reader)
            {
                try
                {
                    DDS.TypedDataReader<T> dataReader = (DDS.TypedDataReader<T>)reader;

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
                        if (infoSeq.get_at(i).valid_data)
                        {
                            T temp = new T();
                            temp.copy_from(dataSeq.get_at(i));
                            subject.OnNext(temp);
                        }
                        else if (infoSeq.get_at(i).instance_state ==
                                 DDS.InstanceStateKind.NOT_ALIVE_DISPOSED_INSTANCE_STATE)
                        {

                            /* FIXME: If the instance comes back online, it will break the Rx contract. */
                            //Console.WriteLine("OnCompleted CALLED FROM LIB CODE on tid "+System.Threading.Thread.CurrentThread.ManagedThreadId);
                            subject.OnCompleted();
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
                    Console.WriteLine("ObservableTopic: take error {0}", ex);
                }
            }

            private DDS.UserRefSequence<T> dataSeq;
            private DDS.SampleInfoSeq infoSeq;
            private ISubject<T, T> subject;
            private IScheduler scheduler;
        }

        private Object mutex;
        private DDS.DomainParticipant participant;
        private string topicName;
        private string typeName;
        private DataReaderListener listener;
        private ISubject<T, T> subject;
        private IScheduler scheduler;
    };
}