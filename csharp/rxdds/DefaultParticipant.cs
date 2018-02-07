using System;

namespace RTI.RxDDS
{
    public class DefaultParticipant
    {
        public static int DomainId
        {
            get { return domainId; }
            set { domainId = value; }
        }

        public static DDS.DomainParticipant Instance
        {
            get
            {
                if (participant == null)
                {
           
                    participant =
                        DDS.DomainParticipantFactory.get_instance().create_participant(
                            domainId,
                            DDS.DomainParticipantFactory.PARTICIPANT_QOS_DEFAULT,
                            null /* listener */,
                            DDS.StatusMask.STATUS_MASK_NONE);
                    if (participant == null)
                    {
                        throw new ApplicationException("create_participant error");
                    }
                }

                return participant;
            }
        }

        public static void Shutdown()
        {
            if (Instance != null)
            {
                Instance.delete_contained_entities();
                DDS.DomainParticipantFactory.get_instance().delete_participant(
                    ref participant);
            }
        }

        public static void RegisterType<Type, TypeSupportClass>()
        {
            typeof(TypeSupportClass)
                .GetMethod("register_type",
                    System.Reflection.BindingFlags.Public |
                    System.Reflection.BindingFlags.Static)        
                .Invoke(null, new Object[] { Instance, typeof(Type).ToString() });
        }
    
        public static DDS.TypedDataWriter<T> CreateDataWriter<T>(string topicName)
        {
            return CreateDataWriter<T>(topicName, typeof(T).ToString());
        }
        public static DDS.TypedDataWriter<T> CreateDataWriter<T>(string topicName, 
            string typeName)
        {
            DDS.DomainParticipant participant = Instance;

            DDS.Publisher publisher = participant.create_publisher(
                DDS.DomainParticipant.PUBLISHER_QOS_DEFAULT,
                null /* listener */,
                DDS.StatusMask.STATUS_MASK_NONE);

            if (publisher == null)
            {
                throw new ApplicationException("create_publisher error");
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
            /* DDS.DataWriterQos dw_qos = new DDS.DataWriterQos();
      participant.get_default_datawriter_qos(dw_qos);
      dw_qos.reliability.kind = DDS.ReliabilityQosPolicyKind.RELIABLE_RELIABILITY_QOS;
      dw_qos.history.kind = DDS.HistoryQosPolicyKind.KEEP_ALL_HISTORY_QOS;*/
            DDS.DataWriterQos dw_qos = new DDS.DataWriterQos();
            participant.get_default_datawriter_qos(dw_qos);
            //Console.WriteLine("LIB CODE DW QOS: " + dw_qos.history.kind);
            //Console.WriteLine("LIB CODE DW QOS: " + dw_qos.reliability.kind);

            DDS.DataWriter writer = publisher.create_datawriter(
                topic,
                DDS.Publisher.DATAWRITER_QOS_DEFAULT,
                null /* listener */,
                DDS.StatusMask.STATUS_MASK_NONE);
            if (writer == null)
            {
                throw new ApplicationException("create_datawriter error");
            }

            return (DDS.TypedDataWriter<T>) writer;
        }

        private static DDS.DomainParticipant participant;
        private static int domainId = 0;
    };
}