using System;

namespace RTI.RxDDS
{
    class DataReaderListenerAdapter : DDS.DataReaderListener
    {
        public override void on_requested_deadline_missed(
            DDS.DataReader reader,
            ref DDS.RequestedDeadlineMissedStatus status) 
        {
            Console.WriteLine("Requested deadline missed {0} total_count.", status.total_count);
        }

        public override void on_requested_incompatible_qos(
            DDS.DataReader reader,
            DDS.RequestedIncompatibleQosStatus status) 
        {
            Console.WriteLine("Requested incompatible qos {0} total_count.", status.total_count);
        }

        public override void on_sample_rejected(
            DDS.DataReader reader,
            ref DDS.SampleRejectedStatus status)
        {
            Console.WriteLine("Sample Rejected. Reason={0}", status.last_reason.ToString());
        }

        public override void on_liveliness_changed(
            DDS.DataReader reader,
            ref DDS.LivelinessChangedStatus status) 
        {
            Console.WriteLine("Liveliness changed. {0} now alive.", status.alive_count);
        }

        public override void on_sample_lost(
            DDS.DataReader reader,
            ref DDS.SampleLostStatus status) 
        {
            Console.WriteLine("Sample lost. Reason={0}", status.last_reason.ToString());
        }

        public override void on_subscription_matched(
            DDS.DataReader reader,
            ref DDS.SubscriptionMatchedStatus status) 
        {
            Console.WriteLine("Subscription changed. {0} current count.", status.current_count);
        }

        public override void on_data_available(
            DDS.DataReader reader) { }
    };
}