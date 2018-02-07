using System;

namespace RTI.RxDDS
{
    internal class DataReaderListenerAdapter : DDS.DataReaderListener
    {
        public override void on_requested_deadline_missed(
            DDS.DataReader reader,
            ref DDS.RequestedDeadlineMissedStatus status)
        {
            Console.WriteLine($"Requested deadline missed {status.total_count} total_count.");
        }

        public override void on_requested_incompatible_qos(
            DDS.DataReader reader,
            DDS.RequestedIncompatibleQosStatus status)
        {
            Console.WriteLine($"Requested incompatible qos {status.total_count} total_count.");
        }

        public override void on_sample_rejected(
            DDS.DataReader reader,
            ref DDS.SampleRejectedStatus status)
        {
            Console.WriteLine($"Sample Rejected. Reason={status.last_reason.ToString()}");
        }

        public override void on_liveliness_changed(
            DDS.DataReader reader,
            ref DDS.LivelinessChangedStatus status)
        {
            Console.WriteLine($"Liveliness changed. {status.alive_count} now alive.");
        }

        public override void on_sample_lost(
            DDS.DataReader reader,
            ref DDS.SampleLostStatus status)
        {
            Console.WriteLine($"Sample lost. Reason={status.last_reason.ToString()}");
        }

        public override void on_subscription_matched(
            DDS.DataReader reader,
            ref DDS.SubscriptionMatchedStatus status)
        {
            Console.WriteLine($"Subscription changed. {status.current_count} current count.");
        }

        public override void on_data_available(
            DDS.DataReader reader)
        {
        }
    }
}