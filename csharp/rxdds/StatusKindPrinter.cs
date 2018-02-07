using System;

namespace RTI.RxDDS
{
    internal class StatusKindPrinter
    {
        public static void print(int kind)
        {
            if ((kind & 1) == 1)
                Console.WriteLine("INCONSISTENT_TOPIC_STATUS");
            if ((kind & 2) == 2)
                Console.WriteLine("OFFERED_DEADLINE_MISSED_STATUS");
            if ((kind & 4) == 4)
                Console.WriteLine("REQUESTED_DEADLINE_MISSED_STATUS");
            if ((kind & 32) == 32)
                Console.WriteLine("OFFERED_INCOMPATIBLE_QOS_STATUS");
            if ((kind & 64) == 64)
                Console.WriteLine("REQUESTED_INCOMPATIBLE_QOS_STATUS");
            if ((kind & 128) == 128)
                Console.WriteLine("SAMPLE_LOST_STATUS");
            if ((kind & 256) == 256)
                Console.WriteLine("SAMPLE_REJECTED_STATUS");
            if ((kind & 512) == 512)
                Console.WriteLine("DATA_ON_READERS_STATUS");
            if ((kind & 1024) == 1024)
                Console.WriteLine("DATA_AVAILABLE_STATUS");
            if ((kind & 2048) == 2048)
                Console.WriteLine("LIVELINESS_LOST_STATUS");
            if ((kind & 4096) == 4096)
                Console.WriteLine("LIVELINESS_CHANGED_STATUS");
            if ((kind & 8192) == 8192)
                Console.WriteLine("PUBLICATION_MATCHED_STATUS");
            if ((kind & 16384) == 16384)
                Console.WriteLine("SUBSCRIPTION_MATCHED_STATUS");
            if ((kind & 16777216) == 16777216)
                Console.WriteLine("RELIABLE_WRITER_CACHE_CHANGED_STATUS");
            if ((kind & 33554432) == 33554432)
                Console.WriteLine("RELIABLE_READER_ACTIVITY_CHANGED_STATUS");
            if ((kind & 67108864) == 67108864)
                Console.WriteLine("DATA_WRITER_CACHE_STATUS");
            if ((kind & 134217728) == 134217728)
                Console.WriteLine("DATA_WRITER_PROTOCOL_STATUS");
            if ((kind & 268435456) == 268435456)
                Console.WriteLine("DATA_READER_CACHE_STATUS");
            if ((kind & 536870912) == 536870912)
                Console.WriteLine("DATA_READER_PROTOCOL_STATUS");
            if ((kind & 1073741824) == 1073741824)
                Console.WriteLine("DATA_WRITER_DESTINATION_UNREACHABLE_STATUS");
            if ((kind & 2147483648) == 2147483648)
                Console.WriteLine("DATA_WRITER_SAMPLE_REMOVED_STATUS");
        }
    }
}