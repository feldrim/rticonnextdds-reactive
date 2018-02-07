using System.Collections.Generic;

namespace RTI.RxDDS
{
    internal class InstanceHandleComparer : IEqualityComparer<DDS.InstanceHandle_t>
    {
        public bool Equals(DDS.InstanceHandle_t h1, DDS.InstanceHandle_t h2)
        {
            return h1.Equals(h2);
        }

        public int GetHashCode(DDS.InstanceHandle_t handle)
        {
            return handle.GetHashCode();
        }
    }
}