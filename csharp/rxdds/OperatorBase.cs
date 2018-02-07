using System;
using System.Reactive.Subjects;

namespace RTI.RxDDS
{
    public abstract class OperatorBase<T, TU> : SubjectOperatorBase<T, TU, Subject<TU>>
    {
        protected OperatorBase(IObservable<T> source)
            : base(source)
        {
        }
    }
}