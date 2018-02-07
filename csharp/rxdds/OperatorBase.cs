using System;
using System.Reactive.Subjects;

namespace RTI.RxDDS
{
    public abstract class OperatorBase<T, U> : SubjectOperatorBase<T, U, Subject<U>>
    {
        public OperatorBase(IObservable<T> source)
            : base(source)
        {
        }
    };
}