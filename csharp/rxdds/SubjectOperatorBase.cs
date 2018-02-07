using System;
using System.Reactive.Disposables;
using System.Reactive.Subjects;

namespace RTI.RxDDS
{
    public abstract class SubjectOperatorBase<T, TU, TSubjectType>
        : IObserver<T>, IObservable<TU>
        where TSubjectType : ISubject<TU, TU>, new()
    {
        //private object mutex;
        //private bool alreadySubscribed = false;
        private readonly IObservable<T> _source;
        protected ISubject<TU, TU> Subject;

        public SubjectOperatorBase(IObservable<T> source)
        {
            _source = source;
            Subject = new TSubjectType();
        }

        public virtual IDisposable Subscribe(IObserver<TU> observer)
        {
            return
                new CompositeDisposable
                {
                    Subject.Subscribe(observer),
                    _source.Subscribe(this)
                };

            /*      lock (mutex)
            {
              if (!alreadySubscribed)
              {
                alreadySubscribed = true;
                return new CompositeDisposable()
                  {  
                    disp1,
                    source.Subscribe(this)
                  };
              }
              else
                return disp1;
            }*/
        }

        public abstract void OnNext(T value);

        public virtual void OnCompleted()
        {
            Subject.OnCompleted();
        }

        public virtual void OnError(Exception error)
        {
            Subject.OnError(error);
        }
    }
}