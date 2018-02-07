/*********************************************************************************************
(c) 2005-2014 Copyright, Real-Time Innovations, Inc.  All rights reserved.                                  
RTI grants Licensee a license to use, modify, compile, and create derivative works 
of the Software.  Licensee has the right to distribute object form only for use with RTI 
products.  The Software is provided “as is”, with no warranty of any type, including 
any warranty for fitness for any purpose. RTI is under no obligation to maintain or 
support the Software.  RTI shall not be liable for any incidental or consequential 
damages arising out of the use or inability to use the software.
**********************************************************************************************/

using System;
using System.Linq;
using System.Reactive.Subjects;
using System.Reactive.Disposables;

namespace RTI.RxDDS
{
  public abstract class SubjectOperatorBase<T, U, SubjectType>
    : IObserver<T>, IObservable<U>
      where SubjectType : ISubject<U, U>, new()
  {
    public SubjectOperatorBase(IObservable<T> source)
    {
      this.source = source;
      this.subject = new SubjectType();
    }

    public abstract void OnNext(T value);
    public virtual void OnCompleted()
    {
      subject.OnCompleted();
    }
    public virtual void OnError(Exception error)
    {
      subject.OnError(error);
    }

    public virtual IDisposable Subscribe(IObserver<U> observer)
    {
      return 
        new CompositeDisposable()
        {  
          subject.Subscribe(observer),
          source.Subscribe(this)
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

    //private object mutex;
    //private bool alreadySubscribed = false;
    private IObservable<T> source;
    protected ISubject<U, U> subject;
  };
}