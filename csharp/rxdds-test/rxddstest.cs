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
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;
using System.Reactive;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Reactive.Concurrency;
using System.Reactive.Disposables;
using RTI.RxDDS;

public class Processor
{

  private ShapeTypeExtended[] _testShapeVals = new ShapeTypeExtended[10] {
          new ShapeTypeExtended { x = 50,  y = 50,  color="BLUE", shapesize = 30  },
          new ShapeTypeExtended { x = 51,  y = 51,  color="BLUE", shapesize = 30  },
          new ShapeTypeExtended { x = 52,  y = 52,  color="BLUE", shapesize = 30  },
          new ShapeTypeExtended { x = 53,  y = 53,  color="BLUE", shapesize = 30  },
          new ShapeTypeExtended { x = 54,  y = 54,  color="BLUE", shapesize = 30  },
          new ShapeTypeExtended { x = 55,  y = 55,  color="BLUE", shapesize = 30  },
          new ShapeTypeExtended { x = 56,  y = 56,  color="BLUE", shapesize = 30  },
          new ShapeTypeExtended { x = 57,  y = 57,  color="BLUE", shapesize = 30  },
          new ShapeTypeExtended { x = 58,  y = 58,  color="BLUE", shapesize = 30  },
          new ShapeTypeExtended { x = 59,  y = 59,  color="BLUE", shapesize = 30  },
        };

  public static void Main(string[] args)
  {
    DefaultParticipant.DomainId = 0;

    if (args.Length >= 1)
    {
        if (!int.TryParse(args[0], out var domainId))
      {
        Console.WriteLine("Invalid domainId. Quitting...");
        return;
      }
      DefaultParticipant.DomainId = domainId;
    }
    
    DDS.DomainParticipant participant = DefaultParticipant.Instance;

    try
    {
      DefaultParticipant.RegisterType<ShapeTypeExtended, ShapeTypeExtendedTypeSupport>();

        var proc = new Processor
        {
            _triangleWriter = DefaultParticipant.CreateDataWriter<ShapeTypeExtended>("Triangle"),
            _circleWriter = DefaultParticipant.CreateDataWriter<ShapeTypeExtended>("Circle")
        };


        IDisposable disposable = null;

      //int workerThreads, completionPortThreads;
      //System.Threading.ThreadPool.GetMaxThreads(out workerThreads, out completionPortThreads);
      //Console.WriteLine("# of workerThreads = {0}, # of completionPortThreads = {1} ", 
      //                  workerThreads, completionPortThreads);

      IScheduler scheduler;
      if (args.Length >= 3)
      {
          switch (args[2])
          {
              case "Default":
                  scheduler = Scheduler.Default;
                  break;
              case "ThreadPool":
                  scheduler = Scheduler.Default;
                  break;
              case "Immediate":
                  scheduler = Scheduler.Immediate;
                  break;
              case "CurrentThread":
                  scheduler = Scheduler.CurrentThread;
                  break;
              case "TaskPool":
                  scheduler = TaskPoolScheduler.Default;
                  break;
              case "EventLoop":
                  scheduler = new EventLoopScheduler();
                  break;
              default:
                  throw new ApplicationException("Unknown Scheduler!");
          }
      }
      else
        scheduler = Scheduler.Immediate;

      if (args.Length >= 2)
      {
        switch (args[1])
        {
            case "demo1":
                disposable = proc.Demo1(participant, scheduler);
                break;
            case "demo2":
                disposable = proc.Demo2(participant, scheduler);
                break;
            case "demo3":
                disposable = proc.Demo3(participant);
                break;
            case "demo4":
                disposable = proc.Demo4(participant);
                break;
            case "demo5":
                disposable = proc.Demo5(participant, scheduler);
                break;
            case "forward":
                disposable = proc.Forward(participant);
                break;
            case "forward_short":
                disposable = proc.forward_short(participant);
                break;
            case "forward_shortest":
                disposable = proc.forward_shortest(participant);
                break;
            case "swap":
                disposable = proc.Swap(participant);
                break;
            case "swap_shortest":
                disposable = proc.Swap(participant);
                break;
            case "flower":
                disposable = proc.Flower(participant);
                break;
            case "instance_forward":
                disposable = proc.instance_forward(participant);
                break;
            case "aggregator":
                disposable = proc.Aggregator(participant);
                break;
            case "collisions_combinelatest":
                disposable = proc.collisions_combinelatest(participant, scheduler);
                break;
            case "collisions":
                disposable = proc.Collisions(participant, scheduler);
                break;
            case "single_circle_correlator":
                disposable = proc.single_circle_correlator(participant);
                break;
            case "selectmany_correlator":
                disposable = proc.selectmany_correlator(participant, false);
                break;
            case "selectmany_correlator_linq":
                disposable = proc.selectmany_correlator(participant, true);
                break;
            case "selectmany_groupby_correlator":
                disposable = proc.selectmany_groupby_correlator(participant);
                break;
            case "many_circle_correlator":
                disposable = proc.Many_circle_correlator(participant, scheduler);
                break;
            case "circle_zip_correlator":
                disposable = proc.circle_zip_correlator(participant);
                break;
            case "splitterDelayNAverageWindow":
                disposable = proc.SplitterDelayNAverageWindow(participant);
                break;
            case "splitterDelayNAverage":
                disposable = proc.SplitterDelayNAverage(participant);
                break;
            case "timeWindowAggregator":
                disposable = proc.TimeWindowAggregator(participant, scheduler);
                break;
            case "key_correlator_flat":
                disposable = proc.key_correlator_flat(participant);
                break;
            case "key_correlator_grouped":
                disposable = proc.Key_correlator_grouped(participant);
                break;
            case "key_correlator_replay":
                disposable = proc.key_correlator_replay(participant, false);
                break;
            case "key_correlator_replay_linq":
                disposable = proc.key_correlator_replay(participant, true);
                break;
            case "key_correlator_zip4":
                disposable = proc.key_correlator_zip4(participant);
                break;
            case "key_correlator_zipN":
                var n = 8;
                if (args.Length == 2)
                    n = int.Parse(args[1]);

                disposable = proc.Key_correlator_zipN(participant, n);
                break;
            case "key_correlator_dynamic":
                disposable = proc.Key_correlator_dynamic(participant, scheduler);
                break;
            case "once":
                disposable = proc.Once(participant);
                break;
            case "join":
                disposable = proc.Join(participant);
                break;
            case "orbit":
                disposable = proc.Orbit(participant);
                break;
            case "orbitSquare":
                disposable = proc.OrbitSquare(participant);
                break;
            case "orbitTwo":
                disposable = proc.OrbitTwo(participant);
                break;
            case "solarSystem":
                disposable = proc.SolarSystem(participant);
                break;
            case "shapewriter":
                disposable = proc.Shapewriter(participant);
                break;
            case "groupJoinInfiniteInner":
                disposable = proc.GroupJoinInfiniteInner();
                break;
        }
      }

      for (; disposable != null; )
      {
        var info = Console.ReadKey(true);
          if (info.Key != ConsoleKey.Enter) continue;
          disposable.Dispose();
          break;
      }
    }
    catch (DDS.Exception e)
    {
      Console.WriteLine("DDS Exception {0}", e);
    }
    catch (Exception ex)
    {
      Console.WriteLine("Generic Exception {0}", ex);
    }
    Console.WriteLine("Quitting...");
    System.Threading.Thread.Sleep(1000);
    DefaultParticipant.Shutdown();
  }

    /* forward */
    private IDisposable Demo1(DDS.DomainParticipant participant, IScheduler scheduler)
  {
    var rxReader = DDSObservable.FromTopic<ShapeTypeExtended>(participant, "Square");

    var disposable =
        rxReader.OnDataAvailable((ShapeTypeExtended shape) =>
        {
          DDS.InstanceHandle_t handle = DDS.InstanceHandle_t.HANDLE_NIL;
          _triangleWriter.write(shape, ref handle);
        });

    return disposable;
  }

    /* forward_short */
    private IDisposable Demo2(DDS.DomainParticipant participant, IScheduler scheduler)
  {
    // In this example Scheduler.Default appears to be indistinguishable from Scheduler.TaskPool.
    return
    DDSObservable
        .FromTopic<ShapeTypeExtended>(participant, "Square")
        .Subscribe((ShapeTypeExtended shape) =>
        {
          DDS.InstanceHandle_t handle = DDS.InstanceHandle_t.HANDLE_NIL;
          _triangleWriter.write(shape, ref handle);
        });
  }

    /* forward_shortest */
    private IDisposable Demo3(DDS.DomainParticipant participant)
  {
    DDS.Duration_t timeout;
    timeout.nanosec = 0;
    timeout.sec = 10;

    return DDSObservable
            .FromTopicWaitSet<ShapeTypeExtended>(participant, "Square", timeout)
            .Subscribe(_triangleWriter);
  }

    /* swap */
    private IDisposable Demo4(DDS.DomainParticipant participant)
  {
    var rxSquareReader =
        DDSObservable.FromTopic<ShapeTypeExtended>(participant, "Square");

    return
    rxSquareReader.Select((ShapeTypeExtended shape) =>
    {
        return new ShapeTypeExtended
        {
            x = shape.y,
            y = shape.x,
            color = shape.color,
            shapesize = shape.shapesize
        };
    })
    .DisposeAtEnd(_triangleWriter,
                  new ShapeTypeExtended { color = "BLUE" })
    .Subscribe();

  }

    /* correlator */
    private IDisposable Demo5(DDS.DomainParticipant participant, IScheduler scheduler)
  {
    var rxCircleReader =
        DDSObservable.FromTopic<ShapeTypeExtended>(participant, "Circle", Scheduler.Default);
    var rxSquareReader =
        DDSObservable.FromTopic<ShapeTypeExtended>(participant, "Square", Scheduler.Default);

    var correlator =
        from square in rxSquareReader
        from circle in rxCircleReader.Take(1)
        where square.color == circle.color
        select new ShapeTypeExtended
        {
          x = square.x,
          y = square.y,
          color = square.color,
          shapesize = circle.x
        };

    return correlator.Do(_ => Console.WriteLine("ThreadId = {0}", System.Threading.Thread.CurrentThread.ManagedThreadId))
                     .Subscribe(_triangleWriter);
  }

    private IDisposable TimeWindowAggregator(DDS.DomainParticipant participant, IScheduler scheduler)
  {
    var rxSquareReader =
        DDSObservable.FromKeyedTopic<string, ShapeTypeExtended>(
            participant, "Square", shape => shape.color);
    //test_shape_vals.ToObservable().GroupBy(shape => shape.color);

    /* Extremely stable memory consumption */
    return
        rxSquareReader
        .Subscribe((IGroupedObservable<string, ShapeTypeExtended> squareInstance) =>
        {
            squareInstance
                .ObserveOn(scheduler)
                .TimeWindowAggregate(
                    TimeSpan.FromSeconds(2),
                    new { avgX = 0.0, avgY = 0.0, shape = new ShapeTypeExtended() },
                    (avg, curVal, expiredList, curCount) =>
                    {
                        var totalX = avg.avgX * (curCount + expiredList.Count - 1);
                        var totalY = avg.avgY * (curCount + expiredList.Count - 1);
                        totalX += curVal.x;
                        totalY += curVal.y;
                        foreach (var ex in expiredList)
                        {
                            totalX -= ex.x;
                            totalY -= ex.y;
                        }
                        return new
                        {
                            avgX = totalX / curCount,
                            avgY = totalY / curCount,
                            shape = curVal
                        };
                    })
                .Select(point =>
                    new ShapeTypeExtended
                    {
                        x = (int)point.avgX,
                        y = (int)point.avgY,
                        color = squareInstance.Key,
                        shapesize = point.shape.shapesize
                    })
                .DisposeAtEnd(_triangleWriter,
                              new ShapeTypeExtended { color = squareInstance.Key })
                .Subscribe();
        });
  }

    private IDisposable Aggregator(DDS.DomainParticipant participant)
  {
    var rxSquareReader =
        DDSObservable.FromTopic<ShapeTypeExtended>(participant, "Square");
    var rxCircleReader =
        DDSObservable.FromTopic<ShapeTypeExtended>(participant, "Circle");

    return new CompositeDisposable(
         new IDisposable[] {
                 rxSquareReader.Subscribe(_triangleWriter),
                 rxCircleReader.Subscribe(_triangleWriter)
             }
    );
  }

    private IDisposable SplitterDelayNAverageWindow(DDS.DomainParticipant participant)
  {
    long windowSize = 50;
    long historySize = 20;

    //circle_writer = DefaultParticipant.CreateDataWriter<ShapeTypeExtended>("Circle");
    var rxSquareReader =
        DDSObservable.FromKeyedTopic<string, ShapeTypeExtended>(
            participant, "Square", shape => shape.color);
    // test_shape_vals.ToObservable().GroupBy(shape => shape.color);

    return new CompositeDisposable(
                new IDisposable[] {
                       rxSquareReader
                          .Subscribe(squareInstance => 
                                     squareInstance
                                       .Shift(historySize)
                                       .DisposeAtEnd(_circleWriter,
                                                     new ShapeTypeExtended { color = squareInstance.Key } )
                                       .Subscribe()),
                       rxSquareReader
                          .Subscribe(squareInstance =>
                              {
                                  squareInstance.Zip(
                                    squareInstance
                                    .Select(square => square.x)
                                    .WindowAggregate(windowSize,
                                                     0.0,
                                                     (avgX, tail, head, count) => (avgX * count + tail - head) / count,
                                                     (avgX, tail, count, prevCount) =>
                                                     {
                                                         if (prevCount > count) tail *= -1;
                                                         return (avgX * prevCount + tail) / count;
                                                     }),
                                    squareInstance
                                    .Select(square => square.y)
                                    .WindowAggregate(windowSize,
                                                     0.0,
                                                     (avgY, tail, head, count) => (avgY * count + tail - head) / count,
                                                     (avgY, tail, count, prevCount) =>
                                                     {
                                                         if (prevCount > count) tail *= -1;
                                                         return (avgY * prevCount + tail) / count;
                                                     }),
                                   (square, avgX, avgY) =>
                                       new ShapeTypeExtended
                                       {
                                           x = (int)avgX,
                                           y = (int)avgY,
                                           shapesize = square.shapesize,
                                           color = square.color
                                       })
                                .DisposeAtEnd(_triangleWriter,
                                              new ShapeTypeExtended { color = squareInstance.Key })
                                .Subscribe();
                              })
                    });
  }

    private IDisposable SplitterDelayNAverage(DDS.DomainParticipant participant)
  {
    //circle_writer = DefaultParticipant.CreateDataWriter<ShapeTypeExtended>("Circle");

    var rxSquareReader =
        DDSObservable.FromTopic<ShapeTypeExtended>(participant, "Square");
    var maxHistory = 20;
    int countX = 0, countY = 0;

    // var rx_square_reader = Observable.Range(10, 100);
    return new CompositeDisposable(
                new IDisposable[] {
                       rxSquareReader
                          .Shift(maxHistory)
                          .DisposeAtEnd(_circleWriter, new ShapeTypeExtended { color = "BLUE" })
                          .Subscribe(),

                       rxSquareReader.Zip(
                          rxSquareReader
                            .Select(square => square.x)
                            .RollingAggregate(0.0, 
                                              (avgX, x) => 
                                              {
                                                  avgX = ((avgX * countX) + x) / (countX+1);
                                                  countX++;
                                                  return avgX;
                                              }),
                          rxSquareReader
                            .Select(square => square.y)
                            .RollingAggregate(0.0, 
                                              (avgY, y) => 
                                              {
                                                  avgY = ((avgY * countY) + y) / (countY+1);
                                                  countY++;
                                                  return avgY;
                                              }),
                          (square, avgX, avgY) => 
                              new ShapeTypeExtended { 
                                  x = (int) avgX, 
                                  y = (int) avgY, 
                                  shapesize = square.shapesize, 
                                  color = square.color })
                          .Subscribe(_triangleWriter)
                    });
  }

    private IDisposable Many_circle_correlator(DDS.DomainParticipant participant, IScheduler scheduler)
  {
    var rxCircleReader =
        DDSObservable.FromTopic<ShapeTypeExtended>(participant, "Circle", Scheduler.Default);
    var rxSquareReader =
        DDSObservable.FromTopic<ShapeTypeExtended>(participant, "Square", Scheduler.Default);
    var map = new Dictionary<string, int>();

    rxCircleReader
        .GroupBy(circle => circle.color)
        .ObserveOn(scheduler)
        .Subscribe(groupedCircle =>
        {
          groupedCircle
            .Do(circle =>
            {
              lock (map)
              {
                map[groupedCircle.Key] = circle.x;
              }
            })
            .Subscribe();
        });

    return
        rxSquareReader
        .GroupBy(shape => shape.color)
        .ObserveOn(scheduler)
        .Do(groupedSquare =>
        {
          lock (map)
          {
            map[groupedSquare.Key] = 45;
          }
        })
        .Subscribe(groupedSquare =>
        {
          groupedSquare
              .Select(square =>
              {
                return new ShapeTypeExtended
                {
                  x = square.x,
                  y = square.y,
                  color = groupedSquare.Key,
                  shapesize = map[groupedSquare.Key]
                };
              })
              .Subscribe(_triangleWriter);
        });
  }

    private IDisposable circle_zip_correlator(DDS.DomainParticipant participant)
  {
    var rxCircleReader =
        DDSObservable.FromTopic<ShapeTypeExtended>(participant, "Circle", Scheduler.Default);
    var rxSquareReader =
        DDSObservable.FromKeyedTopic<string, ShapeTypeExtended>(participant, "Square", sq => sq.color, Scheduler.Default);
    var map = new Dictionary<string, int>();

    IScheduler scheduler = new EventLoopScheduler();

    Console.WriteLine("Main Thread id = {0}", System.Threading.Thread.CurrentThread.ManagedThreadId);
    return
        rxSquareReader
      //.GroupBy(shape => shape.color)
        .Do(_ => Console.WriteLine("Before ObserveOn tid = {0}", System.Threading.Thread.CurrentThread.ManagedThreadId))
        .ObserveOn(scheduler)
        .Do(_ => Console.WriteLine("After ObserveOn tid = {0}", System.Threading.Thread.CurrentThread.ManagedThreadId))
        .Subscribe(groupedSquare =>
        {
          Console.WriteLine("Inner Thread id = {0}", System.Threading.Thread.CurrentThread.ManagedThreadId);
          groupedSquare
                .Do(_ => Console.WriteLine("Before Zip tid = {0}", System.Threading.Thread.CurrentThread.ManagedThreadId))
                .Zip(rxCircleReader
                       .Do(_ => Console.WriteLine("Circle tid = {0}", System.Threading.Thread.CurrentThread.ManagedThreadId))
                       .Where(circle => circle.color == groupedSquare.Key),
                     (square, circle) =>
                     {
                       return new ShapeTypeExtended
                       {
                         x = square.x,
                         y = square.y,
                         color = groupedSquare.Key,
                         shapesize = circle.x
                       };
                     })
                .Do(_ => Console.WriteLine("After Zip tid = {0}", System.Threading.Thread.CurrentThread.ManagedThreadId))
                .Subscribe(shape => Console.WriteLine("{0} {1}", shape.x, shape.y));
        });
  }

    private IDisposable selectmany_correlator(DDS.DomainParticipant participant, bool useLinq)
  {
    var rxCircleReader =
        DDSObservable.FromTopic<ShapeTypeExtended>(participant, "Circle", Scheduler.Default);
    var rxSquareReader =
        DDSObservable.FromTopic<ShapeTypeExtended>(participant, "Square", Scheduler.Default);

    if (useLinq)
    {
      var correlator =
          from square in rxSquareReader
          from circle in rxCircleReader.Take(1)
          where square.color == circle.color
          select new ShapeTypeExtended
          {
            x = square.x,
            y = square.y,
            color = square.color,
            shapesize = circle.x
          };

      return correlator.Subscribe(_triangleWriter);
    }
    else
    {
      /* Consumes unbounded amount of memory. Don't know why. */
      return
          rxSquareReader
              .ObserveOn(Scheduler.Default)
              .SelectMany(square =>
              {
                return
                    rxCircleReader
                        .Where(circle => circle.color == square.color)
                        .Take(1)
                        .Select(circle =>
                            new ShapeTypeExtended
                            {
                              x = square.x,
                              y = square.y,
                              color = square.color,
                              shapesize = circle.x
                            });
              })
              .Subscribe(_triangleWriter);
    }
  }

    private IDisposable selectmany_groupby_correlator(DDS.DomainParticipant participant)
  {
    var rxCircleReader =
        DDSObservable.FromTopic<ShapeTypeExtended>(participant, "Circle", Scheduler.Default);
    var rxSquareReader =
        DDSObservable.FromTopic<ShapeTypeExtended>(participant, "Square", Scheduler.Default);

    /* Very stable memory consumption */
    return
        rxSquareReader
        .GroupBy(shape => shape.color)
        .Subscribe(groupedSquare =>
        {
          groupedSquare
              .ObserveOn(Scheduler.Default)
              .SelectMany(square =>
              {
                return rxCircleReader
                    .Where(circle => circle.color == groupedSquare.Key)
                    .Take(1)
                    .Select(circle =>
                        new ShapeTypeExtended
                        {
                          x = square.x,
                          y = square.y,
                          color = groupedSquare.Key,
                          shapesize = circle.x
                        });
              })
              .Subscribe(_triangleWriter);
        });
  }

    private IDisposable single_circle_correlator(DDS.DomainParticipant participant)
  {
    var rxCircleReader =
        DDSObservable.FromTopic<ShapeTypeExtended>(participant, "Circle");
    var rxSquareReader =
        DDSObservable.FromTopic<ShapeTypeExtended>(participant, "Square");

    var size = 45;
    rxCircleReader.Subscribe(circle => size = circle.x);

    return
        rxSquareReader
        .GroupBy(shape => shape.color)
        .Subscribe(groupedSquare =>
        {
          groupedSquare
              .Select(square =>
              {
                return new ShapeTypeExtended
                {
                  x = square.x,
                  y = square.y,
                  color = groupedSquare.Key,
                  shapesize = size
                };
              })
              .Subscribe(_triangleWriter);
        });
  }

    private IDisposable Forward(DDS.DomainParticipant participant)
  {
    var rxReader = DDSObservable.FromTopic<ShapeTypeExtended>(participant, "Square");

    var disposable =
        rxReader.OnDataAvailable((ShapeTypeExtended shape) =>
        {
          DDS.InstanceHandle_t handle = DDS.InstanceHandle_t.HANDLE_NIL;
          _triangleWriter.write(shape, ref handle);
        });

    return disposable;
  }

    private IDisposable forward_short(DDS.DomainParticipant participant)
  {
    return DDSObservable.FromTopic<ShapeTypeExtended>(participant, "Square")
                .OnDataAvailable((ShapeTypeExtended shape) =>
                {
                  DDS.InstanceHandle_t handle = DDS.InstanceHandle_t.HANDLE_NIL;
                  _triangleWriter.write(shape, ref handle);
                });
  }

    private IDisposable forward_shortest(DDS.DomainParticipant participant)
  {
    return DDSObservable
                .FromTopic<ShapeTypeExtended>(participant, "Square")
                .OnDataAvailable(_triangleWriter);
  }

    private IDisposable Swap(DDS.DomainParticipant participant)
  {
    return DDSObservable
                .FromTopic<ShapeTypeExtended>(participant, "Square")
                .Select(shape => new ShapeTypeExtended
                {
                  x = shape.y,
                  y = shape.x,
                  color = shape.color,
                  shapesize = shape.shapesize
                })
                .DisposeAtEnd(_triangleWriter,
                              new ShapeTypeExtended { color = "BLUE" })
                .Subscribe();
  }

    private IDisposable instance_forward(DDS.DomainParticipant participant)
  {
    DDS.Duration_t timeout = new DDS.Duration_t();
    timeout.nanosec = 0;
    timeout.sec = 10;
    return DDSObservable.FromKeyedTopicWaitSet<string, ShapeTypeExtended>
            (participant, "Square", shape => shape.color, timeout)
                  .Subscribe(ddsInstance =>
                  {
                    ShapeTypeExtended key = new ShapeTypeExtended { color = ddsInstance.Key };
                    DDS.InstanceHandle_t handle = DDS.InstanceHandle_t.HANDLE_NIL;
                    ddsInstance.Subscribe(_triangleWriter,
                                           () => _triangleWriter.dispose(key, ref handle));
                  });
  }

    private IDisposable Flower(DDS.DomainParticipant participant)
  {
      int a = 30, b = 30, c = 10;

      return Observable.Interval(TimeSpan.FromMilliseconds(1), Scheduler.Immediate)
                       .Select((long x) =>
                       {
                           var angle = (int)(x % 360);
                           return new ShapeTypeExtended
                           {
                               x = (int)(120 + (a + b) * Math.Cos(angle) + b * Math.Cos((a / b - c) * angle)),
                               y = (int)(120 + (a + b) * Math.Sin(angle) + b * Math.Sin((a / b - c) * angle)),
                               color = "GREEN",
                               shapesize = 5
                           };
                       })
                       .Subscribe(_triangleWriter);
  }

    private IDisposable Orbit(DDS.DomainParticipant participant)
  {
      int radius = 40, a = 100, b = 100;

      var rxSquareReader
          = DDSObservable.FromTopic<ShapeTypeExtended>(participant, "Square", Scheduler.Default);

      return Observable.Interval(TimeSpan.FromMilliseconds(3), Scheduler.Immediate)
                       .Select((long i) =>
                       {
                           var angle = (int)(i % 360);
                            return new ShapeTypeExtended
                            {
                                x = a + (int)(radius * Math.Cos(angle * Math.PI / 180)),
                                y = b + (int)(radius * Math.Sin(angle * Math.PI / 180)),
                                color = "RED",
                                shapesize = 10
                            };
                       })
                       .Subscribe(_circleWriter);
  }

    private IDisposable OrbitSquare(DDS.DomainParticipant participant)
  {
      var radius = 40;

      var rxSquareReader
          = DDSObservable.FromTopic<ShapeTypeExtended>(participant, "Square", Scheduler.Default);

      return Observable.Interval(TimeSpan.FromMilliseconds(3), Scheduler.Immediate)
                       .SelectMany((long i) =>
                       {
                           var angle = (int)(i % 360);
                           return
                               rxSquareReader
                               .Select(shape =>
                               {
                                   return new ShapeTypeExtended
                                   {
                                       x = shape.x + (int)(radius * Math.Cos(angle * Math.PI / 180)),
                                       y = shape.y + (int)(radius * Math.Sin(angle * Math.PI / 180)),
                                       color = "RED",
                                       shapesize = 10
                                   };
                               }).Take(1);
                       })
                       .Subscribe(_circleWriter);
  }

    private IDisposable OrbitTwo(DDS.DomainParticipant participant)
  {
      var circleRadius = 60;
      var triangleRadius = 30;

      var rxSquareReader
          = DDSObservable.FromTopic<ShapeTypeExtended>(participant, "Square", Scheduler.Default);

      var circleOrbit =
          Observable.Interval(TimeSpan.FromMilliseconds(8), new EventLoopScheduler())
                       .SelectMany((long i) =>
                       {
                           var degree = (int)(i % 360);
                           return
                               rxSquareReader
                               .Select(shape =>
                               {
                                   return new ShapeTypeExtended
                                   {
                                       x = shape.x + (int)(circleRadius * Math.Cos(degree * Math.PI / 180)),
                                       y = shape.y + (int)(circleRadius * Math.Sin(degree * Math.PI / 180)),
                                       color = "RED",
                                       shapesize = 15
                                   };
                               }).Take(1);
                       });

      var angle = 0;
      var triangleOrbit
          = circleOrbit
                .Select(shape =>
                {
                    angle += 3;
                    if (angle >= 360)
                        angle = 0;
                    return new ShapeTypeExtended
                    {
                        x = shape.x + (int)(triangleRadius * Math.Cos(angle * Math.PI / 180)),
                        y = shape.y + (int)(triangleRadius * Math.Sin(angle * Math.PI / 180)),
                        color = "YELLOW",
                        shapesize = 10
                    };
                });

      return new CompositeDisposable(new IDisposable[] { 
          triangleOrbit.Subscribe(_triangleWriter),
          circleOrbit.Subscribe(_circleWriter)
      });
  }

    private IDisposable Shapewriter(DDS.DomainParticipant participant)
  {
      var squareWriter =
          DefaultParticipant.CreateDataWriter<ShapeTypeExtended>("Square");

      for (var i = 0; i < _testShapeVals.Length; ++i)
      {
          Console.WriteLine("Press Enter to write Square.");
          var info = Console.ReadKey(true);
          if (info.Key == ConsoleKey.Enter)
          {
              DDS.InstanceHandle_t handle = DDS.InstanceHandle_t.HANDLE_NIL;
              var shape = _testShapeVals[i];
              squareWriter.write(shape, ref handle);
              Console.WriteLine("x = {0}, y = {0}", shape.x, shape.y);
          }
      }


      return Disposable.Create(() => {});
  }

    private IDisposable SolarSystem(DDS.DomainParticipant participant)
  {
      var sunLoc =
          DDSObservable.FromTopic<ShapeTypeExtended>(participant, "Square", Scheduler.Default);

      var ticks =
        Observable.Interval(TimeSpan.FromMilliseconds(25), new EventLoopScheduler());

      Func<string, int, int, int, IObservable<ShapeTypeExtended>> planetOrbit
          = (color, size, orbitRadius, daysInYear) =>
          {
              var useLinq = false;
              if (useLinq)
              {
                  return from t in ticks
                         from sun in sunLoc.Take(1)
                         let degree = t * 365 / daysInYear
                         select new ShapeTypeExtended
                         {
                             x = sun.x + (int)(orbitRadius * Math.Cos(degree * Math.PI / 180)),
                             y = sun.y + (int)(orbitRadius * Math.Sin(degree * Math.PI / 180)),
                             color = color,
                             shapesize = size
                         };
              }
              else
              {
                  double degree = 0;
                  return ticks
                           .SelectMany((long i) =>
                           {
                               degree = (double)i * 365 / daysInYear;
                               return
                                   sunLoc
                                   .Take(1)
                                   .Select(shape =>
                                   {
                                       Console.WriteLine("i = {0}, degree = {1}, x = {2}, y = {3}", i, degree, shape.x, shape.y);
                                       return new ShapeTypeExtended
                                       {
                                           x = shape.x + (int)(orbitRadius * Math.Cos(degree * Math.PI / 180)),
                                           y = shape.y + (int)(orbitRadius * Math.Sin(degree * Math.PI / 180)),
                                           color = color,
                                           shapesize = size
                                       };
                                   });
                           });
              }
          };

      int mercuryRadius = 30,  mercurySize  = 8,  mercuryYear = 88;
      int venusRadius   = 50,  venusSize    = 15, venusYear   = 225;
      int earthRadius   = 70,  earthSize    = 17, earthYear   = 365;
      int marsRadius    = 90,  marsSize     = 12, marsYear    = 686;
      int jupiterRadius = 120, jupiterSize  = 30, jupiterYear = 4329;
      int moonRadius    = 20,  moonSize     = 8;

      var mercuryLoc = planetOrbit("RED",    mercurySize, mercuryRadius, mercuryYear);
      var venusLoc   = planetOrbit("YELLOW", venusSize,   venusRadius,   venusYear);
      var earthLoc   = planetOrbit("BLUE",   earthSize,   earthRadius,   earthYear);
      var marsLoc    = planetOrbit("ORANGE", marsSize,    marsRadius,    marsYear);
      var jupiterLoc = planetOrbit("CYAN",   jupiterSize, jupiterRadius, jupiterYear);

      var angle = 0;
      var moonLoc
          = earthLoc
                .Select(shape =>
                {
                    angle += 3;
                    return new ShapeTypeExtended
                    {
                        x = shape.x + (int)(moonRadius * Math.Cos(angle * Math.PI / 180)),
                        y = shape.y + (int)(moonRadius * Math.Sin(angle * Math.PI / 180)),
                        color = "GREEN",
                        shapesize = moonSize
                    };
                });
      
      return new CompositeDisposable(new IDisposable[] { 
          //mercuryLoc.Subscribe(circle_writer),
          //venusLoc.Subscribe(circle_writer),
          earthLoc.Subscribe(_circleWriter)
          //marsLoc.Subscribe(circle_writer),
          //jupiterLoc.Subscribe(circle_writer),
          // moonLoc.Subscribe(triangle_writer)
      });
  }

    private double Distance(int x1, int y1, int x2, int y2)
  {
    return Math.Sqrt(Math.Pow(x1 - x2, 2) + Math.Pow(y1 - y2, 2));
  }

    private IDisposable Collisions(DDS.DomainParticipant participant, IScheduler scheduler)
  {
    var rxCircleReader =
        DDSObservable.FromTopic<ShapeTypeExtended>(participant, "Circle", Scheduler.Default);
    var rxSquareReader =
        DDSObservable.FromTopic<ShapeTypeExtended>(participant, "Square", Scheduler.Default);
    /*
            var collisions =  from circle in rx_circle_reader
                              from square in rx_square_reader.Take(30) 
                              where square.color == circle.color
                                    &&
                                    distance(circle.x, circle.y, square.x, square.y) <= 30
                              select new ShapeTypeExtended 
                              { 
                                  x = (circle.x + square.x) / 2, 
                                  y = (circle.y + square.y) / 2,
                                  color = "GREEN",
                                  shapesize = 10
                              }; 

            return collisions.Subscribe(triangle_writer);
    */
    /*
            return
                rx_circle_reader
                  .Where(circle => circle.color == "BLUE")
                  .SelectMany(circle => rx_square_reader.Where(square => square.color == "RED").Take(30),
                              (circle, square) => new { x1 = circle.x, y1 = circle.y, x2 = square.x, y2 = square.y })
                  .Where(point => distance(point.x1, point.y1, point.x2, point.y2) <= 30)
                  .Select(p =>
                  {
                      return new ShapeTypeExtended
                      {
                          x = (p.x1 + p.x2) / 2,
                          y = (p.y1 + p.y2) / 2,
                          color = "GREEN",
                          shapesize = 10
                      };
                  })
                  .Subscribe(triangle_writer);
    */
    /* Very stable memory usage */
    return
        rxCircleReader
          .ObserveOn(scheduler)
          .SelectMany(circle =>
          {
            return
                rxSquareReader
                 .Take(1)
                 .Where(square => square.color == circle.color)
                 .Where(square => Distance(square.x, square.y, circle.x, circle.y) <= 30)
                 .Select(square =>
                 {
                   return new ShapeTypeExtended
                   {
                     x = (square.x + circle.x) / 2,
                     y = (square.y + circle.y) / 2,
                     color = "RED",
                     shapesize = 10
                   };
                 });
          })
          .Subscribe(_triangleWriter);

  }

    private IDisposable collisions_combinelatest(DDS.DomainParticipant participant, IScheduler scheduler)
  {
    var rxCircleReader =
        DDSObservable.FromTopic<ShapeTypeExtended>(participant, "Circle", Scheduler.Default);
    var rxSquareReader =
        DDSObservable.FromTopic<ShapeTypeExtended>(participant, "Square", Scheduler.Default);

    var dummy = new ShapeTypeExtended { color = "DUMMY" };

    return rxCircleReader
              .ObserveOn(scheduler)
              .CombineLatest(rxSquareReader,
                             (circle, square) =>
                             {
                               if (Distance(circle.x, circle.y, square.x, square.y) <= 30)
                                 return new ShapeTypeExtended
                                 {
                                   x = (square.x + circle.x) / 2,
                                   y = (square.y + circle.y) / 2,
                                   color = "RED",
                                   shapesize = 10
                                 };
                               else
                                 return dummy;
                             })
                 .Where(shape => shape.color != "DUMMY")
                 .Subscribe(_triangleWriter);
  }

    private IDisposable key_correlator_flat(DDS.DomainParticipant participant)
  {
    var rxSquareReader =
        DDSObservable.FromTopic<ShapeTypeExtended>(participant, "Square");

    var disp1 =
       rxSquareReader
            .Where(shape => shape.color == "GREEN")
            .CombineLatest(rxSquareReader.Where(shape => shape.color == "BLUE"),
                          (a, b) =>
                          {
                            return new ShapeTypeExtended
                            {
                              x = (a.x + b.x) / 2,
                              y = (a.y + b.y) / 2,
                              color = "RED",
                              shapesize = a.shapesize
                            };
                          })
              .Subscribe(_triangleWriter);

    var disp2 =
      rxSquareReader
          .Where(shape => shape.color == "YELLOW")
          .CombineLatest(rxSquareReader.Where(shape => shape.color == "MAGENTA"),
                        (a, b) =>
                        {
                          return new ShapeTypeExtended
                          {
                            x = (a.x + b.x) / 2,
                            y = (a.y + b.y) / 2,
                            color = "CYAN",
                            shapesize = a.shapesize
                          };
                        })
            .Subscribe(_triangleWriter);


    return new CompositeDisposable(new IDisposable[] { disp1, disp2 });
  }

    private IDisposable Key_correlator_grouped(DDS.DomainParticipant participant)
  {
    var rxSquareReader =
        DDSObservable.FromKeyedTopic<string, ShapeTypeExtended>(participant, "Square", shape => shape.color);

    var colorObservableMap =
      new Dictionary<string, IGroupedObservable<string, ShapeTypeExtended>>();

      var associations = new Dictionary<string, string>
      {
          {"PURPLE", "BLUE"},
          {"BLUE", "PURPLE"},
          {"RED", "GREEN"},
          {"GREEN", "RED"},
          {"YELLOW", "CYAN"},
          {"CYAN", "YELLOW"},
          {"MAGENTA", "ORANGE"},
          {"ORANGE", "MAGENTA"}
      };

      /* Increasing memory consumption */
    return
      rxSquareReader
        .Do(groupedSq => colorObservableMap.Add(groupedSq.Key, groupedSq))
        .Subscribe(groupedSq =>
        {
          var pairKey = associations[groupedSq.Key];
          if (colorObservableMap.ContainsKey(pairKey))
          {
            groupedSq
              .CombineLatest(colorObservableMap[pairKey],
                            (a, b) =>
                            {
                              return new ShapeTypeExtended
                              {
                                x = (a.x + b.x) / 2,
                                y = (a.y + b.y) / 2,
                                color = a.color,
                                shapesize = a.shapesize
                              };
                            })
              .Subscribe(_triangleWriter);
          }
        });
  }

    private IDisposable key_correlator_replay(DDS.DomainParticipant participant, bool useLinq)
  {
    var maxColors = 8;

    var rxSquareReader =
        DDSObservable.FromKeyedTopic<string, ShapeTypeExtended>(participant, "Square", shape => shape.color);

      var dictionary = new Dictionary<string, string>
      {
          {"PURPLE", "BLUE"},
          {"RED", "GREEN"},
          {"YELLOW", "CYAN"},
          {"MAGENTA", "ORANGE"}
      };

      var associations = new ReadOnlyDictionary<string, string>(dictionary);

    var cache = rxSquareReader.Replay();
    cache.Connect();

    if (useLinq)
    {
      return
          (from group1 in cache.Where(groupedSq => associations.ContainsKey(groupedSq.Key))
           from group2 in cache.Take(maxColors)
           where associations[group1.Key] == group2.Key
           select new { k1 = group1, k2 = group2 })
            .Subscribe(pair =>
            {
              Console.WriteLine("MATCH {0}--{1}", pair.k1.Key, pair.k2.Key);
              pair.k1.CombineLatest(pair.k2,
                                   (a, b) =>
                                   {
                                     return new ShapeTypeExtended
                                     {
                                       x = (a.x + b.x) / 2,
                                       y = (a.y + b.y) / 2,
                                       color = a.color,
                                       shapesize = a.shapesize
                                     };
                                   })
                     .Subscribe(_triangleWriter);
            });
    }
    else
    {
      return
        cache
           .Where(groupedSq => associations.ContainsKey(groupedSq.Key))
           .SelectMany(groupedSq => cache.Take(maxColors),
                      (groupedSq, cachedGroupedSq) =>
                      {
                        if (associations[groupedSq.Key] == cachedGroupedSq.Key)
                        {
                          Console.WriteLine("MATCH {0} -- {1}", groupedSq.Key, cachedGroupedSq.Key);
                          groupedSq
                            .CombineLatest(cachedGroupedSq,
                                          (a, b) =>
                                          {
                                            return new ShapeTypeExtended
                                            {
                                              x = (a.x + b.x) / 2,
                                              y = (a.y + b.y) / 2,
                                              color = a.color,
                                              shapesize = a.shapesize
                                            };
                                          })
                            .Subscribe(_triangleWriter);
                        }
                        else
                          Console.WriteLine("NO-MATCH {0} -- {1}", groupedSq.Key, cachedGroupedSq.Key);

                        return (IGroupedObservable<string, ShapeTypeExtended>)groupedSq;
                      }).Subscribe();
    }
  }

    private IDisposable key_correlator_zip4(DDS.DomainParticipant participant)
  {
    var rxSquareReader =
        DDSObservable.FromKeyedTopic<string, ShapeTypeExtended>(participant, "Square", shape => shape.color);

      var colorMap = new Dictionary<string, IObservable<IGroupedObservable<string, ShapeTypeExtended>>>
      {
          {"BLUE", rxSquareReader.Where(sq => sq.Key == "BLUE")},
          {"RED", rxSquareReader.Where(sq => sq.Key == "RED")},
          {"GREEN", rxSquareReader.Where(sq => sq.Key == "GREEN")},
          {"YELLOW", rxSquareReader.Where(sq => sq.Key == "YELLOW")}
      };


      return
      colorMap["RED"]
              .Zip(colorMap["GREEN"],
                   colorMap["BLUE"],
                   colorMap["YELLOW"],
                  (rs, gs, bs, ys) =>
                  {
                    rs.CombineLatest(gs, bs, ys,
                                      (r, g, b, y) =>
                                      {
                                        return new ShapeTypeExtended
                                        {
                                          x = (r.x + g.x + b.x + y.x) / 4,
                                          y = (r.y + g.y + b.y + y.y) / 4,
                                          color = "MAGENTA",
                                          shapesize = 30
                                        };
                                      })
                        .Subscribe(_triangleWriter);

                    return rs;
                  }).Subscribe();
  }

    private IDisposable Key_correlator_zipN(DDS.DomainParticipant participant, int n)
  {
    var rxSquareReader =
        DDSObservable.FromKeyedTopic<string, ShapeTypeExtended>(participant, "Square", shape => shape.color);

    // The order of the keys is the same that of the Shapes Demo UI.
    var allkeys = new[] { "PURPLE", "BLUE", "RED", "GREEN", "YELLOW", "CYAN", "MAGENTA", "ORANGE" };
    var nkeys = allkeys.Take(n);

    Console.Write("Averaging {0}: ", n);
    nkeys.Subscribe(Observer.Create<string>(key => Console.Write("{0} ", key)));
    Console.WriteLine();

    var colorObservables =
      nkeys.Select(key => rxSquareReader.Where(sq => sq.Key == key));

    return
    Observable.Zip(colorObservables)
              .Subscribe((IList<IGroupedObservable<string, ShapeTypeExtended>> keyList) =>
              {
                keyList.CombineLatest()
                          .Select((IList<ShapeTypeExtended> shapes) =>
                          {
                            var avg = new ShapeTypeExtended
                            {
                              x = 0,
                              y = 0,
                              color = "RED",
                              shapesize = 30
                            };

                            foreach (var shape in shapes)
                            {
                              avg.x += shape.x;
                              avg.y += shape.y;
                            }
                            avg.x /= shapes.Count;
                            avg.y /= shapes.Count;

                            return avg;
                          })
                          .Subscribe(_triangleWriter);
              });
  }

    private IDisposable Key_correlator_dynamic(DDS.DomainParticipant participant, IScheduler scheduler)
  {
    var rxSquareReader =
        DDSObservable.FromKeyedTopic<string, ShapeTypeExtended>(participant, "Square", shape => shape.color, scheduler);
    var triangleColor = "RED";
    
    return
      rxSquareReader
        .ActiveKeyScan(new { lastSub = Disposable.Empty },
            (seed, streamList) =>
            {
              seed.lastSub.Dispose();
              if (streamList.Count == 0)
              {
                var handle = DDS.InstanceHandle_t.HANDLE_NIL;
                _triangleWriter.dispose(new ShapeTypeExtended { color = triangleColor }, ref handle);
                return new { lastSub = Disposable.Empty };
              }
              else
                return new
                {
                  lastSub =
                    streamList.CombineLatest()
                              .Select((IList<ShapeTypeExtended> shapes) =>
                              {
                                var avg = new ShapeTypeExtended
                                {
                                  x = 0,
                                  y = 0,
                                  color = triangleColor,
                                  shapesize = 30
                                };

                                foreach (var shape in shapes)
                                {
                                  avg.x += shape.x;
                                  avg.y += shape.y;
                                }
                                avg.x /= shapes.Count;
                                avg.y /= shapes.Count;

                                return avg;
                              })
                              .Subscribe(_triangleWriter)
                };
            })
        .Subscribe();
  }

    private IDisposable Once(DDS.DomainParticipant participant)
  {
      return Observable
              .Timer(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1))
              .Once(10)
              .Once(() => 20)
              .Subscribe(Console.WriteLine);
  }

    private IDisposable Join(DDS.DomainParticipant participant)
  {
      var squares =
          DDSObservable.FromTopic<ShapeTypeExtended>(participant, "Square")
                       .Publish()
                       .RefCount();
      var circles =
          DDSObservable.FromTopic<ShapeTypeExtended>(participant, "Circle")
                       .Publish()
                       .RefCount();

      /* Check if the shapes jump more than 1 pixel at a time.*/
      /* Yes, they do if fast. */
      
      ShapeTypeExtended dummy = null;
      squares.Scan(new 
                   {
                         lastx = -1, 
                         lasty = -1, 
                         shape = dummy,
                         largeDiff = false
                   },
                   (seed, shape) =>
                   {
                       return new {
                         lastx = shape.x,
                         lasty = shape.y,
                         shape = shape,
                         largeDiff = (Math.Abs(seed.lastx - shape.x) > 1) ||
                                     (Math.Abs(seed.lasty - shape.y) > 1)
                       };
                   })
             .Where(obj => obj.largeDiff)
             .Subscribe(obj =>
                   {
                        Console.WriteLine("{0} {1}", obj.shape.x, obj.shape.y);
                   });
      
      return squares.ObserveOn(Scheduler.Default)
                    .Join(circles, 
                          _ => squares, 
                          _ => circles,
                          (square, circle) => new { 
                              sq = square, 
                              cr = circle
                          })
                    .Where(obj => (obj.sq.x == obj.cr.x) || 
                                  (obj.sq.y == obj.cr.y))
                    .Select(obj => new ShapeTypeExtended
                          {
                              x = obj.sq.x,
                              y = obj.sq.y,
                              color = "GREEN",
                              shapesize = 15
                          })
                    .Subscribe(_triangleWriter);
  }

  public static IEnumerable<int> Numbers()
  {
    var i = 0;
    while (true)
    {
      yield return unchecked(i++);
    }
  }

    private IDisposable GroupJoinInfiniteInner()
  {
    var xs = new[] { 0, 1, 2, 3 };
    var ys = Enumerable.Repeat(0, 1000000000).Scan((n, _) => n + 1, 0);
    //IEnumerable<int> ys = Numbers();
    var result = xs.GroupJoin(ys,
                                x => x % 2 == 0,
                                y => y % 2 == 0,
                                (x, g) => g.Take(5))
                     .Take(1).SelectMany(x => x);

    //foreach(var r in result)
    //  Console.WriteLine("{0}", r);

    return Disposable.Empty;
  }

  //private DDS.TypedDataWriter<ShapeTypeExtended> square_writer;
  private DDS.TypedDataWriter<ShapeTypeExtended> _triangleWriter;
  private DDS.TypedDataWriter<ShapeTypeExtended> _circleWriter;
  private DDS.InstanceHandle_t _instanceHandle = DDS.InstanceHandle_t.HANDLE_NIL;
}


