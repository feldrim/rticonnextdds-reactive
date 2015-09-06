/* ShapeType_subscriber.cxx

A subscription example

This file is derived from code automatically generated by the rtiddsgen 
command:

rtiddsgen -language C++11 -example <arch> ShapeType.idl

Example subscription of type ShapeType automatically generated by 
'rtiddsgen'. To test them, follow these steps:

(1) Compile this file and the example publication.

(2) Start the subscription on the same domain used for RTI Data Distribution
Service  with the command
objs/<arch>/ShapeType_subscriber <domain_id> <sample_count>

(3) Start the publication on the same domain used for RTI Data Distribution
Service with the command
objs/<arch>/ShapeType_publisher <domain_id> <sample_count>

(4) [Optional] Specify the list of discovery initial peers and 
multicast receive addresses via an environment variable or a file 
(in the current working directory) called NDDS_DISCOVERY_PEERS. 

You can run any number of publishers and subscribers programs, and can 
add and remove them dynamically from the domain.

Example:

To run the example application on domain <domain_id>:

On UNIX systems: 

objs/<arch>/ShapeType_publisher <domain_id> 
objs/<arch>/ShapeType_subscriber <domain_id> 

On Windows systems:

objs\<arch>\ShapeType_publisher <domain_id>  
objs\<arch>\ShapeType_subscriber <domain_id>   

*/
#include <algorithm>
#include <iostream>
#include <vector>

#include <dds/sub/ddssub.hpp>
#include <dds/core/ddscore.hpp>
// Or simply include <dds/dds.hpp> 

#include "ShapeType.hpp"
#include "rx4dds/rx4dds.h"
#include "solar_system.h"

namespace rx = rxcpp;
namespace rxu = rxcpp::util;

struct print_exception_on_error
{
  void operator()(std::exception_ptr eptr) const
  {
    try {
      std::rethrow_exception(eptr);
    }
    catch (std::exception & ex)
    {
      std::cout << ex.what() << "\n";
    }
  }
};

void dynamic_correlator(int domain_id, int sample_count)
{
  dds::domain::DomainParticipant participant(domain_id);
  dds::topic::Topic<ShapeType> triangle_topic(participant, "Triangle");
  dds::pub::DataWriter<ShapeType> triangle_writer(
    dds::pub::Publisher(participant), triangle_topic);
  dds::core::cond::WaitSet waitset;
  int count = 0;

  rxcpp::schedulers::scheduler scheduler =
    rxcpp::schedulers::make_current_thread();

  rxcpp::schedulers::worker worker
    = scheduler.create_worker();

  rx4dds::TopicSubscription<ShapeType> 
    topic_sub(participant, "Square", waitset, worker);

  auto subscription =
    topic_sub
      .create_data_observable()
      .op(rx4dds::group_by_key([](const ShapeType & shape) { 
            return shape.color(); 
        }))
      .map([](rxcpp::grouped_observable<dds::core::string, rti::sub::LoanedSample<ShapeType>> go) {
         return go.op(rx4dds::to_unkeyed())
                  .op(rx4dds::complete_on_dispose())
                  .op(rx4dds::error_on_no_alive_writers())
                  .op(rx4dds::skip_invalid_samples())
                  .op(rx4dds::map_sample_to_data())
                  .publish()
                  .ref_count()
                  .as_dynamic();
      })
      .op(rx4dds::coalesce_alive())
      .map([](const std::vector<rxcpp::observable<ShapeType>> & sources) {
        return rx4dds::combine_latest(sources.begin(), sources.end());
      })
      .switch_on_next()
      .map([](const std::vector<ShapeType> & shapes) {
        ShapeType avg("ORANGE", 0, 0, 30);
        
        for(auto & shape : shapes)
        {
          avg.x(avg.x() + shape.x());
          avg.y(avg.y() + shape.y());
        }
        avg.x(avg.x() / shapes.size());
        avg.y(avg.y() / shapes.size());

        return avg;
      })
      .op(rx4dds::publish_over_dds(triangle_writer, ShapeType("ORANGE", -1, -1, -1)))
      .subscribe();

  rxcpp::schedulers::schedulable schedulable =
    rxcpp::schedulers::make_schedulable(
    worker,
    [waitset](const rxcpp::schedulers::schedulable &) {
    const_cast<dds::core::cond::WaitSet &>(waitset).dispatch(dds::core::Duration(4));
  });

  while (count < sample_count || sample_count == 0) {
    worker.schedule(schedulable);
  }
  
}

void solar_system(int domain_id)
{
  SolarSystem system(domain_id);
  rxcpp::composite_subscription subscription = system.blue();
  //rxcpp::composite_subscription subscription = system.multiple();
  system.orbit(subscription);
}

void test_keyed_topic(int domain_id, int sample_count)
{
  dds::domain::DomainParticipant participant(domain_id);
  dds::core::cond::WaitSet waitset;
  int count = 0;

  rxcpp::schedulers::scheduler scheduler =
    rxcpp::schedulers::make_current_thread();

  rxcpp::schedulers::worker worker
    = scheduler.create_worker();

  rx4dds::TopicSubscription<ShapeType> topic_sub(participant, "Square", waitset, worker);

  auto observable = 
    topic_sub.create_data_observable()
            >> rx4dds::group_by_key(
                [](const ShapeType & shape) { return shape.color(); });

  auto subscription1 =
    observable
    .subscribe([&count](rxcpp::grouped_observable<dds::core::string, rti::sub::LoanedSample<ShapeType>> go) {
      (go >> rx4dds::to_unkeyed()
          >> rx4dds::error_on_no_alive_writers()
          >> rx4dds::skip_invalid_samples())
      .subscribe([&count](ShapeType shape) {
        std::cout << "color = " << shape.color()
                  << " x = " << shape.x() << "\n";
        count++;
      }, print_exception_on_error());
  }, print_exception_on_error());

  rxcpp::schedulers::schedulable schedulable =
    rxcpp::schedulers::make_schedulable(
    worker,
    [waitset](const rxcpp::schedulers::schedulable &) {
    const_cast<dds::core::cond::WaitSet &>(waitset).dispatch(dds::core::Duration(4));
  });

  while (count < sample_count || sample_count == 0) {
    worker.schedule(schedulable);
  }

  //std::cout << "unsubscribing\n";
  //subscription2.unsubscribe();
  //subscription1.unsubscribe();
}

void test_keyless_topic(int domain_id, int sample_count)
{
  dds::domain::DomainParticipant participant(domain_id);
  dds::core::cond::WaitSet waitset;
  int count = 0;

  rxcpp::schedulers::scheduler scheduler =
    rxcpp::schedulers::make_current_thread();

  rxcpp::schedulers::worker worker
    = scheduler.create_worker();

  rx4dds::TopicSubscription<ShapeType> topic_sub(participant, "Square", waitset, worker);

  auto observable = topic_sub.create_data_observable()
                      >> rx4dds::complete_on_dispose()
                      >> rx4dds::error_on_no_alive_writers();

  auto subscription1 =
    observable.subscribe([&count](rti::sub::LoanedSample<ShapeType> shape) {
      std::cout << "x= " << shape.data().x() << "\n";
      count++;
  }, 
  print_exception_on_error());

  auto subscription2 =
    (observable 
     >> rx4dds::map_sample_to_data())
    .subscribe([](const ShapeType & shape) {
       std::cout << "y= " << shape.y() << "\n";
    },
    print_exception_on_error());


  rxcpp::schedulers::schedulable schedulable =
    rxcpp::schedulers::make_schedulable(
    worker,
    [waitset](const rxcpp::schedulers::schedulable &) {
    const_cast<dds::core::cond::WaitSet &>(waitset).dispatch(dds::core::Duration(4));
  });

  while (count < sample_count || sample_count == 0) {
    worker.schedule(schedulable);
  }

  //std::cout << "unsubscribing\n";
  //subscription2.unsubscribe();
  //subscription1.unsubscribe();
}

void original_subscriber_main(int domain_id, int sample_count)
{
    // Create a DomainParticipant with default Qos
    dds::domain::DomainParticipant participant(domain_id);

    // Create a Topic -- and automatically register the type
    dds::topic::Topic<ShapeType> topic(participant, "Square");

    // Create a DataReader with default Qos (Subscriber created in-line)
    dds::sub::DataReader<ShapeType> reader(dds::sub::Subscriber(participant), topic);

    rx::subjects::subject<ShapeType> subject;
    auto subscriber = subject.get_subscriber();
    rxcpp::composite_subscription subscription  = 
      subject.get_observable().subscribe([](ShapeType & shape) {
      std::cout << "x= " << shape.x() << " "
                << "y= " << shape.y() << "\n";
    });

    // Create a ReadCondition for any data on this reader and associate a handler
    int count = 0;
    dds::sub::cond::ReadCondition read_condition(
        reader,
        dds::sub::status::DataState::any(),
        [&reader, &count, &subscriber, &subscription]()
        {
            // Take all samples
            dds::sub::LoanedSamples<ShapeType> samples = reader.take();
            for (auto sample : samples){
                if (sample.info().valid()){
                    count++;
                    //std::cout << sample.data() << std::endl; 
                    subscriber.on_next(sample.data());
                    if (count == 300)
                      subscription.unsubscribe();
                }   
            }

        } // The LoanedSamples destructor returns the loan
    );

    // Create a WaitSet and attach the ReadCondition
    dds::core::cond::WaitSet waitset;
    waitset += read_condition;

    while (count < sample_count || sample_count == 0) {
        // Dispatch will call the handlers associated to the WaitSet conditions
        // when they activate
        //std::cout << "ShapeType subscriber sleeping for 4 sec...\n";
        waitset.dispatch(dds::core::Duration(4)); // Wait up to 4s each time
    }
}

int main(int argc, char *argv[])
{
    int domain_id = 0;
    int sample_count = 0; // infinite loop

    if (argc >= 2) {
        domain_id = atoi(argv[1]);
    }
    if (argc >= 3) {
        sample_count = atoi(argv[2]);
    }

    // To turn on additional logging, include <rti/config/Logger.hpp> and
    // uncomment the following line:
    // rti::config::Logger::instance().verbosity(rti::config::Verbosity::STATUS_ALL);

    try {
        dynamic_correlator(domain_id, sample_count);
        //solar_system(domain_id);
        //test_keyed_topic(domain_id, sample_count);
        //test_keyless_topic(domain_id, sample_count);
        //original_subscriber_main(domain_id, sample_count);
    } catch (const std::exception& ex) {
        // This will catch DDS exceptions
        std::cerr << "Exception in subscriber_main(): " << ex.what() << std::endl;
        return -1;
    }

    // RTI Connext provides a finalize_participant_factory() method
    // if you want to release memory used by the participant factory singleton.
    // Uncomment the following line to release the singleton:
    //
    // dds::domain::DomainParticipant::finalize_participant_factory();
    std::cout << "Done. Press any key to continue...\n";
    getchar();
    return 0;
}
