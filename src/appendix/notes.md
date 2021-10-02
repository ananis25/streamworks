* The java code implements the serializable interface for the Source class, how do we replicate that in python? 
  * Hmm, that seems to be used for cloning a component to create different instances of the corresponding executor, for ex, multiple SourceInstanceExecutor from a single Source. So you only write the logic for getting events in the original Source class but different instance executors access different source of events. 
  * Instead, we can implement a method on the Source class to return different instances of it. 
  * Hmm, better ergonomics would be ensuring each subclass of Source implements its own clone method, which can then be further set up. 

* The parallelization implemented in this chapter is not useful when running python. All of our code is single threaded anyway so more executor instances will just slow the processing time over using just one. Java threads can use multiple cores so will actually provide a performance boost. 
  * Instructional though.  

* Async creep rearing its head again. 
  * To set up an async socket connection when initializing the SensorReader, we wrote an async classmethod. Implementing it as a separate routine isn't ergonomic since that might get called twice, which we want to avoid. 
  * But the socket is specific to each instance of the SensorReader, now the clone method would need to be async too. Ughhh. 
  * Hmm, the crux is, if the initializer for Source objects is async, the clone method must be async too. And so the ComponentExecutor class must be initialized with an async method too. 
  * Just an async initializer for the Source was okay. 
  * Async creeps in this way: SourceExecutor - SourceInstanceExecutor - Source with async stream. 
  * So, everything is async. Yep. 