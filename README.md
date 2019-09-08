# spark-session-enricher
Enrich events with user sessions &amp; stats on top of them for imaginary ecom site using Spark sql &amp; aggregations.

# Config
```
{
  session = 5 minute
  debug = false
  topN = 10
  inputFile = "example1.csv"
}
```

```session``` - max amount of time between consecutive events in user session

```debug``` - if true, output will be printed to console rather than dumped to file

```topN``` - number of products to calculate for each category, sorted by time users spent on products page

```inputFile``` - name of input file

# SessionEnricherAggregator

This file contains implementation based on Spark Aggregator.
The idea is to group incoming events by userId and category, then aggregate each group using 
SessionAggregator, which has following signature:

```
class SessionAggregator extends Aggregator[Event, List[SessionBuffer], List[EventSession]]
``` 

Input type is ```Event``` - raw data, read from input file.
Second type parameter stands for buffer class - ```List[SessionBuffer]```.
Finally, the last type parameter - ```List[EventSession]```, stands for output class.

```
case class Event(category: String, product: String, userId: String, eventTime: Timestamp, eventType: String)

case class SessionBuffer(events: List[Event], sessionId: String, sessionStartTime: Timestamp, sessionEndTime: Timestamp)

case class EventSession(category: String, product: String, userId: String, eventTime: Timestamp, eventType: String,
                          sessionId: String, sessionStartTime: Timestamp, sessionEndTime: Timestamp)
```

SessionAggregator implements a few methods, let's take a look at most important ones:
```reduce``` and ```merge```.

## Reduce
As far as data that comes to reduce method cannot be guarantied to be sorted, it implements
an algorithm that does not rely on the order.

Reduce method has following signature:
 
 ```reduce(b: List[SessionBuffer], a: Event): List[SessionBuffer]```

Let's describe all possible states of ```b```:
1) empty
2) already contains some data

In #1 we simply add event to buffer, creating a new session.
In #2 we go through all sessions and find any that current event ```a``` can belong to.
That means if current session is ```s```, event is ```e```, 
then:
 
```e.eventTime in (s.sessionStartTime - N, s.sessionEndTime + N)```
 
 where ```N``` is 
```session``` from configuration file described above.
If session found, event can be added to events list of that session, ```sessionStartTime``` and ```sessionEndTime``` are
recalculated. In other case we just create a new session.

## Merge
Reduced data is passed to merge method, it has following signature:
 
 ```merge(b1: List[SessionBuffer], b2: List[SessionBuffer]): List[SessionBuffer]```
 
 Let's consider a situation when simple concatenation of arguments will not work.
 Suppose, we have following list of events (only eventTimes for simplicity): 
 
 ```e = [35, 40, 45]```
 
 Event ```35``` comes first, session buffer is empty, new session will be created.
 Then, ```45``` comes, session buffer is not empty, but those events are too far away from 
 each other, new session will be created again.
 Finally, ```40``` comes, it can belong to both sessions created earlier, but will only be added
 to one of them.
 As a result, we'll have two session:
 
 ```(35, 40)``` and ```(45, 45)```
 
 The idea of merge method is to:
 1) concatenate incoming list of buffers
 2) sort sessions by ```sessionStartTime```
 3) merge sessions like the ones described above
 
 
# SessionEnricherSql

This file contains implementation based on Spark SQL.

There're two methods for calculations user sessions: ```eventWithSession``` and ```eventWithProductSession```.
Rest of the methods used for calculating statistics:

1) For each category find median session duration
2) For each category find # of unique users spending less than 1 min, 1 to 5 mins and more than 5 mins
3) For each category find top 10 products ranked by time spent by users on product pages 


# eventWithSession
As far as this implementation should group consecutive events standing not further away from
each other then ```session``` minutes, we need to somehow calculate that time difference. 
Main idea is to use window over ```userId``` and ```category``` to calculate previous 
```eventTime```.
Next step would be calculating ```isNewSession``` - column, indicating whether a new sessions 
has started or old one continued. Finally, we sum up those ```isNewSession```
, getting sequential id as a sessionId.

# eventWithProductSession 
This method is used for calculating ```topN products```. In this case session contains
all consecutive events of a user, making some actions on a product page. Idea is similar 
to ```eventWithSession``` with main difference that previous ```product``` is calculated instead 
of ```eventTime```.
 
# medianSessionDuration
For calculating median session duration among each category, let's do following: 
1) group events by ```category``` and ```sessionId```
2) sum up ```sessionDuration``` - ```(sessionEnd - sessionStart)```
3) calculate ```percentile_approx(sessionDuration, 0.5)```

# userSpentTimeStat
For each category find # of unique users spending:
1) less than 1 min
2) 1 to 5 mins
3) more than 5 mins.

Let's do following:
1) group events by ```category``` and ```sessionId```
2) sum up ```sessionDuration``` - ```(sessionEnd - sessionStart)```
3) use ```countDistinct``` and ```when``` functions
for implementing all 3 cases described above.  

# topNProducts
For each category find top 10 products ranked by time spent by users on product pages.
Let's do following:

1) group events by ```category``` and ```product```
2) sum up ```sessionDuration``` - ```(sessionEnd - sessionStart)```
3) rank records using ```dense_rank()``` over ```category``` ordered by ```duration``` 