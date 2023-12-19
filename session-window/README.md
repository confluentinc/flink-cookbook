# Using session windows

## Grouping data that belongs to one session

Session windows are commonly used when processing clickstream data. When a user visits your website and performs 
multiple actions, these actions have different intervals between them. At some point, the user has completed its 
goal on your website and goes elsewhere. A session window closes when no actions have been reported for a certain 
period of time. When you determine a session, you can create insights such as the number of interactions per user. 

In this recipe, you are going to consume events from Apache Kafka and count the number of interactions per session.
A session is finished when no activities have occurred 5 seconds after the last one. 

This recipe for Apache Flink is a self-contained recipe that you can directly copy and run from your favorite editor.
There is no need to download Apache Flink or Apache Kafka.

## The JSON input data

The recipe uses Kafka topic `input`, containing JSON-encoded records.

```json
{"id":1,"user":"Grover Larson","timestamp":"2022-07-24T18:42:06.064708Z"}
{"id":2,"user":"Brian Kiehn","timestamp":"2022-07-24T18:42:06.382717Z"}
{"id":3,"user":"Santos Romaguera","timestamp":"2022-07-24T18:42:06.627165Z"}
{"id":4,"user":"Bella Marks","timestamp":"2022-07-24T18:42:06.866655Z"}
{"id":5,"user":"Elvis Welch","timestamp":"2022-07-24T18:42:07.462994Z"}
```

## Define your session window

You are going to use Flink's [Session Windows](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/operators/windows/#session-windows) 
to create a session window and define the length of your session. 

To define your session window, you need to define on which key you want to aggregate your results. This can be compared
with a `GROUP BY` statement in SQL. You are using the value from `user` and you define your session window as having no 
data occurring for that specific `user` for 5 seconds.

```java SessionWindow.java focus=58:59
                kafka.keyBy(event -> event.user)
                        .window(EventTimeSessionWindows.withGap(Time.seconds(5)))
```

## Aggregate the number of interactions

You are going to aggregate the number of interactions with Flink's built-in [`AggregateFunction`](https://nightlies.apache.org/flink/flink-docs-stable/api/java/index.html?org/apache/flink/api/common/functions/AggregateFunction.html)
This uses `UserActivity` POJO:

```java UserActivity.java focus=23:27
public class UserActivity {
    public String user;
    public int numInteractions;
    public Instant activityStart;
    public Instant activityEnd;
```

### Adding data to a session window

Inside the `AggregateFunction` function you use the `UserActivity` accumulator to add data to a session window for each 
event. If a session window doesn't exist yet for this user, it will be created. 

```java SessionWindow.java focus=68:78
                                    public UserActivity add(Event value, UserActivity accumulator) {
                                        accumulator.user = value.user;
                                        accumulator.activityStart =
                                                ObjectUtils.min(
                                                        value.timestamp, accumulator.activityStart);
                                        accumulator.activityEnd =
                                                ObjectUtils.max(
                                                        value.timestamp, accumulator.activityEnd);
                                        accumulator.numInteractions += 1;
                                        return accumulator;
                                    }
```

### Merging data in one session window

If a session window already exists for this user and the defined gap of 5 seconds hasn't passed yet, the session windows
for that user needs to be merged. That's why you're using the `merge` method in the `AggregateFunction` function to 
aggregate the total number of interactions and to determine what is the correct timestamp for when the session window 
was started and when it ended. 

```java SessionWindow.java focus=81:88
                                    public UserActivity merge(UserActivity a, UserActivity b) {
                                        a.numInteractions += b.numInteractions;
                                        a.activityStart =
                                                ObjectUtils.min(a.activityStart, b.activityStart);
                                        a.activityEnd =
                                                ObjectUtils.max(a.activityEnd, b.activityEnd);
                                        return a;
                                    }
```

### Getting results of a session window

If the gap of 5 seconds has passed, you will return the results of the accumulator. This contains the final result
of the session window.

```java SessionWindow.java focus=91:93
                                    public UserActivity getResult(UserActivity accumulator) {
                                        return accumulator;
                                    }
```

## The full recipe

This recipe is self-contained. You can run the `SessionWindowTest#testProductionJob` class to see the full recipe
in action. That test uses an embedded Apache Kafka and Apache Flink setup, so you can run it directly via Maven or in 
your favorite editor such as IntelliJ IDEA or Visual Studio Code.
