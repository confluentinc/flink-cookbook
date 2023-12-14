# Migrating state away from Kryo

## Background

Data is one of the cornerstones of Flink applications. Almost all Apache Flink applications execute business logic that 
requires to know what has happened in the past or access to intermediate results. The data is modeled in a data model. 
Over time, use-cases and business requirements evolve, and so must the data model.

To that end Flink
introduced [schema evolution](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/fault-tolerance/serialization/schema_evolution/),
allowing users who use POJO or Avro types to change their data model.

Users mostly are taking great care to ensure they use POJOs everywhere; unfortunately, that in and of itself isn't sufficient.

For example, have a look at this type (the structure of `Pojo2` is irrelevant):
```java Pojo1.java
public class Pojo1 {
    public List<Pojo2> pojos;
}
```

Can `Pojo1` evolve? Of course, it is a POJO after all!

Can `Pojo2` evolve? No!

This is because the `List<Pojo2>` will be serialized with Kryo, which does not support schema evolution.

Manifesting itself in many different forms, be it as `Maps`, `Optionals`, Scala collections, etc., the common theme is that they are
all cases of _non_-POJOs containing POJOs.

This issue has a tendency to go unnoticed until schema evolution is attempted (in part because in certain cases Flink
does not inform the user that Kryo is used!), and once found, users face the challenge
of having to migrate their state away from Kryo.

In this recipe you are going to migrate a value state containing a POJO that was partially serialized with Kryo to
another serializer using
the [State Processor API](https://nightlies.apache.org/flink/flink-docs-stable/docs/libs/state_processor_api/).

## The application

For the purposes of this recipe, there is an application that stores `Events` in a value state.
It generates a stream of `Events`, keys the stream by `userId`, and passes the data into a `LatestEventFunction`.

```java Job.java focus=27:34
        DataStream<Event> transactionStream =
                env.fromSequence(0, 1_000_000_000).map(new EventGenerator());

        transactionStream
                .keyBy(getEventKeySelector())
                .process(new LatestEventFunction())
                .uid(STATEFUL_OPERATOR_UID);
```

The `LatestEventFunction` stores the latest received element in state.

```java Job.java focus=20,24,33,37
public class LatestEventFunction extends KeyedProcessFunction<Long, Event, Event> {

    private ValueState<Event> latestEvent;

    @Override
    public void open(Configuration config) {
        latestEvent = getRuntimeContext().getState(createStateDescriptor());
    }

    @Override
    public void processElement(
            Event incoming,
            KeyedProcessFunction<Long, Event, Event>.Context context,
            Collector<Event> out)
            throws Exception {
        latestEvent.update(incoming);
    }

    public static ValueStateDescriptor<Event> createStateDescriptor() {
        return new ValueStateDescriptor<>("latest event", TypeInformation.of(Event.class));
    }
}
```

Note that the application and operator don't have any practical application; they only exist for demonstration purposes.

## The problem

Our state contains `Events`, which is a POJO containing a `long` user ID and a `List` of `SubEvents`.

```java Event.java
    public long userId;
    public List<SubEvent> subEvents;
```

`SubEvent` is another POJO:

```java SubEvent.java focus=7:15
public class SubEvent {

    public String content1;
    public String content2;
```

Both of these classes are proper POJOs. They are serialized with the PojoSerializer and _on their own_ would support
schema evolution.

However, the `List` throws a wrench into the whole affair, because it, _and its contents_, will be serialized with Kryo.
This happens because List itself is neither a POJO nor another type that Flink natively supports.
With Kryo not supporting schema evolution you now end up in a strange situation where you can't evolve a POJO.

<details>
<summary>Why "the PojoSerializer supports schema evolution" is misleading:</summary>
<p></p>

Serializers for structured types (like POJOs, Tuples, Collections) are composed of several serializers, one for each of
the contained fields. Each of those serializers individually controls whether it supports schema evolution or further
serializer nesting for the field it is responsible for.

For example, let's take the `Event` class.
The POJO serializer for this class contains 2 serializers: one each for the `userId`/`subEvents`
fields. The schema evolution support that these POJO serializers provide is limited to the _top-level_ structure of the
POJO; you can add/remove fields, but you aren't _necessarily_ able to change `SubEvent` because that is handled by
another serializer.

When Kryo is used for the `subEvents` field then you can't evolve the `SubEvent` class, because Kryo does not support
schema evolution, and it serializes both the
list _and_ its contents, never deferring the serialization of the `SubEvent` class to another (POJO) serializer.
Meanwhile, the ListSerializer does rely on other serializers, and in this
case will use the PojoSerializer internally for the `SubEvents`, allowing us to evolve the type.
</details>

## Migration

### Taking control

The first thing you do is take control over which serializer is used for the List, using the `@TypeInfo` annotation.

```java Event.java focus=19:20
    @TypeInfo(SubEventListTypeInfoFactory.class)
    public List<SubEvent> subEvents;
```

This annotation allows you to supply a `TypeInfoFactory`, which Flink will call when it encounters the annotated field
during type extraction. You can then return a `TypeInformation` of your choosing, which in the end provides the
serializer for the annotated field.

The factory needs 2 code paths, so that during migration the state will be read using Kryo, but written with the new
serializer.
For this recipe you will leverage Flink's `ListTypeInfo` for brevity, but you could also implement a fully-custom
`TypeInformation` and `TypeSerializer`.

Flink does have a built-in serializer for lists, but its not used by default. Changing this would break existing states that used Kryo for lists.

To control which code path is used you unfortunately have to do some static shenanigans, because the access to the
factory happens in the background, not allowing you to directly provide an appropriately configured factory.
You will see later how this is used.

```java SubEventListTypeInfoFactory.java focus=43,45,47:49,62:65
public class SubEventListTypeInfoFactory extends TypeInfoFactory<List<SubEvent>> {

    // this flag can (and should!) be removed after migration
    private static boolean USE_LIST_TYPE_INFO = true;

    private final boolean useListTypeInfo;

    public SubEventListTypeInfoFactory() {
        this.useListTypeInfo = USE_LIST_TYPE_INFO;
    }

    @Override
    public TypeInformation<List<SubEvent>> createTypeInfo(
            Type t, Map<String, TypeInformation<?>> genericParameters) {
        if (useListTypeInfo) {
            return new ListTypeInfo<>(TypeInformation.of(SubEvent.class));
        } else {
            // fall back to standard type extraction (i.e., use Kryo)
            return TypeInformation.of(new TypeHint<>() {});
        }
    }

    public static AutoCloseable temporarilyEnableKryoPath() {
        USE_LIST_TYPE_INFO = false;
        return () -> USE_LIST_TYPE_INFO = true;
    }
}
```

### Rewriting state

The [State Processor API](https://nightlies.apache.org/flink/flink-docs-stable/docs/libs/state_processor_api/) is
a powerful tool that allows you to create and modify savepoints.

The API allows you to treat existing state as a source or sink; you write functions that extract data from state, which
is passed to another set of functions that write it into state.

You will use it to read a particular state with the Kryo-infected POJO serializer, and create a new savepoint
containing the same state but serialized with a POJO serializer that leverages the `ListSerializer` instead.

#### Reading state

To extract the state you will use the `SavepointReader` API.

Given the path to the savepoint you create a `SavepointReader`
and use `readKeyedState()` to setup the extraction (because value state is a keyed state!),  
providing the UID of the operator whose state you want to read,  
a reader function that extracts the state,  
the type information of the key,  
and the type information of the state.

```java Migration.java focus=63:72
    static DataStream<Event> readSavepointAndExtractValueState(
            final StreamExecutionEnvironment env, final String savepointPath) throws Exception {
        final SavepointReader savepoint = SavepointReader.read(env, savepointPath);

        return savepoint.readKeyedState(
                Job.STATEFUL_OPERATOR_UID,
                new SimpleValueStateReaderFunction<>(LatestEventFunction.createStateDescriptor()),
                TypeInformation.of(Long.class),
                TypeInformation.of(Event.class));
    }
```

The reader function emits the value held in state:

```java SimpleValueStateReaderFunction.java focus=29:47
public class SimpleValueStateReaderFunction<K, T> extends KeyedStateReaderFunction<K, T> {

    private final ValueStateDescriptor<T> descriptor;
    private ValueState<T> state;

    public SimpleValueStateReaderFunction(ValueStateDescriptor<T> descriptor) {
        this.descriptor = descriptor;
    }

    @Override
    public void open(Configuration configuration) {
        state = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void readKey(K k, Context context, Collector<T> collector) throws Exception {
        collector.collect(state.value());
    }
}
```

#### Writing state

To write the state you will use the `SavepointWriter` API.

You create a `SavepointWriter` using `fromExistingSavepoint()`,  
define a transformation that uses the previously extracted state as its
input,  
key the stream using the key selector from the application,  
apply a bootstrap function to write the data into state,  
add the transformation to the SavepointWriter,  
and finalize the preparation by providing the path to which the savepoint should be written.

```java Migration.java focus=74:94
    static void writeSavepointWithValueState(
            final DataStream<Event> state,
            final String sourceSavepointPath,
            final String targetSavepointPath)
            throws Exception {
        final SavepointWriter savepointWriter =
                SavepointWriter.fromExistingSavepoint(sourceSavepointPath);

        final StateBootstrapTransformation<Event> transformation =
                OperatorTransformation.bootstrapWith(state)
                        .keyBy(Job.getEventKeySelector())
                        .transform(
                                new SimpleValueStateBootstrapFunction<>(
                                        LatestEventFunction.createStateDescriptor()));

        savepointWriter
                .removeOperator(Job.STATEFUL_OPERATOR_UID)
                .withOperator(Job.STATEFUL_OPERATOR_UID, transformation);

        savepointWriter.write(targetSavepointPath);
    }
```

The bootstrap function writes every received value into state:

```java SimpleValueStateBootstrapFunction.java focus=30:50
public class SimpleValueStateBootstrapFunction<K, T> extends KeyedStateBootstrapFunction<K, T> {

    private final ValueStateDescriptor<T> descriptor;
    private ValueState<T> state;

    public SimpleValueStateBootstrapFunction(ValueStateDescriptor<T> descriptor) {
        this.descriptor = descriptor;
    }

    @Override
    public void open(Configuration parameters) {
        state = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(T value, KeyedStateBootstrapFunction<K, T>.Context ctx)
            throws Exception {
        state.update(value);
    }
}
```

`SavepointWriter#fromExistingSavepoint()` automatically determines the maxParallelism and state backend from the savepoint,
while also forwarding the states of all operators that you don't explicitly process. This is perfect for this recipe because you only want
to change the serializer of a particular state, without changing the statebackend or maxParallelism.

If an operator contains multiple states, like 2 value states, then the reader/bootstrap functions for that operator
must extract and write both states, even if you only want to modify one of them.

Wrapping all extracted state values in a Tuple is a good way to implement this.

#### Putting it together

Now that you can read and write state you use the two methods to define a migration function.

To control which serializer is used for reading the state,
you use `SubEventListTypeInfoFactory.temporarilyEnableKryoPath()` to enable Kryo when reading
state.  
Outside of this try-with-resources statement the list serializer will be used instead.

```java Migration.java focus=46:61
    static void migrateSavepoint(final String sourceSavepointPath, final String targetSavepointPath)
            throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // extract the state while using the Kryo serializer
        final DataStream<Event> state;
        try (AutoCloseable ignored = SubEventListTypeInfoFactory.temporarilyEnableKryoPath()) {
            state = readSavepointAndExtractValueState(env, sourceSavepointPath);
        }

        // write a new savepoint while using the List serializer (new default set by the
        // SubEventListTypeInfoFactory)
        writeSavepointWithValueState(state, sourceSavepointPath, targetSavepointPath);

        env.execute();
    }
```

### Post-migration

After the migration is complete you keep the `@TypeInfo` annotation in the Event, to ensure the list serializer continues
to be used.
The type info factory can be cleaned up however, and should look like this:

```java SubEventListTypeInfoFactory.java focus=35:42
@SuppressWarnings("unused")
public class PostMigrationSubEventListTypeInfoFactory extends TypeInfoFactory<List<SubEvent>> {

    @Override
    public TypeInformation<List<SubEvent>> createTypeInfo(
            Type t, Map<String, TypeInformation<?>> genericParameters) {
        return new ListTypeInfo<>(TypeInformation.of(SubEvent.class));
    }
}
```

## The full recipe

This recipe is self-contained. Follow the instructions in the `MigrationTest` javadocs to see the recipe
in action. That test uses an embedded Apache Flink setup, so you can run it directly via
Maven or in your favorite editor such as IntelliJ IDEA or Visual Studio Code.
