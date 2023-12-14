# Writing an application in Kotlin™

## Use the Kotlin programming language

In this recipe, you will create an Apache Flink application that every 5 seconds counts the number of words that are 
posted on an Apache Kafka topic using the [Kotlin programming language](https://kotlinlang.org/).

Flink natively supports Java and Scala, but you can use any JVM language to create your application.

This recipe for Apache Flink is a self-contained recipe that you can directly copy and run from your favorite editor.
There is no need to download Apache Flink or Apache Kafka.

## Kotlin project setup

This project setup is based on the
official [Apache Flink Maven quickstart for the Java API](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/configuration/overview/#getting-started)
.

To add support for Kotlin, you add dependencies and configure them correctly in the Maven project.
The full documentation can be found at the [Kotlin Maven](https://kotlinlang.org/docs/maven.html) build tool
documentation.

### Add dependency on Kotlin standard library

The `kotlin-stdlib-jdk8` artifact can be used with JDK 8 and JDK 11 (or higher).

```xml pom.xml focus=49:54
        <!-- The Kotlin standard library + extensions to work with Java 7/8 features. -->
        <dependency>
            <groupId>org.jetbrains.kotlin</groupId>
            <artifactId>kotlin-stdlib-jdk8</artifactId>
            <version>1.7.22</version>
        </dependency>
```

### Configure source/test directories

You need to explicitly define your Kotlin source- and test directories in Maven, so you can reference them
in the necessary configuration for the Kotlin Maven plugin.

```xml pom.xml focus=153:154
        <sourceDirectory>src/main/kotlin</sourceDirectory>
        <testSourceDirectory>src/test/kotlin</testSourceDirectory>
```

### Setup Kotlin Maven plugin

The Kotlin Maven plugin uses the previously configured values and connects them to the correct Maven lifecycles.

```xml pom.xml focus=202:225
            <plugin>
                <groupId>org.jetbrains.kotlin</groupId>
                <artifactId>kotlin-maven-plugin</artifactId>
                <version>1.7.22</version>
                <executions>
                    <execution>
                        <id>compile</id>
                        <phase>process-sources</phase>
                        <goals>
                            <goal>compile</goal>
                        </goals>
                    </execution>
                    <execution>
                        <id>test-compile</id>
                        <phase>test-compile</phase>
                        <goals>
                            <goal>test-compile</goal>
                        </goals>
                    </execution>
                </executions>
                <configuration>
                    <jvmTarget>${target.java.version}</jvmTarget>
                </configuration>
            </plugin>
```

### Configure main class in the shade-plugin

By configuring your main class, Maven can properly create a fat JAR when you are building the recipe.

```xml pom.xml focus=263:266
<!-- Note that the Kotlin compiler adds a "Kt" suffix to the class name! -->
<mainClass>io.confluent.developer.cookbook.flink.WordCountKt</mainClass>
```

## The record input data

The recipe uses Kafka topic `input`, containing String records.

```text
"Frodo Baggins"
"Meriadoc Brandybuck"
"Boromir"
"Gollum"
"Sauron"
...
```

## Count the number of words

The recipe uses the Apache
Flink [`KafkaSource`](https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/datastream/kafka/#kafka-source)
connector in the application to connect to your Apache Kafka broker.

The recipe uses a Kotlin data class.
The class has public and mutable properties, with a no-arg constructor so that Flink treats it as a POJO.
This has performance benefits, and more importantly allows schema evolution (i.e., you can add/remove fields) when using this class for state.
An immutable data class would be serialized via Kryo, because Flink does not have built-in support for such classes.

Alternatively you could also use Avro types, or implement custom TypeSerializers for your data types.

```kotlin WordCount.kt focus=19:20 
data class Event(var word: String, var count: Int) {
    constructor() : this("", 0)
}
```

Each appearing word and the number of appearances will be stored in this data class. Everytime 5 seconds have passed, 
the application will output the word and how many times it has appeared in that timeframe. Since the data is unbounded, 
the output to the console will never stop.

```kotlin WordCount.kt focus=52:58
    val counts = textLines
        .flatMap(Tokenizer())
        .name("tokenizer")
        .keyBy { value -> value.word }
        .window(TumblingEventTimeWindows.of(Time.seconds(5)))
        .reduce(Sum())
        .name("counter")
```

## The full recipe

This recipe is self-contained. You can run the `WordCountTest#testProductionJob` test to see the full recipe
in action. That test uses an embedded Kafka and Flink instance, so you can run it directly via
Maven or in your favorite editor such as IntelliJ IDEA or Visual Studio Code.

