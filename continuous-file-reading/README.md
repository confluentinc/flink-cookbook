# Continuously reading CSV files

## Reading CSV files in Apache Flink

To get started with your first event processing application, you will need to read data from one
or multiple sources.

In this recipe, you are going to continuously read CSV-formatted files from a folder and transform this data into your
data model.

This recipe for Apache Flink is a self-contained recipe that you can directly copy and run from your favorite editor.
There is no need to download Apache Flink or Apache Kafka.

## The CSV input data

The recipe will generate one or more comma-separated values (CSV) files in a temporary directory.
The files are encoded in UTF-8.

```
1,Shadowfax,Sauerfort
2,Quickbeam,Tremblayside
3,Galadriel,Latriciamouth
4,Denethor,Koeppshire
5,Théoden,Larsonland
6,Tom Bombadil,Hegmannton
7,Barliman Butterbur,Madelinechester
```

## The data model

You want to consume these records in your Apache Flink application and make them available in the data model. The data
model is defined in the following POJO:

```java Event.java focus=14:20
@JsonPropertyOrder({"id", "character", "location"})
public class Event {

    /** A Flink POJO must have public fields, or getters and setters */
    public long id;

    public String character;
    public String location;
```

You have to explicitly define the order of fields using the `@JsonPropertyOrder` annotation.

## Setup a FileSource

You are using the Apache
Flink [`FileSource`](https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/datastream/filesystem/#file-source)
connector in the application to connect to your local file system. You can use
Flink's [pluggable file systems](https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/filesystems/overview/)
to connect to other file systems, such as S3.

You can specify a location by providing the argument `--inputFolder`.

```java ContinuousFileReading.java focus=23
    public static void main(String[] args) throws Exception {
        final ParameterTool parameters = ParameterTool.fromArgs(args);

        Path inputFolder = new Path(parameters.getRequired("inputFolder"));

        runJob(inputFolder);
    }
```

Then you configure
the [`CsvReaderFormat`](https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/datastream/formats/csv/)
to use the defined POJO to parse the CSV files.

```java ContinuousFileReading.java focus=38
        CsvReaderFormat<Event> csvFormat = CsvReaderFormat.forPojo(Event.class);
```

To complete the setup you configure the `FileSource` connector with the defined `csvFormat` and the directory that
you want to monitor. You configure the connector to monitor the directory every 5 seconds for any new files.
Because you are monitoring this directory continuously, the connector is set to streaming (unbounded) mode.

```java ContinuousFileReading.java focus=40:42
        FileSource<Event> source =
                FileSource.forRecordStreamFormat(csvFormat, dataDirectory)
                        .monitorContinuously(Duration.of(5, SECONDS))
                        .build();

        final DataStreamSource<Event> file =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "File");
```

## The full recipe

This recipe is self-contained. You can run the `ContinuousFileReadingTest#testProductionJob` class to see the full recipe
in action. The test generates csv files into a temporary directory and will output the data to the console.
You can run it directly via Maven or in your favorite editor such as IntelliJ IDEA or Visual Studio Code.

See the comments included in the code for more
details.
