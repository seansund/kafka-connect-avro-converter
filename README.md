# kafka-connect-avro-converter
A converter for the Kafka Connect framework that supports the Apache Avro data serialisation format

The connector is supplied as source code which you can easily build into a JAR file.

## Installation

1. Clone the repository with the following command:

```bash
git@github.com:ibm-messaging/kafka-connect-avro-converter.git
```

2. Change directory to the `kafka-connect-avro-converter` directory:

```shell
cd kafka-connect-avro-converter
```

3. Build the connector using Maven:

```bash
mvn clean package
```

4. Copy the compiled jar file into the `/usr/local/share/java/` directory:

```bash
cp target/kafka-connect-kafka-connect-avro-converter-1.0-SNAPSHOT-jar-with-dependencies.jar /usr/local/share/java/
```
