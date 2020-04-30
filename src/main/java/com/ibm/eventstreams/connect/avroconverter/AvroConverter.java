package com.ibm.eventstreams.connect.avroconverter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.connect.avro.AvroData;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.storage.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;


public class AvroConverter implements Converter {
    private JsonConverter jsonConverter;
    private final JsonDeserializer jsonDeserializer = new JsonDeserializer();
    private static final Logger logger = LoggerFactory.getLogger(AvroConverter.class);
    private final IBMSchemaRegistry schemaRegistry = new IBMSchemaRegistry();
    org.apache.avro.Schema keySchema;
    private final String KEY_SCHEMA_PATH = "/Users/mdenunez/Documents/projects/tch/kafka-connect-avro-converter/src/main/resources/key.avsc";
    private boolean isKey;

    public AvroConverter() throws IOException {
        jsonConverter = new JsonConverter();

        try {
            org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();
            keySchema = parser.parse(new File(KEY_SCHEMA_PATH));
        } catch (IOException error) {
            logger.error("key schema could not be found: " + error.getMessage());
            throw error;
        }
    }

    private org.apache.avro.Schema avroSchema = null;
    private AvroData avroDataHelper = null;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        jsonConverter.configure(configs, isKey);
        this.isKey = isKey;
    }

    @Override
    public byte[] fromConnectData(String topic, Headers headers, Schema schema, Object value) {
        logger.info("CONVERTING FROM CONNECT DATA");
        logger.info(topic);
        logger.info("value");
        logger.info(value != null ? value.toString() : "null");

        byte[] jsonBytes = jsonConverter.fromConnectData(topic, schema, value);
        JsonNode jsonValue = jsonDeserializer.deserialize(topic, jsonBytes);

        if(jsonValue != null) {
            logger.warn(jsonValue.toString());
        }
        logger.info("jsonValue.getNodeType()");
        logger.info(jsonValue.getNodeType().toString());


        if (isKey) {
            return jsonValue.asText().getBytes();
        }
        logger.warn("jsonValue.toString()");
        logger.warn(jsonValue.toString());
        // TODO this is to guard against stringified JSON but is there a better way
        try {
            ObjectMapper mapper = new ObjectMapper();
            jsonValue = mapper.readTree(jsonValue.asText());
        } catch (JsonProcessingException error) {
            throw new DataException(error);
        }

        logger.info("IBM AVRO SCHEMA");
        org.apache.avro.Schema avroSchema = this.schemaRegistry.getSchema(headers);
        logger.info(avroSchema.toString());

        logger.info("-- GENERIC RECORD --");
        GenericRecord genericRecord;
        try {
            DecoderFactory decoderFactory = new DecoderFactory();
            Decoder decoder = decoderFactory.jsonDecoder(avroSchema, jsonValue.toString());
            DatumReader<GenericData.Record> reader =
                    new GenericDatumReader<>(avroSchema);
            genericRecord = reader.read(null, decoder);
        } catch (IOException e) {
            throw new DataException("Converting Kafka Connect data to byte[] failed due to serialization error: ", e);
        }

        logger.warn("created generic record");
        logger.warn(genericRecord.toString());

        logger.info("-- CONVERTING TO BYTE ARRAY --");
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(avroSchema);
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(stream, null);

        try {
            datumWriter.write(genericRecord, encoder);
            encoder.flush();
        } catch (IOException e) {
            throw new DataException("Converting Kafka Connect data to byte[] failed due to serialization error: ", e);
        }
        logger.info("flushed");
        logger.info(Arrays.toString(stream.toByteArray()));
        return stream.toByteArray();
    }

    @Override
    public SchemaAndValue toConnectData(String topic, Headers headers, byte[] bytes) {
        logger.warn("2 --- To CONNECT DATA ---");
        if (isKey) {
            return jsonConverter.toConnectData(topic, bytes);
        }
        org.apache.avro.Schema avroSchema = this.schemaRegistry.getSchema(headers);

        GenericRecord genericRecord;
        try {
            DecoderFactory decoderFactory = new DecoderFactory();
            Decoder decoder = decoderFactory.binaryDecoder(bytes, null);
            DatumReader<GenericData.Record> reader =
                    new GenericDatumReader<>(avroSchema);
            genericRecord = reader.read(null, decoder);
        } catch (IOException e) {
            throw new DataException("Converting Kafka Connect data to byte[] failed due to serialization error: ", e);
        }

        logger.warn("created generic record");
        logger.warn(genericRecord.toString());

        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(avroSchema);
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        JsonEncoder encoder = null;
        try {
            encoder = EncoderFactory.get().jsonEncoder(avroSchema, stream);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            datumWriter.write(genericRecord, encoder);
            encoder.flush();
        } catch (IOException e) {
            throw new DataException("Converting Kafka Connect data to byte[] failed due to serialization error: ", e);
        }
        logger.warn("flusssshed");
        logger.warn(stream.toByteArray().toString());
        SchemaAndValue jsonSchemaAndValue = jsonConverter.toConnectData(topic, stream.toByteArray());
        return jsonSchemaAndValue;
    }

    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        return this.fromConnectData(topic, null, schema, value);
    }

    @Override
    public SchemaAndValue toConnectData(String topic, byte[] bytes) {
        org.apache.avro.Schema avroSchema = null;
        logger.warn("2 --- To CONNECT DATA ---");
        try {
//            TODO: determine schema based on header
            String fileName = true ? "key.avsc" : "value.avsc";
            String path = String.format("/Users/mdenunez/Documents/projects/tch/kafka-connect-avro-converter/src/main/resources/%s", fileName);
            org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();
            this.avroSchema = parser.parse(new File(path));
        } catch (IOException e) {
            e.printStackTrace();
        }

        GenericRecord genericRecord;
        try {
            DecoderFactory decoderFactory = new DecoderFactory();
            Decoder decoder = decoderFactory.binaryDecoder(bytes, null);
            DatumReader<GenericData.Record> reader =
                    new GenericDatumReader<>(this.avroSchema);
            genericRecord = reader.read(null, decoder);
        } catch (IOException e) {
            throw new DataException("Converting Kafka Connect data to byte[] failed due to serialization error: ", e);
        }

        logger.warn("created generic record");
        logger.warn(genericRecord.toString());

        return avroDataHelper.toConnectData(this.avroSchema, genericRecord);
    }
}
