package com.ibm.eventstreams.connect.avroconverter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeType;
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

    public AvroConverter() {
        jsonConverter = new JsonConverter();
    }

    private org.apache.avro.Schema avroSchema = null;
    private AvroData avroDataHelper = null;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] fromConnectData(String topic, Headers headers, Schema schema, Object value) {
        logger.info("CONVERTING FROM CONNECT DATA");
        logger.info(topic);

        logger.info("CONVERTING TO JSON");
        byte[] jsonBytes = jsonConverter.fromConnectData(topic, schema, value);
        JsonNode jsonValue = jsonDeserializer.deserialize(topic, jsonBytes);

        if(jsonValue != null) {
            logger.warn(jsonValue.toString());
        }

//        // TODO this is to guard against stringified JSON but is there a better way
//        ObjectMapper mapper = new ObjectMapper();
//        try {
//            jsonValue = jsonValue.getNodeType() == JsonNodeType.STRING ? mapper.readTree(jsonValue.asText()) : jsonValue;
//        } catch (JsonProcessingException e) {
//            e.printStackTrace();
//        }

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
    public SchemaAndValue toConnectData(String topic, Headers headers, byte[] value) {
        logger.warn("1 --- To CONNECT DATA ---");
        return null;
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
