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
import java.io.InputStream;
import java.util.Arrays;
import java.util.Map;


public class AvroConverter implements Converter {
    private static final Logger logger = LoggerFactory.getLogger(AvroConverter.class);
    private JsonConverter jsonConverter;
    private final JsonDeserializer jsonDeserializer = new JsonDeserializer();
    private final IBMSchemaRegistry schemaRegistry = new IBMSchemaRegistry();


    public AvroConverter() {
        jsonConverter = new JsonConverter();
    }

    /**
     * The default schema cache size. We pick 50 so that there's room in the cache for some recurring
     * nested types in a complex schema.
     */
    private Integer schemaCacheSize = 50;

    private org.apache.avro.Schema avroSchema = null;
    private AvroData avroDataHelper = null;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        if (configs.get("schema.cache.size") instanceof Integer) {
            schemaCacheSize = (Integer) configs.get("schema.cache.size");
        }

        avroDataHelper = new AvroData(schemaCacheSize);
        jsonConverter.configure(configs, isKey);
    }

    @Override
    public byte[] fromConnectData(String topic, Headers headers, Schema schema, Object value) {
        logger.info("CONVERTING FROM CONNECT DATA");
        logger.info(topic);
        logger.info(value.toString());
        logger.info(headers.toString());

        logger.info("FINDING AVRO SCHEMA");
        org.apache.avro.Schema avroSchema = this.schemaRegistry.getSchema(headers);
        logger.info(avroSchema.toString());

        logger.info("-- GENERIC RECORD --");
        Schema convertedSchema = avroDataHelper.toConnectSchema(avroSchema);
        GenericRecord genericRecord = (GenericRecord)avroDataHelper.fromConnectData(convertedSchema, value);

        logger.info("-- CONVERTING TO BYTE ARRAY --");
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(this.avroSchema);
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
        return null;
    }

    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        logger.warn(topic);
        logger.warn(value.toString());

        byte[] jsonBytes = jsonConverter.fromConnectData(topic, schema, value);
        JsonNode jsonValue = jsonDeserializer.deserialize(topic, jsonBytes);
        logger.warn("jsonValue");
        logger.warn(jsonValue.toString());

//        TODO: improve this
//        parse JSON payload if it is a string. Currently key is a JSON with messageId while message is a stringified Json
        ObjectMapper mapper = new ObjectMapper();
        try {
            jsonValue = jsonValue.getNodeType() == JsonNodeType.STRING ? mapper.readTree(jsonValue.asText()) : jsonValue;
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

        logger.warn("jsonValue 2");
        logger.warn(jsonValue.toString());
        logger.warn(jsonValue.getNodeType().toString());

        org.apache.avro.Schema avroSchema = null;
        try {
//            TODO: figure how to get schema
            String fileName = jsonValue.has("messageId") ? "key.avsc" : "value.avsc";

            System.out.println("FILENAME: " + fileName);
            InputStream in = this.getClass().getClassLoader().getResourceAsStream(fileName);
            org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();
            this.avroSchema = parser.parse(in);
        } catch (IOException e) {
            e.printStackTrace();
        }
        logger.warn("avroSchema");
        if(this.avroSchema != null) {
            logger.warn(avroSchema.toString());
        }

        GenericRecord genericRecord = (GenericRecord)avroDataHelper.fromConnectData(schema, value);

        logger.warn("created generic record");

        if(genericRecord != null) {
            logger.warn(genericRecord.toString());
        }


        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(this.avroSchema);
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(stream, null);

        try {
            datumWriter.write(genericRecord, encoder);
            encoder.flush();
        } catch (IOException e) {
            throw new DataException("Converting Kafka Connect data to byte[] failed due to serialization error: ", e);
        }
        logger.warn("flusssshed");
        logger.warn(stream.toByteArray().toString());
        return stream.toByteArray();
    }

    @Override
    public SchemaAndValue toConnectData(String topic, byte[] bytes) {
        org.apache.avro.Schema avroSchema = null;
        try {
//            TODO: determine schema based on header
            String fileName = true ? "key.avsc" : "value.avsc";
            String path = String.format("/Users/mdenunez/Documents/projects/tch/kafka-connect-avro-converter/src/main/resources/%s", fileName);
            org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();
            this.avroSchema = parser.parse(new File(path));
        } catch (IOException e) {
            e.printStackTrace();
        }

        GenericRecord genericRecord = null;
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
