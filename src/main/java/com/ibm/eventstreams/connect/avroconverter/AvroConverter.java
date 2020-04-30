package com.ibm.eventstreams.connect.avroconverter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.eventstreams.connect.avroconverter.schemaregistry.IBMSchemaRegistry;
import com.ibm.eventstreams.connect.avroconverter.schemaregistry.exceptions.SchemaRegistryInitException;
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
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;


public class AvroConverter implements Converter {
    private static final Logger logger = LoggerFactory.getLogger(AvroConverter.class);

    private final JsonDeserializer jsonDeserializer = new JsonDeserializer();
    private final IBMSchemaRegistry schemaRegistry = new IBMSchemaRegistry();
    private final JsonConverter jsonConverter = new JsonConverter();

    private boolean isKeyConverter;

    public AvroConverter() throws SchemaRegistryInitException {}

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        logger.info("Avro Converter Configurations");
        logger.info(configs.toString());
        jsonConverter.configure(configs, isKey);
        this.isKeyConverter = isKey;
    }

    @Override
    public byte[] fromConnectData(String topic, Headers headers, Schema schema, Object value) {
        logger.info("CONVERTING FROM CONNECT DATA");
        logger.info("-- isKey --");
        logger.info(String.valueOf(this.isKeyConverter));
        logger.info("-- topic --");
        logger.info(topic);
        logger.info("-- value --");
        logger.info(value != null ? value.toString() : "null");

        logger.info("-- Converting to JSON --");
        byte[] jsonBytes = jsonConverter.fromConnectData(topic, schema, value);
        JsonNode jsonValue = jsonDeserializer.deserialize(topic, jsonBytes);

        if (jsonValue == null) {
            return new byte[0];
        }

        logger.info(jsonValue.toString());
        logger.info(jsonValue.getNodeType().toString());

        if (isKeyConverter) {
            return jsonValue.asText().getBytes();
        }

        // This is to guard against stringified JSON
        // TODO is there a better way
        try {
            ObjectMapper mapper = new ObjectMapper();
            jsonValue = mapper.readTree(jsonValue.asText());
        } catch (JsonProcessingException error) {
            throw new DataException(error);
        }

        logger.info("-- IBM Avro Schema -- ");
        org.apache.avro.Schema avroSchema = this.schemaRegistry.getSchema(headers);
        logger.info(avroSchema.toString());

        logger.info("-- Generic Record --");
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
        logger.info(genericRecord.toString());

        logger.info("-- Byte Stream --");
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(avroSchema);
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(stream, null);

        try {
            datumWriter.write(genericRecord, encoder);
            encoder.flush();
        } catch (IOException e) {
            throw new DataException("Converting Kafka Connect data to byte[] failed due to serialization error: ", e);
        }
        logger.info(Arrays.toString(stream.toByteArray()));

        logger.info("FROM CONNECT DATA CONVERSION COMPLETE!");
        return stream.toByteArray();
    }

    @Override
    public SchemaAndValue toConnectData(String topic, Headers headers, byte[] bytes) {
        logger.info("CONVERSION TO CONNECT DATA");
        logger.info("-- isKey --");
        logger.info(String.valueOf(this.isKeyConverter));
        logger.info("-- topic --");
        logger.info(topic);
        logger.info("-- value --");
        logger.info(bytes != null ? Arrays.toString(bytes) : "null");

        if (isKeyConverter) {
            logger.info("-- Converting as JSON --");
            SchemaAndValue sv = jsonConverter.toConnectData(topic, bytes);
            logger.info("TO CONNECT DATA CONVERSION COMPLETE!");
            return sv;
        }

        logger.info("-- IBM Avro Schema -- ");
        org.apache.avro.Schema avroSchema = this.schemaRegistry.getSchema(headers);
        logger.info(avroSchema.toString());

        logger.info("-- Generic Record --");
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
        logger.info(genericRecord.toString());

        logger.info("-- Byte Stream --");
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(avroSchema);
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        JsonEncoder encoder = null;
        try {
            encoder = EncoderFactory.get().jsonEncoder(avroSchema, stream);
        } catch (IOException e) {
            throw new DataException("Could not encode byte stream based on avro schema: ", e);
        }

        if(encoder == null) {
            return null;
        }

        try {
            datumWriter.write(genericRecord, encoder);
            encoder.flush();
        } catch (IOException e) {
            throw new DataException("Converting Kafka Connect data to byte[] failed due to serialization error: ", e);
        }
        logger.warn(Arrays.toString(stream.toByteArray()));

        logger.info("-- Converting as JSON --");
        SchemaAndValue jsonSchemaAndValue = jsonConverter.toConnectData(topic, stream.toByteArray());
        logger.info("TO CONNECT DATA CONVERSION COMPLETE!");
        return jsonSchemaAndValue;
    }

    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        return this.fromConnectData(topic, null, schema, value);
    }

    @Override
    public SchemaAndValue toConnectData(String topic, byte[] bytes) {
        return this.toConnectData(topic, null, bytes);
    }
}
