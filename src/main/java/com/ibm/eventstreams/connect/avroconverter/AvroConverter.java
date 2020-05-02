package com.ibm.eventstreams.connect.avroconverter;

import com.ibm.eventstreams.connect.avroconverter.schemaregistry.IBMSchemaRegistry;
import com.ibm.eventstreams.connect.avroconverter.schemaregistry.exceptions.SchemaRegistryInitException;
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
import org.apache.kafka.connect.storage.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import static org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA;


public class AvroConverter implements Converter {
    private static final Logger logger = LoggerFactory.getLogger(AvroConverter.class);

    private final JsonConverter jsonConverter = new JsonConverter();

    private IBMSchemaRegistry schemaRegistry;
    private boolean isKeyConverter;

    private AvroData avroDataHelper = null;
    private Integer schemaCacheSize = 50;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        logger.info("Avro Converter Configurations");

        if (configs.get("schema.cache.size") instanceof Integer) {
            schemaCacheSize = (Integer) configs.get("schema.cache.size");
        }

        avroDataHelper = new AvroData(schemaCacheSize);

        logger.info(configs.toString());
        jsonConverter.configure(configs, isKey);
        this.isKeyConverter = isKey;

        try {
            schemaRegistry = new IBMSchemaRegistry(configs);
        } catch (SchemaRegistryInitException e) {
            throw new DataException(e);
        }
    }

    @Override
    public byte[] fromConnectData(String topic, Headers headers, Schema schema, Object value) {
        logger.info("CONVERTING FROM CONNECT DATA");
        logger.info("-- isKey --");
        logger.info(String.valueOf(this.isKeyConverter));
        logger.info("-- topic --");
        logger.info(topic);
        logger.info("-- schema --");
        logger.info(schema != null ? schema.toString() : "null");
        logger.info("-- value --");
        logger.info(value != null ? value.toString() : "null");

        if (value == null) {
            return new byte[0];
        }

        if (isKeyConverter) {
            return value.toString().getBytes();
        }

        logger.info("-- IBM Avro Schema -- ");
        org.apache.avro.Schema avroSchema = this.schemaRegistry.getSchema(headers);
        logger.info(avroSchema.toString());

        logger.info("-- Generic Record --");
        GenericRecord genericRecord;
        try {
            DecoderFactory decoderFactory = new DecoderFactory();
            Decoder decoder = decoderFactory.jsonDecoder(avroSchema, value.toString());
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

            logger.info("TO CONNECT DATA CONVERSION COMPLETE!");
            return new SchemaAndValue(OPTIONAL_STRING_SCHEMA, new String(bytes));
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
        JsonEncoder encoder;
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
        SchemaAndValue jsonSchemaAndValue = avroDataHelper.toConnectData(avroSchema, genericRecord);
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
