package com.ibm.eventstreams.connect.avroconverter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Map;


public class AvroConverter implements Converter, HeaderConverter {
    private JsonConverter jsonConverter;
    private final JsonDeserializer jsonDeserializer = new JsonDeserializer();
    private static final Logger logger = LoggerFactory.getLogger(AvroConverter.class);

    public AvroConverter() {
        jsonConverter = new JsonConverter();
    }


    @Override
    public void close() {
        jsonConverter.close();
    }

    //    TODO: throw exception when schemas is enabled
    @Override
    public ConfigDef config() {
        return jsonConverter.config();
    }

    @Override
    public void configure(Map<String, ?> configs) {
        jsonConverter.configure(configs);
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        jsonConverter.configure(configs, isKey);
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
            avroSchema = parser.parse(in);
        } catch (IOException e) {
            e.printStackTrace();
        }
        logger.warn("avroSchema");
        if(avroSchema != null) {
            logger.warn(avroSchema.toString());
        }

        GenericRecord genericRecord = null;
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

        if(genericRecord != null) {
            logger.warn(genericRecord.toString());
        }


        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(avroSchema);
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
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
            avroSchema = parser.parse(new File(path));
        } catch (IOException e) {
            e.printStackTrace();
        }

        GenericRecord genericRecord = null;
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
    public SchemaAndValue toConnectHeader(String topic, String headerKey, byte[] value) {
        return jsonConverter.toConnectHeader(topic, headerKey, value);
    }

    @Override
    public byte[] fromConnectHeader(String topic, String headerKey, Schema schema, Object value) {
        return jsonConverter.fromConnectHeader(topic, headerKey, schema, value);
    }
}
