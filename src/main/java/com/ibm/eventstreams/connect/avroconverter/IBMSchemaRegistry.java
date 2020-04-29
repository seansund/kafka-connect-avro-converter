/*
 *
 * Copyright 2020 IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.ibm.eventstreams.connect.avroconverter;

import com.ibm.eventstreams.serdes.SchemaInfo;
import com.ibm.eventstreams.serdes.SchemaRegistry;
import com.ibm.eventstreams.serdes.SchemaRegistryConfig;
import com.ibm.eventstreams.serdes.exceptions.SchemaRegistryApiException;
import com.ibm.eventstreams.serdes.exceptions.SchemaRegistryAuthException;
import com.ibm.eventstreams.serdes.exceptions.SchemaRegistryConnectionException;
import com.ibm.eventstreams.serdes.exceptions.SchemaRegistryServerErrorException;
import org.apache.avro.Schema;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;
import java.util.Objects;
import java.util.Properties;

public class IBMSchemaRegistry {

    private static final Logger logger = LoggerFactory.getLogger(IBMSchemaRegistry.class);
    private SchemaRegistry schemaRegistry = null;

    public IBMSchemaRegistry() {
        try {
            this.schemaRegistry = new SchemaRegistry(configure());
        } catch (KeyManagementException | NoSuchAlgorithmException | SchemaRegistryAuthException | SchemaRegistryServerErrorException | SchemaRegistryApiException | SchemaRegistryConnectionException e) {
            e.printStackTrace();
        }
    }

    private Properties configure() {
        Properties props = new Properties();

//        String trustStoreFilePath = Objects.requireNonNull(this.getClass().getClassLoader().getResource("es-cert.jks")).getPath();
//
//        System.out.println("TRUST STORE PATH " + trustStoreFilePath);

        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "tch-kafka-dev-ibm-es-proxy-route-bootstrap-eventstreams.tchcluster-cp4i-0143c5dd31acd8e030a1d6e0ab1380e3-0000.us-south.containers.appdomain.cloud:443");
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        props.put(SslConfigs.SSL_PROTOCOL_CONFIG, "TLSv1.2");
        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "/Users/crossman@us.ibm.com/Downloads/es-cert.jks");
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "password");
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        String saslJaasConfig = "org.apache.kafka.common.security.plain.PlainLoginModule required "
                + "username=\"token\" password=\"V0rn1QUy2sydGS990l66MWGhSnsumiiVmBYNj_FUsoMQ\";";
        props.put(SaslConfigs.SASL_JAAS_CONFIG, saslJaasConfig);

        props.put(SchemaRegistryConfig.PROPERTY_API_URL, "https://tch-kafka-dev-ibm-es-rest-route-eventstreams.tchcluster-cp4i-0143c5dd31acd8e030a1d6e0ab1380e3-0000.us-south.containers.appdomain.cloud");
        props.put(SchemaRegistryConfig.PROPERTY_API_SKIP_SSL_VALIDATION, true);

        // Set the value serializer for produced messages to use the Event Streams serializer
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "com.ibm.eventstreams.serdes.EventStreamsSerializer");

        // Set the encoding type used by the message serializer
        props.put(SchemaRegistryConfig.PROPERTY_ENCODING_TYPE, SchemaRegistryConfig.ENCODING_BINARY);

        return props;
    }

    private SchemaInfo getSchemaInfo(String schemaName, String schemaVersion) {
        SchemaInfo schema = null;
        try {
            schema = this.schemaRegistry.getSchema(schemaName, schemaVersion);
        } catch (SchemaRegistryAuthException | SchemaRegistryConnectionException | SchemaRegistryServerErrorException | SchemaRegistryApiException e) {
            logger.info("WE GOT HERE!!!!NOOO" + e.toString());
            throw new Error(e);
            // TODO handle this
        }

        return schema;
    }

    public Schema getSchema(Headers headers) {

        Iterator<Header> headerIterator = headers.iterator();
        StringDeserializer stringDeserializer = new StringDeserializer();

        String schemaName = null;
        String schemaVersion = null;

        while(headerIterator.hasNext()) {
            Header header = headerIterator.next();

            if(header.key().equals(SchemaRegistryConfig.HEADER_MSG_SCHEMA_ID)) {
                schemaName = stringDeserializer.deserialize(null, header.value());
            } else if(header.key().equals(SchemaRegistryConfig.HEADER_MSG_SCHEMA_VERSION)) {
                schemaVersion = stringDeserializer.deserialize(null, header.value());
            }

            if (schemaName != null && schemaVersion != null) {
                break;
            }
        }

        logger.info("GETTING SCHEMA!!!");
        logger.info(schemaName + " " + schemaVersion);

        return this.getSchemaInfo(schemaName, schemaVersion)
                .getSchema();
    }

    public static void main(String[] args) {
        IBMSchemaRegistry reg = new IBMSchemaRegistry();
        SchemaInfo schema = reg.getSchemaInfo("simpleSchemaTest", "1.0.0");

        System.out.println(schema.toString());
    }
}
