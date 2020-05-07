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

package com.ibm.eventstreams.connect.avroconverter.schemaregistry;

import com.ibm.eventstreams.connect.avroconverter.schemaregistry.exceptions.IBMSchemaRegistryConfig;
import com.ibm.eventstreams.connect.avroconverter.schemaregistry.exceptions.SchemaRegistryInitException;
import com.ibm.eventstreams.serdes.SchemaInfo;
import com.ibm.eventstreams.serdes.SchemaRegistry;
import com.ibm.eventstreams.serdes.SchemaRegistryConfig;
import com.ibm.eventstreams.serdes.exceptions.SchemaRegistryApiException;
import com.ibm.eventstreams.serdes.exceptions.SchemaRegistryAuthException;
import com.ibm.eventstreams.serdes.exceptions.SchemaRegistryConnectionException;
import com.ibm.eventstreams.serdes.exceptions.SchemaRegistryServerErrorException;
import org.apache.avro.Schema;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

public class IBMSchemaRegistry {

    private static final Logger logger = LoggerFactory.getLogger(IBMSchemaRegistry.class);

    private SchemaRegistry schemaRegistry;

    public IBMSchemaRegistry(Map<String, ?> configs) throws SchemaRegistryInitException {
        try {
            Properties props = IBMSchemaRegistryConfig.toProps(configs);
            logger.info("------ Printing Properties --------");
            logger.info(props.toString());
            this.schemaRegistry = new SchemaRegistry(props);
        } catch (KeyManagementException | NoSuchAlgorithmException | SchemaRegistryAuthException | SchemaRegistryServerErrorException | SchemaRegistryApiException | SchemaRegistryConnectionException e) {
            throw new SchemaRegistryInitException(e);
        }
    }

    private SchemaInfo getSchemaInfo(String schemaName, String schemaVersion) {
        SchemaInfo schema;
        try {
            schema = this.schemaRegistry.getSchema(schemaName, schemaVersion);
        } catch (SchemaRegistryAuthException | SchemaRegistryConnectionException | SchemaRegistryServerErrorException | SchemaRegistryApiException e) {
            throw new Error(e);
            // TODO handle this, consider retrying on failure up to X times (configurable)
        }

        return schema;
    }

    public Schema getSchema(Headers headers) {

        if(headers == null) {
            return null;
        }

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

        logger.info("-- Extracted Schema Header Values --");
        logger.info(schemaName + " " + schemaVersion);

        return this.getSchemaInfo(schemaName, schemaVersion)
                .getSchema();
    }
}
