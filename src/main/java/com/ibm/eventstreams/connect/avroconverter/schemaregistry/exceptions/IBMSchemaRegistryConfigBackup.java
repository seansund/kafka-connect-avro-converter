package com.ibm.eventstreams.connect.avroconverter.schemaregistry.exceptions;

import com.ibm.eventstreams.serdes.SchemaRegistryConfig;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;

import java.util.Map;
import java.util.Properties;

// TODO add java docs
// TODO refactor out schema.registry prefix
public class IBMSchemaRegistryConfigBackup {

    /**
     *
     */
    private static final String BOOTSTRAP_SERVERS_CONFIG = "schema.registry.bootstrap.server";

    /**
     *
     */
    private static final String SECURITY_PROTOCOL_CONFIG = "schema.registry.security.protocol";

    /**
     *
     */
    private static final String SSL_PROTOCOL_CONFIG = "schema.registry.ssl.protocol";

    /**
     *
     */
    private static final String SSL_TRUSTSTORE_LOCATION_CONFIG = "schema.registry.ssl.truststore.location";

    /**
     *
     */
    private static final String SSL_TRUSTSTORE_PASSWORD_CONFIG = "schema.registry.ssl.truststore.password";

    /**
     *
     */
    private static final String SASL_JAAS_USERNAME = "schema.registry.sasl.jaas.username";

    /**
     *
     */
    private static final String SASL_JAAS_PASSWORD = "schema.registry.sasl.jaas.password";

    /**
     *
     */
    private static final String PROPERTY_API_URL = "schema.registry.property.api.url";

    /**
     *
     */
    private static final String PROPERTY_API_SKIP_SSL_VALIDATION = "schema.registry.property.api.skip_ssl_validation";


    // TODO move this to a Config class
    public static Properties toProps(Map<String, ?> configs) {
        Properties props = new Properties();

        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, configs.get(BOOTSTRAP_SERVERS_CONFIG));
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, configs.get(SECURITY_PROTOCOL_CONFIG));

        props.put(SslConfigs.SSL_PROTOCOL_CONFIG, configs.get(SSL_PROTOCOL_CONFIG));
        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, configs.get(SSL_TRUSTSTORE_LOCATION_CONFIG));
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, configs.get(SSL_TRUSTSTORE_PASSWORD_CONFIG));

        // TODO dynamically construct based SASL Configurations
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        String saslJaasConfig = String.format("org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
                configs.get(SASL_JAAS_USERNAME),
                configs.get(SASL_JAAS_PASSWORD)
        );
        props.put(SaslConfigs.SASL_JAAS_CONFIG, saslJaasConfig);

        props.put(SchemaRegistryConfig.PROPERTY_API_URL, configs.get(PROPERTY_API_URL));
        props.put(SchemaRegistryConfig.PROPERTY_API_SKIP_SSL_VALIDATION, configs.get(PROPERTY_API_SKIP_SSL_VALIDATION));

        // Set the value serializer for produced messages to use the Event Streams serializer
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "com.ibm.eventstreams.serdes.EventStreamsSerializer");

        // Set the encoding type used by the message serializer
        // TODO create dynamic mapping to determine encoding type
        props.put(SchemaRegistryConfig.PROPERTY_ENCODING_TYPE, SchemaRegistryConfig.ENCODING_BINARY);

        return props;
    }
}
