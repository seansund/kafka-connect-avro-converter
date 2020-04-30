package com.ibm.eventstreams.connect.avroconverter.schemaregistry.exceptions;

public class SchemaRegistryInitException extends Exception {
    public SchemaRegistryInitException(Exception e) {
        super(e);
    }
}
