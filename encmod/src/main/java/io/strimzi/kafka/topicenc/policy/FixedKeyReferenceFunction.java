package io.strimzi.kafka.topicenc.policy;

import org.apache.kafka.common.record.Record;

import java.util.Objects;

public class FixedKeyReferenceFunction implements KeyReferenceFunction {

    private final String keyReference;

    public FixedKeyReferenceFunction(String keyReference) {
        Objects.requireNonNull(keyReference);
        if (keyReference.isBlank()) {
            throw new IllegalArgumentException("keyReference is blank");
        }
        this.keyReference = keyReference;
    }

    @Override
    public String getKeyReference(Record record) {
        return keyReference;
    }
}
