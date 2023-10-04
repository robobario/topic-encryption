package io.strimzi.kafka.topicenc.policy;

import org.apache.kafka.common.record.Record;

public interface KeyReferenceFunction {
    String getKeyReference(Record record);
}
