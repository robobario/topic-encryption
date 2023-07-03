package io.strimzi.kafka.topicenc.kroxylicious.kms;

import io.strimzi.kafka.topicenc.kms.KeyMgtSystem;
import io.strimzi.kafka.topicenc.kms.KmsDefinition;
import io.strimzi.kafka.topicenc.kms.KmsFactory;

public class MockKmsFactory implements KmsFactory {

    public static final String MOCK_KMS_NAME = "mock-kms";

    @Override
    public String getName() {
        return MOCK_KMS_NAME;
    }

    @Override
    public KeyMgtSystem createKms(KmsDefinition kmsDef) {
        return new MockKms(kmsDef.getCredential());
    }
}
