/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.topicenc.policy;

import io.strimzi.kafka.topicenc.kms.KeyMgtSystem;
import io.strimzi.kafka.topicenc.kms.KmsDefinition;
import io.strimzi.kafka.topicenc.kms.KmsException;
import io.strimzi.kafka.topicenc.kms.KmsFactory;
import io.strimzi.kafka.topicenc.kms.KmsFactoryManager;

/**
 * An trivial implementation of a policy repository used only for testing. All
 * topics will be encrypted with the key from TestKms.
 */
public class TestPolicyRepository implements PolicyRepository {

    TopicPolicy policy;
    KmsDefinition kmsDef;

    /**
     * Initializes the test repository with a single policy for all topics. The
     * encryption key is hard coded in the TestKms. This is to be used for testing
     * only.
     * 
     * @throws KmsException
     */
    public TestPolicyRepository() throws KmsException {

        // create the test KMS:
        KmsDefinition kmsDef = new KmsDefinition()
                .setName("test")
                .setType("test");

        KeyMgtSystem kms = KmsFactoryManager.getInstance().createKms(kmsDef);

        // create the single test policy for all topics:
        policy = new TopicPolicy()
                .setEncMethod("AesGcmV1")
                .setKeyReference("test")
                .setKeyReferenceFunction(new FixedKeyReferenceFunction("test"))
                .setTopic(TopicPolicy.ALL_TOPICS)
                .setKms(kms);
    }

    @Override
    public TopicPolicy getTopicPolicy(String topicName) {
        return policy;
    }
}
