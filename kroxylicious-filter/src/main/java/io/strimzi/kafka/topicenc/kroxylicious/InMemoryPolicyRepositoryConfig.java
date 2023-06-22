package io.strimzi.kafka.topicenc.kroxylicious;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.kroxylicious.proxy.config.BaseConfig;
import io.strimzi.kafka.topicenc.policy.InMemoryPolicyRepository;
import io.strimzi.kafka.topicenc.policy.JsonPolicyLoader;
import io.strimzi.kafka.topicenc.policy.PolicyRepository;
import io.strimzi.kafka.topicenc.policy.TopicPolicy;

import java.io.File;
import java.util.List;

public class InMemoryPolicyRepositoryConfig extends BaseConfig {

    public static final String KMS_DEFINITIONS_FILE_PROP_NAME = "kmsDefinitionsFile";
    public static final String TOPIC_POLICIES_FILE_PROP_NAME = "topicPoliciesFile";
    private final PolicyRepository policyRepository;

    @JsonCreator
    public InMemoryPolicyRepositoryConfig(@JsonProperty(KMS_DEFINITIONS_FILE_PROP_NAME) String kmsDefinitionsFile, @JsonProperty(TOPIC_POLICIES_FILE_PROP_NAME) String topicPoliciesFile) {
        File kmsDefsFile = new File(kmsDefinitionsFile);
        if (!kmsDefsFile.exists()) {
            throw new IllegalArgumentException(KMS_DEFINITIONS_FILE_PROP_NAME + " " + kmsDefinitionsFile + " does not exist");
        }
        File policyFile = new File(topicPoliciesFile);
        if (!policyFile.exists()) {
            throw new IllegalArgumentException(TOPIC_POLICIES_FILE_PROP_NAME + " " + policyFile + " does not exist");
        }
        try {
            List<TopicPolicy> topicPolicies = JsonPolicyLoader.loadTopicPolicies(kmsDefsFile, policyFile);
            policyRepository = new InMemoryPolicyRepository(topicPolicies);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create topic policies", e);
        }
    }

    public PolicyRepository getPolicyRepository() {
        return policyRepository;
    }
}
