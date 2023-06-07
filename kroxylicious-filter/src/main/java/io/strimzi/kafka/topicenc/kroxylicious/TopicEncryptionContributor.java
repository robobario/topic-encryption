package io.strimzi.kafka.topicenc.kroxylicious;

import io.kroxylicious.proxy.filter.FilterContributor;
import io.kroxylicious.proxy.filter.KrpcFilter;
import io.kroxylicious.proxy.service.BaseContributor;

public class TopicEncryptionContributor extends BaseContributor<KrpcFilter> implements FilterContributor {

    public static final String TOPIC_ENCRYPTION_SHORTNAME = "TopicEncryption";
    public static final BaseContributorBuilder<KrpcFilter> FILTERS = BaseContributor.<KrpcFilter>builder()
            .add(TOPIC_ENCRYPTION_SHORTNAME, TopicEncryptionFilter::new);

    public TopicEncryptionContributor() {
        super(FILTERS);
    }

}
