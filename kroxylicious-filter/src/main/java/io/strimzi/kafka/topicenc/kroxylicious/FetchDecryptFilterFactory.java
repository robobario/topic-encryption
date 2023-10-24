package io.strimzi.kafka.topicenc.kroxylicious;

import io.kroxylicious.proxy.filter.FilterCreationContext;
import io.kroxylicious.proxy.filter.FilterFactory;

public class FetchDecryptFilterFactory implements FilterFactory<FetchDecryptFilter, Void> {

    @Override
    public FetchDecryptFilter createFilter(FilterCreationContext context, Void configuration) {
        return new FetchDecryptFilter();
    }

    @Override
    public Class<FetchDecryptFilter> filterType() {
        return FetchDecryptFilter.class;
    }

    @Override
    public Class<Void> configType() {
        return Void.class;
    }


}
