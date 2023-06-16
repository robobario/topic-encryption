package io.strimzi.kafka.topicenc.kroxylicious;

import io.kroxylicious.proxy.filter.FetchRequestFilter;
import io.kroxylicious.proxy.filter.FetchResponseFilter;
import io.kroxylicious.proxy.filter.KrpcFilterContext;
import io.kroxylicious.proxy.filter.MetadataResponseFilter;
import io.strimzi.kafka.topicenc.EncryptionModule;
import io.strimzi.kafka.topicenc.kms.KmsDefinition;
import io.strimzi.kafka.topicenc.kms.test.TestKms;
import io.strimzi.kafka.topicenc.policy.InMemoryPolicyRepository;
import io.strimzi.kafka.topicenc.policy.TopicPolicy;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.MetadataResponseDataJsonConverter;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.Errors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class FetchDecryptFilter implements FetchRequestFilter, FetchResponseFilter, MetadataResponseFilter {

    private static final Logger log = LoggerFactory.getLogger(FetchDecryptFilter.class);
    public static final short METADATA_VERSION_SUPPORTING_TOPIC_IDS = (short) 10;
    private final Map<Uuid, String> topicUuidToName = new HashMap<>();

    private final EncryptionModule module = new EncryptionModule(new InMemoryPolicyRepository(List.of(new TopicPolicy().setTopic(TopicPolicy.ALL_TOPICS).setKms(new TestKms(new KmsDefinition())))));

    @Override
    public void onFetchRequest(short apiVersion, RequestHeaderData header, FetchRequestData request, KrpcFilterContext context) {
        boolean allTopicsResolvableToName = request.topics().stream().allMatch(this::isResolvable);
        if (!allTopicsResolvableToName) {
            Stream<Uuid> topicIdsToResolve = request.topics().stream().filter(Predicate.not(this::isResolvable)).map(FetchRequestData.FetchTopic::topicId);
            // fire-and-forget a metadata request to resolve topic ids, preparing for fetch response
            resolveAndCache(context, topicIdsToResolve);
        }
        context.forwardRequest(header, request);
    }

    @Override
    public void onFetchResponse(short apiVersion, ResponseHeaderData header, FetchResponseData response, KrpcFilterContext context) {
        boolean allTopicsResolvableToName = response.responses().stream().allMatch(this::isResolvable);
        if (allTopicsResolvableToName) {
            decryptFetchResponse(header, response, context);
        } else {
            log.warn("We did not know all topic names for topic ids within a fetch response, requesting metadata and returning error response");
            resolveTopicsAndReturnError(header, response, context);
        }
    }

    /**
     * We should know the topic names by the time we get the response, because the fetch request sends a metadata request
     * for unknown topic ids before sending the fetch request. This is a safeguard in case that request fails somehow.
     */
    private void resolveTopicsAndReturnError(ResponseHeaderData header, FetchResponseData response, KrpcFilterContext context) {
        Stream<Uuid> topicIdsToResolve = response.responses().stream().filter(Predicate.not(this::isResolvable)).map(FetchResponseData.FetchableTopicResponse::topicId);
        // fire-and-forget a metadata request to resolve topic ids, preparing for future fetches
        resolveAndCache(context, topicIdsToResolve);
        FetchResponseData data = new FetchResponseData();
        data.setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code());
        context.forwardResponse(header, data);
    }

    private void resolveAndCache(KrpcFilterContext context, Stream<Uuid> topicIdsToResolve) {
        MetadataRequestData request = new MetadataRequestData();
        topicIdsToResolve.forEach(uuid -> {
            MetadataRequestData.MetadataRequestTopic e = new MetadataRequestData.MetadataRequestTopic();
            e.setTopicId(uuid);
            request.topics().add(e);
        });
        // if the client is sending topic ids we will assume the broker can support at least the lowest metadata apiVersion
        // supporting topicIds
        CompletionStage<MetadataResponseData> stage = context.sendRequest(METADATA_VERSION_SUPPORTING_TOPIC_IDS, request);
        stage.thenAccept(response -> cacheTopicIdToName(response, METADATA_VERSION_SUPPORTING_TOPIC_IDS));
    }

    private void decryptFetchResponse(ResponseHeaderData header, FetchResponseData response, KrpcFilterContext context) {
        for (FetchResponseData.FetchableTopicResponse fetchResponse : response.responses()) {
            Uuid originalUuid = fetchResponse.topicId();
            String originalName = fetchResponse.topic();
            if (originalName == null || originalName.equals("")) {
                fetchResponse.setTopic(topicUuidToName.get(originalUuid));
                fetchResponse.setTopicId(null);
            }
            try {
                module.decrypt(fetchResponse);
            } catch (Exception e) {
                log.error("Failed to decrypt a fetchResponse for topic: " + fetchResponse.topic(), e);
                throw new RuntimeException(e);
            }
            fetchResponse.setTopic(originalName);
            fetchResponse.setTopicId(originalUuid);
        }
        context.forwardResponse(header, response);
    }


    private boolean isResolvable(FetchResponseData.FetchableTopicResponse fetchableTopicResponse) {
        return (fetchableTopicResponse.topic() != null && !fetchableTopicResponse.topic().equals("")) || topicUuidToName.containsKey(fetchableTopicResponse.topicId());
    }

    private boolean isResolvable(FetchRequestData.FetchTopic fetchTopic) {
        return (fetchTopic.topic() != null && !fetchTopic.topic().equals("")) || topicUuidToName.containsKey(fetchTopic.topicId());
    }

    @Override
    public void onMetadataResponse(short apiVersion, ResponseHeaderData header, MetadataResponseData response, KrpcFilterContext context) {
        cacheTopicIdToName(response, apiVersion);
        context.forwardResponse(header, response);
    }

    private void cacheTopicIdToName(MetadataResponseData response, short apiVersion) {
        if (log.isDebugEnabled()) {
            MetadataResponseDataJsonConverter.write(response, apiVersion);
        }
        response.topics().forEach(topic -> topicUuidToName.put(topic.topicId(), topic.name()));
    }
}
