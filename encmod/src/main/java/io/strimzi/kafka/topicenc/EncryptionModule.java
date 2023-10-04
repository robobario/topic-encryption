/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.topicenc;

import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.util.HashMap;
import java.util.Map;

import javax.crypto.SecretKey;

import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.FetchResponseData.FetchableTopicResponse;
import org.apache.kafka.common.message.ProduceRequestData.PartitionProduceData;
import org.apache.kafka.common.message.ProduceRequestData.TopicProduceData;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.strimzi.kafka.topicenc.enc.AesGcmEncrypter;
import io.strimzi.kafka.topicenc.enc.EncData;
import io.strimzi.kafka.topicenc.enc.EncrypterDecrypter;
import io.strimzi.kafka.topicenc.kms.KeyMgtSystem;
import io.strimzi.kafka.topicenc.kms.KmsException;
import io.strimzi.kafka.topicenc.policy.PolicyRepository;
import io.strimzi.kafka.topicenc.policy.TopicPolicy;
import io.strimzi.kafka.topicenc.ser.AesGcmV1SerDer;
import io.strimzi.kafka.topicenc.ser.EncSerDer;
import io.strimzi.kafka.topicenc.ser.EncSerDerException;

/**
 * This class is the main component encompassing the Kafka topic encryption
 * implementation.
 */
public class EncryptionModule implements EncModControl {

    private static final Logger LOGGER = LoggerFactory.getLogger(EncryptionModule.class);

    // lowercase-TopicName -> KeyReference -> EncrypterDecrypter
    private Map<String, Map<String, EncrypterDecrypter>> keyCache;
    private EncSerDer encSerDer;
    private PolicyRepository policyRepo;

    public EncryptionModule(PolicyRepository policyRepo) {
        this.policyRepo = policyRepo;
        keyCache = new HashMap<>();
        encSerDer = new AesGcmV1SerDer();
    }

    public boolean encrypt(TopicProduceData topicData)
            throws EncSerDerException, GeneralSecurityException, KmsException {
        String topicKey = topicData.name().toLowerCase();
        TopicPolicy topicPolicy = getTopicPolicy(topicKey);

        if (topicPolicy == null) {
            LOGGER.debug("No encryption - topic {} is not configured for encryption",
                    topicData.name());
            return false;
        }

        // If this far, the data should be encrypted.
        // Navigate into each record and encrypt.
        for (PartitionProduceData partitionData : topicData.partitionData()) {

            MemoryRecords recs = (MemoryRecords) partitionData.records();
            MemoryRecordsBuilder builder = createMemoryRecsBuilder(recs.buffer().capacity());
            for (org.apache.kafka.common.record.Record record : recs.records()) {
                if (record.hasValue()) {
                    String keyReference = topicPolicy.getKeyReferenceFunction().getKeyReference(record);
                    if (keyReference == null || keyReference.isEmpty()) {
                        throw new IllegalStateException("could not extract a keyReference from record");
                    }

                    EncrypterDecrypter encrypter = getOrCreateTopicCrypter(topicKey, keyReference, topicPolicy);
                    // encrypt record value:
                    byte[] plaintext = new byte[record.valueSize()];
                    record.value().get(plaintext);
                    EncData ciphertext = encrypter.encrypt(plaintext);

                    // serialize the ciphertext and metadata, add to the builder:
                    encSerDer.serialize(builder, record, ciphertext);
                }
            }
            // overwrite the partition's memoryrecords with the encrypted records:
            partitionData.setRecords(builder.build());
        }
        return true;
    }

    private EncrypterDecrypter getOrCreateTopicCrypter(String topicKey, String keyReference, TopicPolicy topicPolicy) {
        return keyCache.computeIfAbsent(topicKey, key -> new HashMap<>())
                .computeIfAbsent(keyReference, keyRef -> createTopicEncrypter(topicPolicy, keyRef));
    }

    public boolean decrypt(FetchableTopicResponse fetchRsp)
            throws EncSerDerException, GeneralSecurityException, KmsException {
        String topicKey = fetchRsp.topic().toLowerCase();
        TopicPolicy topicPolicy = getTopicPolicy(topicKey);

        if (topicPolicy == null) {
            LOGGER.debug("No encryption - topic {} is not configured for encryption",
                    fetchRsp.topic());
            return false;
        }

        // If this far, the data was encrypted.
        // Navigate into each record and decrypt.
        for (FetchResponseData.PartitionData partitionData : fetchRsp.partitions()) {

            if (LOGGER.isDebugEnabled()) {
                String msg = String.format(
                        "partition: %d, logStartOffset: %08X, lastStableOffset: %08X, "
                                + "partition leader epoch: %04X",
                        partitionData.partitionIndex(), partitionData.currentLeader().leaderEpoch(),
                        partitionData.logStartOffset(), partitionData.lastStableOffset());
                LOGGER.debug(msg);
            }

            MemoryRecords recs = (MemoryRecords) partitionData.records();

            long firstOffset = getFirstOffset(recs);
            MemoryRecordsBuilder builder = createMemoryRecsBuilder(recs.sizeInBytes(),
                    partitionData.currentLeader().leaderEpoch(), firstOffset);
            for (org.apache.kafka.common.record.Record record : recs.records()) {
                if (record.hasValue()) {
                    byte[] ciphertext = new byte[record.valueSize()];
                    record.value().get(ciphertext);

                    // serialize value into version, iv, ciphertext:
                    EncData md = encSerDer.deserialize(ciphertext);
                    String keyReference = topicPolicy.getKeyReferenceFunction().getKeyReference(record);
                    EncrypterDecrypter crypter = getOrCreateTopicCrypter(topicKey, keyReference, topicPolicy);

                    // decrypt, add to records builder:
                    byte[] plaintext = crypter.decrypt(md);

                    SimpleRecord newRec = new SimpleRecord(record.timestamp(), record.key(),
                            ByteBuffer.wrap(plaintext),
                            record.headers());
                    builder.append(newRec);
                }
            }
            // overwrite the partition's memoryrecords with the decrypted records:
            MemoryRecords newRecs = builder.build();
            partitionData.setRecords(newRecs);
        }
        return true;
    }

    /**
     * EncMod control interface. Empty, placeholder implementation for the time
     * being.
     */
    @Override
    public void purgeKey(String keyref) {
    }

    /**
     * Consults the policy db whether a topic is to be encrypted. If topic is not to
     * be encrypted, returns null.
     *
     * @throws Exception
     */
    protected EncrypterDecrypter createTopicEncrypter(TopicPolicy policy, String keyReference) {
        try {
            KeyMgtSystem kms = policy.getKms();
            SecretKey key = kms.getKey(keyReference);
            return new AesGcmEncrypter(key);
        } catch (Exception e) {
            throw new IllegalStateException("failed to create topic encrypter" + e);
        }
    }

    private TopicPolicy getTopicPolicy(String topicKey) {
        return policyRepo.getTopicPolicy(topicKey);
    }

    private long getFirstOffset(MemoryRecords recs) {
        for (org.apache.kafka.common.record.Record r : recs.records()) {
            if (r.hasValue()) {
                return r.offset();
            }
        }
        return 0;
    }

    private MemoryRecordsBuilder createMemoryRecsBuilder(int bufSize) {
        return createMemoryRecsBuilder(bufSize, RecordBatch.NO_PARTITION_LEADER_EPOCH);
    }

    private MemoryRecordsBuilder createMemoryRecsBuilder(int bufSize, int partitionEpoch) {
        return createMemoryRecsBuilder(bufSize, partitionEpoch, 0L);
    }

    private MemoryRecordsBuilder createMemoryRecsBuilder(int bufSize, int partitionEpoch,
                                                         long baseOffset) {
        ByteBuffer buffer = ByteBuffer.allocate(10); // will be expanded as needed
        return new MemoryRecordsBuilder(buffer, RecordBatch.CURRENT_MAGIC_VALUE,
                CompressionType.NONE,
                TimestampType.CREATE_TIME,
                baseOffset,
                RecordBatch.NO_TIMESTAMP, // log appendTime
                RecordBatch.NO_PRODUCER_ID,
                RecordBatch.NO_PRODUCER_EPOCH,
                0, // baseSequence. RecordBatch.NO_SEQUENCE
                false, // isTransactional
                false, // isBatch
                partitionEpoch, // RecordBatch.NO_PARTITION_LEADER_EPOCH,
                bufSize);
    }
}
