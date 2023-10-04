package io.strimzi.kafka.topicenc.policy;

import org.apache.kafka.common.record.Record;

import java.nio.charset.StandardCharsets;

public class RecordKeyKeyReferenceFunction implements KeyReferenceFunction {

    private static final RecordKeyKeyReferenceFunction INSTANCE = new RecordKeyKeyReferenceFunction();

    public static KeyReferenceFunction instance() {
        return INSTANCE;
    }

    private RecordKeyKeyReferenceFunction() {
    }

    @Override
    public String getKeyReference(Record record) {
        if (!record.hasKey()) {
            throw new IllegalStateException("record has no key, cannot obtain a key reference for decryption");
        }
        byte[] keyBytes = new byte[record.keySize()];
        record.key().get(keyBytes);
        // we could also support other record key formats like numeric bytes, or convert arbitrary bytes to base64
        return new String(keyBytes, StandardCharsets.UTF_8);
    }
}
