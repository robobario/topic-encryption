package io.strimzi.kafka.topicenc.kroxylicious;

public class Strings {
    public static boolean isNullOrBlank(String originalName) {
        return originalName == null || originalName.strip().equals("");
    }
}
