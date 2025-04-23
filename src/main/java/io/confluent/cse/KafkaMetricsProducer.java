package io.confluent.cse;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Properties;

public class KafkaMetricsProducer {
    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private static KafkaMetricsProducer instance;
    private final Producer<String, String> producer;
    private final String metricsTopic;

    private KafkaMetricsProducer(String bootstrapServers, String metricsTopic) {
        this.metricsTopic = metricsTopic;
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ENABLE_METRICS_PUSH_CONFIG, true);
        
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384); 
        props.put(ProducerConfig.LINGER_MS_CONFIG, 100); 
        
        this.producer = new KafkaProducer<>(props);
    }

    public static synchronized KafkaMetricsProducer getInstance(String bootstrapServers, String metricsTopic) {
        if (instance == null) {
            instance = new KafkaMetricsProducer(bootstrapServers, metricsTopic);
        }
        return instance;
    }

    public void sendMetric(String metricName, String metricValue) {
        try {
            ProducerRecord<String, String> record = new ProducerRecord<>(
                metricsTopic,
                metricName,
                metricValue
            );
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    LOG.error("Error sending metric: " + metricName, exception);
                } else {
                    LOG.debug("Sent metric: {} to topic: {}", metricName, metadata.topic());
                }
            });
        } catch (Exception e) {
            LOG.error("Error sending metric: " + metricName, e);
        }
    }

    public void close() {
        if (producer != null) {
            producer.flush();
            producer.close();
        }
    }
} 