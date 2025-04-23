package io.confluent.cse;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.opentelemetry.proto.metrics.v1.MetricsData;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.telemetry.ClientTelemetry;
import org.apache.kafka.server.telemetry.ClientTelemetryPayload;
import org.apache.kafka.server.telemetry.ClientTelemetryReceiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Map;

public class KafkaClientTelemetry implements ClientTelemetry, MetricsReporter, ClientTelemetryReceiver {

    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private static final String DEFAULT_METRICS_TOPIC = "default-kafka-metrics";
    private static final String DEFAULT_BROKER_ADDRESS = "localhost:9092";
    private static final String METRICS_TOPIC_ENV = "KAFKA_METRICS_TOPIC";
    private static final String BROKER_ADDRESS_ENV = "KAFKA_BROKER_ADDRESS";

    private final KafkaMetricsProducer metricsProducer;
    private final String metricsTopic;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public KafkaClientTelemetry() {
        this.metricsTopic = System.getenv().getOrDefault(METRICS_TOPIC_ENV, DEFAULT_METRICS_TOPIC);
        String brokerAddress = System.getenv().getOrDefault(BROKER_ADDRESS_ENV, DEFAULT_BROKER_ADDRESS);
        LOG.info("Initializing KafkaClientTelemetry with topic: {} and broker: {}", metricsTopic, brokerAddress);
        this.metricsProducer = KafkaMetricsProducer.getInstance(brokerAddress, metricsTopic);
    }

    @Override
    public void init(List<KafkaMetric> metrics) {
        LOG.info("*** KafkaClientTelemetry :: init() ***");
        for (KafkaMetric m : metrics){
            LOG.info("Metric Name: "+m.metricName()+" - value: -  "+m.metricValue().toString());
        }
    }

    @Override
    public void metricChange(KafkaMetric kafkaMetric) {
        LOG.info("**** metricChange detected ****");
        LOG.info("** metricChange - name: "+kafkaMetric.metricName() + " - value: - " + kafkaMetric.metricValue().toString());
    }

    @Override
    public void metricRemoval(KafkaMetric metric) {
        LOG.info("**** metricRemoval detected ****");
    }

    @Override
    public void close() {
        LOG.info("******************** CLOSE *************************");
        if (metricsProducer != null) {
            metricsProducer.close();
        }
    }

    @Override
    public void configure(Map<String, ?> configs) {
        LOG.info("*** KafkaClientTelemetry :: configure() ***");
        LOG.info("Configuration: {}", configs);
    }

    @Override
    public ClientTelemetryReceiver clientReceiver() {
        return this;
    }

    @Override
    public void exportMetrics(AuthorizableRequestContext context, ClientTelemetryPayload payload) {
        LOG.info("***** KafkaClientTelemetry :: exportMetrics *****");
        try {
            LOG.info("*** Context *** : clientId="+context.clientId()+", requestType=" +context.requestType()+", clientAddress=" +context.clientAddress()+", listenerName=" +context.listenerName());
            LOG.info("*** Payload *** : "+payload.data().toString());
            MetricsData data = MetricsData.parseFrom(payload.data());
            List<ResourceMetrics> resourceMetricsList = data.getResourceMetricsList();
            
            for (ResourceMetrics r : resourceMetricsList){
                LOG.info(" > "+r.getScopeMetricsList().get(0).getMetrics(0).getName() + " | " + r.getScopeMetricsList().get(0).getMetrics(0).getUnit() + " | "+ r.getScopeMetricsList().get(0).getMetrics(0).toString());
                String metricName = r.getScopeMetricsList().get(0).getMetrics(0).getName();
                String metricValue = r.getScopeMetricsList().get(0).getMetrics(0).toString();
                LOG.info("Metric Name: " + metricName + " - value: - " + metricValue);
                
                // Create JSON object for the metric
                ObjectNode metricJson = objectMapper.createObjectNode();
                metricJson.put("name", metricName);
                metricJson.put("value", metricValue);
                metricJson.put("unit", r.getScopeMetricsList().get(0).getMetrics(0).getUnit());
                metricJson.put("timestamp", System.currentTimeMillis());
                
                // Add context information
                ObjectNode contextJson = objectMapper.createObjectNode();
                contextJson.put("clientId", context.clientId());
                contextJson.put("requestType", context.requestType());
                contextJson.put("clientAddress", context.clientAddress().getHostAddress());
                contextJson.put("listenerName", context.listenerName());
                metricJson.set("context", contextJson);
                
                // Convert to JSON string and send
                String jsonMetric = objectMapper.writeValueAsString(metricJson);
                metricsProducer.sendMetric(metricName, jsonMetric);
            }
        } catch (Exception e) {
            LOG.info("+++ CLIENT TELEMETRY: clientInstanceId=" + payload.clientInstanceId()
                    + ", isTerminating=" + payload.isTerminating()
                    + ", contentType=" + payload.contentType()
                    + ", exception=" + e);
        }
    }
}