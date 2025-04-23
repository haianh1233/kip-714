package io.confluent.cse;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.opentelemetry.proto.metrics.v1.MetricsData;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.Sum;
import io.opentelemetry.proto.metrics.v1.Gauge;
import io.opentelemetry.proto.metrics.v1.Histogram;
import io.opentelemetry.proto.metrics.v1.ExponentialHistogram;
import io.opentelemetry.proto.metrics.v1.Summary;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.HistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.ExponentialHistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.SummaryDataPoint;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.telemetry.ClientTelemetry;
import org.apache.kafka.server.telemetry.ClientTelemetryPayload;
import org.apache.kafka.server.telemetry.ClientTelemetryReceiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.*;

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
            
            // Create the main event data structure
            Map<String, Object> eventData = new LinkedHashMap<>();
            eventData.put("clientInstanceId", payload.clientInstanceId().toString());
            eventData.put("isTerminating", payload.isTerminating());
            eventData.put("contentType", payload.contentType());
            
            // Add context information
            Map<String, Object> contextInfo = new LinkedHashMap<>();
            contextInfo.put("clientId", context.clientId());
            contextInfo.put("requestType", context.requestType());
            contextInfo.put("clientAddress", context.clientAddress().getHostAddress());
            contextInfo.put("listenerName", context.listenerName());
            eventData.put("context", contextInfo);

            // Parse and add metrics data
            MetricsData data = MetricsData.parseFrom(payload.data());
            Map<String, Object> metrics = new LinkedHashMap<>();
            
            for (ResourceMetrics resourceMetrics : data.getResourceMetricsList()) {
                for (ScopeMetrics scopeMetrics : resourceMetrics.getScopeMetricsList()) {
                    for (Metric metric : scopeMetrics.getMetricsList()) {
                        Map<String, Object> metricMap = new LinkedHashMap<>();
                        metricMap.put("name", metric.getName());
                        metricMap.put("description", metric.getDescription());
                        metricMap.put("unit", metric.getUnit());

                        switch (metric.getDataCase()) {
                            case SUM: {
                                Sum sum = metric.getSum();
                                metricMap.put("aggregationTemporality", sum.getAggregationTemporality().name());
                                List<Map<String, Object>> dataPoints = new ArrayList<>();
                                for (NumberDataPoint dp : sum.getDataPointsList()) {
                                    Map<String, Object> dpMap = new LinkedHashMap<>();
                                    if (dp.hasAsDouble()) dpMap.put("asDouble", dp.getAsDouble());
                                    if (dp.hasAsInt()) dpMap.put("asInt", dp.getAsInt());
                                    dpMap.put("timeUnixNano", dp.getTimeUnixNano());
                                    dpMap.put("startTimeUnixNano", dp.getStartTimeUnixNano());
                                    List<Map<String, Object>> attributes = new ArrayList<>();
                                    dp.getAttributesList().forEach(keyValue -> {
                                        Map<String, Object> attribute = new LinkedHashMap<>();
                                        attribute.put("key", keyValue.getKey());
                                        attribute.put("value", keyValue.getValue().getStringValue());
                                        attributes.add(attribute);
                                    });
                                    dpMap.put("attributes", attributes);
                                    dataPoints.add(dpMap);
                                }
                                metricMap.put("dataPoints", dataPoints);
                                break;
                            }
                            case GAUGE: {
                                Gauge gauge = metric.getGauge();
                                List<Map<String, Object>> dataPoints = new ArrayList<>();
                                for (NumberDataPoint dp : gauge.getDataPointsList()) {
                                    Map<String, Object> dpMap = new LinkedHashMap<>();
                                    if (dp.hasAsDouble()) dpMap.put("asDouble", dp.getAsDouble());
                                    if (dp.hasAsInt()) dpMap.put("asInt", dp.getAsInt());
                                    dpMap.put("timeUnixNano", dp.getTimeUnixNano());
                                    dpMap.put("startTimeUnixNano", dp.getStartTimeUnixNano());
                                    List<Map<String, Object>> attributes = new ArrayList<>();
                                    dp.getAttributesList().forEach(keyValue -> {
                                        Map<String, Object> attribute = new LinkedHashMap<>();
                                        attribute.put("key", keyValue.getKey());
                                        attribute.put("value", keyValue.getValue().getStringValue());
                                        attributes.add(attribute);
                                    });
                                    dpMap.put("attributes", attributes);
                                    dataPoints.add(dpMap);
                                }
                                metricMap.put("dataPoints", dataPoints);
                                break;
                            }
                            case HISTOGRAM: {
                                Histogram histogram = metric.getHistogram();
                                metricMap.put("aggregationTemporality", histogram.getAggregationTemporality().name());
                                List<Map<String, Object>> dataPoints = new ArrayList<>();
                                for (HistogramDataPoint dp : histogram.getDataPointsList()) {
                                    Map<String, Object> dpMap = new LinkedHashMap<>();
                                    dpMap.put("count", dp.getCount());
                                    dpMap.put("sum", dp.getSum());
                                    dpMap.put("bucketCounts", dp.getBucketCountsList());
                                    dpMap.put("explicitBounds", dp.getExplicitBoundsList());
                                    dpMap.put("timeUnixNano", dp.getTimeUnixNano());
                                    dpMap.put("startTimeUnixNano", dp.getStartTimeUnixNano());
                                    List<Map<String, Object>> attributes = new ArrayList<>();
                                    dp.getAttributesList().forEach(keyValue -> {
                                        Map<String, Object> attribute = new LinkedHashMap<>();
                                        attribute.put("key", keyValue.getKey());
                                        attribute.put("value", keyValue.getValue().getStringValue());
                                        attributes.add(attribute);
                                    });
                                    dpMap.put("attributes", attributes);
                                    dataPoints.add(dpMap);
                                }
                                metricMap.put("dataPoints", dataPoints);
                                break;
                            }
                            case EXPONENTIAL_HISTOGRAM: {
                                ExponentialHistogram eh = metric.getExponentialHistogram();
                                metricMap.put("aggregationTemporality", eh.getAggregationTemporality().name());
                                List<Map<String, Object>> dataPoints = new ArrayList<>();
                                for (ExponentialHistogramDataPoint dp : eh.getDataPointsList()) {
                                    Map<String, Object> dpMap = new LinkedHashMap<>();
                                    dpMap.put("count", dp.getCount());
                                    dpMap.put("sum", dp.getSum());
                                    dpMap.put("scale", dp.getScale());
                                    dpMap.put("zeroCount", dp.getZeroCount());
                                    dpMap.put("timeUnixNano", dp.getTimeUnixNano());
                                    dpMap.put("startTimeUnixNano", dp.getStartTimeUnixNano());
                                    List<Map<String, Object>> attributes = new ArrayList<>();
                                    dp.getAttributesList().forEach(keyValue -> {
                                        Map<String, Object> attribute = new LinkedHashMap<>();
                                        attribute.put("key", keyValue.getKey());
                                        attribute.put("value", keyValue.getValue().getStringValue());
                                        attributes.add(attribute);
                                    });
                                    dpMap.put("attributes", attributes);
                                    dataPoints.add(dpMap);
                                }
                                metricMap.put("dataPoints", dataPoints);
                                break;
                            }
                            case SUMMARY: {
                                Summary summary = metric.getSummary();
                                List<Map<String, Object>> dataPoints = new ArrayList<>();
                                for (SummaryDataPoint dp : summary.getDataPointsList()) {
                                    Map<String, Object> dpMap = new LinkedHashMap<>();
                                    dpMap.put("count", dp.getCount());
                                    dpMap.put("sum", dp.getSum());
                                    List<Map<String, Object>> quantiles = new ArrayList<>();
                                    for (var q : dp.getQuantileValuesList()) {
                                        quantiles.add(Map.of(
                                            "quantile", q.getQuantile(),
                                            "value", q.getValue()
                                        ));
                                    }
                                    dpMap.put("quantileValues", quantiles);
                                    dpMap.put("timeUnixNano", dp.getTimeUnixNano());
                                    dpMap.put("startTimeUnixNano", dp.getStartTimeUnixNano());
                                    List<Map<String, Object>> attributes = new ArrayList<>();
                                    dp.getAttributesList().forEach(keyValue -> {
                                        Map<String, Object> attribute = new LinkedHashMap<>();
                                        attribute.put("key", keyValue.getKey());
                                        attribute.put("value", keyValue.getValue().getStringValue());
                                        attributes.add(attribute);
                                    });
                                    dpMap.put("attributes", attributes);
                                    dataPoints.add(dpMap);
                                }
                                metricMap.put("dataPoints", dataPoints);
                                break;
                            }
                            case DATA_NOT_SET:
                                metricMap.put("data", "unknown");
                                break;
                        }
                        metrics.put(metric.getName(), metricMap);
                    }
                }
            }
            eventData.put("metrics", metrics);

            // Convert to JSON and send
            String jsonMetric = objectMapper.writeValueAsString(eventData);
            metricsProducer.sendMetric("client_telemetry", jsonMetric);
            
        } catch (Exception e) {
            LOG.info("+++ CLIENT TELEMETRY: clientInstanceId=" + payload.clientInstanceId()
                    + ", isTerminating=" + payload.isTerminating()
                    + ", contentType=" + payload.contentType()
                    + ", exception=" + e);
        }
    }
}