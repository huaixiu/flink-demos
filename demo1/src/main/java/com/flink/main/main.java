package com.flink.main;

import com.flink.common.MetricEvent;
import com.flink.common.ExecutionEnvUtil;
import com.flink.common.GsonUtil;
import com.flink.common.KafkaConfigUtil;
import com.flink.connectors.ElasticSearchSinkUtil;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.http.HttpHost;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.List;

public class main {
    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        DataStreamSource<MetricEvent> data = KafkaConfigUtil.buildSource(env);

        List<HttpHost> esAddresses = ElasticSearchSinkUtil.getEsAddresses(parameterTool.get(ELASTICSEARCH_HOSTS));
        int bulkSize = parameterTool.getInt(ELASTICSEARCH_BULK_FLUSH_MAX_ACTIONS, 40);
        int sinkParallelism = parameterTool.getInt(STREAM_SINK_PARALLELISM, 5);

        //log.info("-----esAddresses = {}, parameterTool = {}, ", esAddresses, parameterTool);

        ElasticSearchSinkUtil.addSink(esAddresses, bulkSize, sinkParallelism, data,
                (MetricEvent metric, RuntimeContext runtimeContext, RequestIndexer requestIndexer) -> {
                    requestIndexer.add(Requests.indexRequest()
                            .index(ZHISHENG + "_" + metric.getName())
                            .type(ZHISHENG)
                            .source(GsonUtil.toJSONBytes(metric), XContentType.JSON));
                },
                parameterTool);
        env.execute("flink learning connectors es6");
    }
}
