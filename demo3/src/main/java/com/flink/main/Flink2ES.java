package com.flink.main;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.flink.domain.Customer;
import com.flink.source.StreamSource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.client.Requests;
import java.util.ArrayList;
import java.util.List;

public class Flink2ES {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);


        DataStream<Customer> source = env.addSource(new StreamSource());


        DataStream<Customer> filterSource = source.filter(new FilterFunction<Customer>() {
            @Override
            public boolean filter(Customer customer) throws Exception {
                if (customer.getName().equals("曹操") && customer.getAddress().getProvince().equals("湖北省")) {
                    return false;
                }
                return true;
            }
        });


        DataStream<JSONObject> transSource = filterSource.map(new MapFunction<Customer, JSONObject>() {
            @Override
            public JSONObject map(Customer customer) throws Exception {
                String jsonString = JSONObject.toJSONString(customer, SerializerFeature.WriteDateUseDateFormat);
                System.out.println("当前正在处理:" + jsonString);
                JSONObject jsonObject = JSONObject.parseObject(jsonString);
                return jsonObject;
            }
        });

        /**
         * 创建一个ElasticSearchSink对象
         */
        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("10.180.171.226", 9200, "http"));
        ElasticsearchSink.Builder<JSONObject> esSinkBuilder = new ElasticsearchSink.Builder<JSONObject>(
                httpHosts,
                new ElasticsearchSinkFunction<JSONObject>() {
                    @Override
                    public void process(JSONObject customer, RuntimeContext ctx, RequestIndexer indexer) {
                        // 数据保存在Elasticsearch中名称为index_customer的索引中，保存的类型名称为type_customer
                        indexer.add(Requests.indexRequest().index("index_customer").type("type_customer").id(String.valueOf(customer.getLong("id"))).source(customer));
                    }
                }
        );
        // 设置批量写数据的缓冲区大小
        esSinkBuilder.setBulkFlushMaxActions(50);


        transSource.addSink(esSinkBuilder.build());


        env.execute("execute FlinkElasticsearchDemo");
    }
}
