package com.atguigu.writer;

import com.alibaba.fastjson.JSONObject;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MaxAggregation;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class ESReader {

    public static void main(String[] args) throws IOException {

        //1.创建ES客户端
        JestClientFactory jestClientFactory = new JestClientFactory();
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://hadoop102:9200").build();
        jestClientFactory.setHttpClientConfig(httpClientConfig);
        JestClient jestClient = jestClientFactory.getObject();

        //创建查询语句的对象
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //Bool
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("sex", "male"));
        boolQueryBuilder.must(new MatchQueryBuilder("favo", "球"));
        searchSourceBuilder.query(boolQueryBuilder);
        //aggs
        TermsAggregationBuilder count_by_class1 = new TermsAggregationBuilder("count_by_class", ValueType.LONG);
        count_by_class1.field("class_id");
        count_by_class1.size(2);
        searchSourceBuilder.aggregation(count_by_class1);

        MaxAggregationBuilder max_age1 = new MaxAggregationBuilder("max_age");
        max_age1.field("age");
        searchSourceBuilder.aggregation(max_age1);
        //分页
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(2);

        //2.创建Search对象
//        Search search = new Search.Builder("{\n" +
//                "  \"query\": {\n" +
//                "    \"bool\": {\n" +
//                "      \"filter\": {\n" +
//                "        \"term\": {\n" +
//                "          \"sex\": \"male\"\n" +
//                "        }\n" +
//                "      },\n" +
//                "      \"must\": [\n" +
//                "        {\n" +
//                "          \"match\": {\n" +
//                "            \"favo\": \"球\"\n" +
//                "          }\n" +
//                "        }\n" +
//                "      ]\n" +
//                "    }\n" +
//                "  },\n" +
//                "  \"aggs\": {\n" +
//                "    \"count_by_class\": {\n" +
//                "      \"terms\": {\n" +
//                "        \"field\": \"class_id\",\n" +
//                "        \"size\": 2\n" +
//                "      }\n" +
//                "    },\n" +
//                "    \"max_age\":{\n" +
//                "      \"max\": {\n" +
//                "        \"field\": \"age\"\n" +
//                "      }\n" +
//                "    }\n" +
//                "  },\n" +
//                "  \"from\": 0,\n" +
//                "  \"size\": 2\n" +
//                "}").build();
        Search search = new Search.Builder(searchSourceBuilder.toString()).build();

        //3.执行查询
        SearchResult result = jestClient.execute(search);

        //4.解析result
        System.out.println("成功获取数据" + result.getTotal() + "条！");
        System.out.println("最高分：" + result.getMaxScore());

        //获取hits标签的对应数据
        List<SearchResult.Hit<Map, Void>> hits = result.getHits(Map.class);
        for (SearchResult.Hit<Map, Void> hit : hits) {
            JSONObject jsonObject = new JSONObject();
            //获取数据
            Map source = hit.source;
            for (Object o : source.keySet()) {
                jsonObject.put((String) o, source.get(o));
            }
            jsonObject.put("index", hit.index);
            jsonObject.put("type", hit.type);
            jsonObject.put("id", hit.id);
            System.out.println(jsonObject.toString());
        }

        //解析聚合组数据
        MetricAggregation aggregations = result.getAggregations();

        //获取年龄组
        MaxAggregation max_age = aggregations.getMaxAggregation("max_age");
        System.out.println("最大年龄为:" + max_age.getMax() + "岁！");

        //获取班级分组
        TermsAggregation count_by_class = aggregations.getTermsAggregation("count_by_class");
        for (TermsAggregation.Entry entry : count_by_class.getBuckets()) {
            System.out.println(entry.getKey() + "->" + entry.getCount());
        }

        //关闭连接
        jestClient.shutdownClient();

    }

}
