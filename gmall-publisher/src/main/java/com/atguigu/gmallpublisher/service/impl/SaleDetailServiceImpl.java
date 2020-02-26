package com.atguigu.gmallpublisher.service.impl;

import com.atguigu.constants.GmallConstants;
import com.atguigu.gmallpublisher.service.SaleDetailService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.jcodings.util.Hash;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class SaleDetailServiceImpl implements SaleDetailService {

    @Autowired
    JestClient jestClient;

    //根据传入的参数查询ES,获取数据
    @Override
    public HashMap<String, Object> getSaleDetail(String date, Integer startpage, Integer size, String keyword) {

        //1.构建查询语句
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        //添加查询过滤条件
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("dt", date));
        MatchQueryBuilder sku_name = new MatchQueryBuilder("sku_name", keyword);
        sku_name.operator(MatchQueryBuilder.Operator.AND);
        boolQueryBuilder.must(sku_name);
        searchSourceBuilder.query(boolQueryBuilder);

        //添加聚合组
        TermsBuilder genderTerms = AggregationBuilders.terms("count_by_gender").field("user_gender").size(2);
        TermsBuilder ageTerms = AggregationBuilders.terms("count_by_age").field("user_age").size(100);
        searchSourceBuilder.aggregation(genderTerms);
        searchSourceBuilder.aggregation(ageTerms);

        //分页
        searchSourceBuilder.from((startpage - 1) * size);
        searchSourceBuilder.size(size);

        //2.执行查询
        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstants.GMALL_SALE_DETAIL_INDEX).addType("_doc").build();
        SearchResult result = null;
        try {
            result = jestClient.execute(search);
        } catch (IOException e) {
            e.printStackTrace();
        }

        //3.解析结果
        assert result != null;

        HashMap<String, Object> resultMap = new HashMap<>();

        //获取总数
        Long total = result.getTotal();

        //获取明细数据
        ArrayList<Map> detailMap = new ArrayList<>();
        List<SearchResult.Hit<Map, Void>> hits = result.getHits(Map.class);
        for (SearchResult.Hit<Map, Void> hit : hits) {
            detailMap.add(hit.source);
        }

        //获取聚合组数据
        MetricAggregation aggregations = result.getAggregations();
        TermsAggregation count_by_gender = aggregations.getTermsAggregation("count_by_gender");
        HashMap<String, Long> genderMap = new HashMap<>();
        for (TermsAggregation.Entry entry : count_by_gender.getBuckets()) {
            genderMap.put(entry.getKey(), entry.getCount());
        }

        TermsAggregation count_by_age = aggregations.getTermsAggregation("count_by_age");
        HashMap<Integer, Long> ageMap = new HashMap<>();
        for (TermsAggregation.Entry entry : count_by_age.getBuckets()) {
            ageMap.put(Integer.parseInt(entry.getKey()), entry.getCount());
        }

        //添加元素
        resultMap.put("total", total);
        resultMap.put("detail", detailMap);
        resultMap.put("genderMap", genderMap);
        resultMap.put("ageMap", ageMap);

        return resultMap;
    }
}
