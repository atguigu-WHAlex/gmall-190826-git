package com.atguigu.writer;

import com.atguigu.bean.Stu;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Index;

import java.io.IOException;

public class ESWriter {

    public static void main(String[] args) throws IOException {

        //1.创建ES客户端
        JestClientFactory jestClientFactory = new JestClientFactory();
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://hadoop102:9200").build();
        jestClientFactory.setHttpClientConfig(httpClientConfig);
        JestClient jestClient = jestClientFactory.getObject();

        //创建Stu对象
        Stu stu = new Stu();
        stu.setStu_id(3);
        stu.setName("wangwu");

        //2.创建ES对象
        Index index = new Index.Builder(stu)
                .index("stu")
                .type("_doc")
                .id("1003")
                .build();

        //3.执行插入数据操作
        jestClient.execute(index);

        //4.关闭连接
        jestClient.shutdownClient();

    }

}
