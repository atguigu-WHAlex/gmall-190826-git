package com.atguigu.writer;

import com.atguigu.bean.Stu;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;

import java.io.IOException;

public class ESWriteByBulk {

    public static void main(String[] args) throws IOException {

        //1.创建ES客户端
        JestClientFactory jestClientFactory = new JestClientFactory();
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://hadoop102:9200").build();
        jestClientFactory.setHttpClientConfig(httpClientConfig);
        JestClient jestClient = jestClientFactory.getObject();

        //2.创建2个Index对象
        Stu stu1 = new Stu();
        stu1.setStu_id(6);
        stu1.setName("xiangang");

        Stu stu2 = new Stu();
        stu2.setStu_id(7);
        stu2.setName("xiaohong");

        Index index1 = new Index.Builder(stu1).id("1006").build();
        Index index2 = new Index.Builder(stu2).id("1007").build();

        //创建Bulk对象
        Bulk bulk = new Bulk.Builder()
                .defaultIndex("stu")
                .defaultType("_doc")
                .addAction(index1)
                .addAction(index2)
                .build();

        //执行
        jestClient.execute(bulk);

        //关闭连接
        jestClient.shutdownClient();
    }

}
