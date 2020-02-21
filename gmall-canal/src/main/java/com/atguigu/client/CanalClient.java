package com.atguigu.client;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.atguigu.constants.GmallConstants;
import com.atguigu.utils.KafkaSender;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

//读取Canal数据，解析之后发送到Kafka
public class CanalClient {

    public static void main(String[] args) throws InvalidProtocolBufferException {

        //获取Canal连接器
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop102", 11111), "example", "", "");

        //抓取数据并解析
        while (true) {
            //连接Canal
            canalConnector.connect();
            //指定订阅的数据库
            canalConnector.subscribe("gmall.*");
            //抓取数据
            Message message = canalConnector.get(100);

            //判断当前抓取是否有数据
            if (message.getEntries().size() <= 0) {
                System.out.println("没有数据，休息一下。。。");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {

                //有数据,取出Entry集合并遍历
                for (CanalEntry.Entry entry : message.getEntries()) {

                    //判断当前操作的类型，只留下对于数据操作的内容
                    if (CanalEntry.EntryType.ROWDATA.equals(entry.getEntryType())) {
                        //反序列数据
                        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                        //获取表名
                        String tableName = entry.getHeader().getTableName();
                        //取出事件类型
                        CanalEntry.EventType eventType = rowChange.getEventType();
                        //处理数据，发送至Kafka
                        handler(tableName, eventType, rowChange);
                    }
                }
            }
        }
    }

    //处理数据，发送至Kafka
    private static void handler(String tableName, CanalEntry.EventType eventType, CanalEntry.RowChange rowChange) {

        //订单表并且是下单数据
        if ("order_info".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) {

            //遍历RowDatasList
            for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {

                //创建JSON对象，用于存放一行数据
                JSONObject jsonObject = new JSONObject();
                jsonObject.put("tableName", tableName);

                //获取变化后的数据并遍历
                for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                    jsonObject.put(column.getName(), column.getValue());
                }

                //发送至Kafka
                System.out.println(jsonObject.toString());
                KafkaSender.send(GmallConstants.GMALL_ORDER_INFO_TOPIC, jsonObject.toString());
            }
        }
    }
}
