package com.atguigu.gmalllogger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.constants.GmallConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

//@RestController = @Controller + @ResponseBody
@RestController
@Slf4j
public class LoggerController {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @GetMapping("test1")
    public String test1() {
        return "success";
    }

    @GetMapping("test2")
    public String test2(@RequestParam("aa") String aa) {
        return aa;
    }

    //    @ResponseBody
    @PostMapping("log")
    public String logger(@RequestParam("logString") String logStr) {

//        System.out.println(logStr);
        //0.添加时间戳字段
        JSONObject jsonObject = JSON.parseObject(logStr);
        jsonObject.put("ts", System.currentTimeMillis());

        String tsJson = jsonObject.toString();

        //1.使用log4j打印日志到控制台及文件
        log.info(tsJson);

        //2.使用Kafka生产者将数据发送到Kafka集群
        if (tsJson.contains("startup")) {
            //将数据发送至启动日志主题
            kafkaTemplate.send(GmallConstants.GMALL_STARTUP_TOPIC, tsJson);
        } else {
            //将数据发送至事件日志主题
            kafkaTemplate.send(GmallConstants.GMALL_EVENT_TOPIC, tsJson);
        }

        return "success";
    }

}