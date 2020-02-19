package com.atguigu.gmallpublisher.controller;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmallpublisher.service.DauService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@RestController
public class PublisherController {

    @Autowired
    DauService dauService;

    @GetMapping("realtime-total")
    public String getRealTimeTotal(@RequestParam("date") String date) {

        //1.获取总数
        int total = dauService.getTotal(date);

        //2.创建集合用于存放JSON对象
        ArrayList<Map> result = new ArrayList<>();

        //3.创建Map用于存放日活数据
        HashMap<String, Object> dauMap = new HashMap<>();
        dauMap.put("id", "dau");
        dauMap.put("name", "新增日活");
        dauMap.put("value", total);

        //4.创建Map用于存放新增数据
        HashMap<String, Object> newMidMap = new HashMap<>();
        newMidMap.put("id", "new_mid");
        newMidMap.put("name", "新增设备");
        newMidMap.put("value", "233");

        //5.将日活数据及新增数据添加至集合
        result.add(dauMap);
        result.add(newMidMap);

        //6.将集合转换为字符串返回
        return JSON.toJSONString(result);
    }

    @GetMapping("realtime-hours")
    public String getRealTimeHours(@RequestParam("id") String id, @RequestParam("date") String date) {

        //创建集合用户存放查询结果
        HashMap<String, Map> result = new HashMap<>();

        //查询当天数据 date:2020-02-19
        Map todayMap = dauService.getRealTimeHours(date);

        //查询昨日数据 date:2020-02-18
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Calendar instance = Calendar.getInstance();
        try {
            instance.setTime(sdf.parse(date));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        //将当天时间减一
        instance.add(Calendar.DAY_OF_MONTH, -1);
        //2020-02-18
        String yesterday = sdf.format(new Date(instance.getTimeInMillis()));

        Map yesterdayMap = dauService.getRealTimeHours(yesterday);

        //将今天的以及昨天的数据存放至result
        result.put("yesterday", yesterdayMap);
        result.put("today", todayMap);

        //将集合转换为字符串返回
        return JSON.toJSONString(result);
    }

}
