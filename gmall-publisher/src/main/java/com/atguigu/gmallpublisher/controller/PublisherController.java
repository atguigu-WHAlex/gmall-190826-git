package com.atguigu.gmallpublisher.controller;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmallpublisher.bean.Option;
import com.atguigu.gmallpublisher.bean.Stat;
import com.atguigu.gmallpublisher.service.DauService;
import com.atguigu.gmallpublisher.service.GmvService;
import com.atguigu.gmallpublisher.service.SaleDetailService;
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

    @Autowired
    GmvService gmvService;

    @Autowired
    SaleDetailService saleDetailService;

    @GetMapping("realtime-total")
    public String getRealTimeTotal(@RequestParam("date") String date) {

        //1.获取总数
        int total = dauService.getTotal(date);
        Double amount = gmvService.getTotal(date);

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

        //5.创建Map用于存放GMV数据
        HashMap<String, Object> gmvMap = new HashMap<>();
        gmvMap.put("id", "order_amount");
        gmvMap.put("name", "新增交易额");
        gmvMap.put("value", amount);

        //5.将日活数据及新增数据添加至集合
        result.add(dauMap);
        result.add(newMidMap);
        result.add(gmvMap);

        //6.将集合转换为字符串返回
        return JSON.toJSONString(result);
    }

    @GetMapping("realtime-hours")
    public String getRealTimeHours(@RequestParam("id") String id, @RequestParam("date") String date) {

        //创建集合用户存放查询结果
        HashMap<String, Map> result = new HashMap<>();
        String yesterday = getYesterday(date);
        Map todayMap = null;
        Map yesterdayMap = null;

        if ("dau".equals(id)) {
            //查询当天数据 date:2020-02-19
            todayMap = dauService.getRealTimeHours(date);
            //查询昨日数据 date:2020-02-18
            yesterdayMap = dauService.getRealTimeHours(yesterday);

        } else if ("order_amount".equals(id)) {
            todayMap = gmvService.getHoursGmv(date);
            yesterdayMap = gmvService.getHoursGmv(yesterday);
        }

        //将今天的以及昨天的数据存放至result
        result.put("yesterday", yesterdayMap);
        result.put("today", todayMap);

        //将集合转换为字符串返回
        return JSON.toJSONString(result);
    }


    //获取订单详情数据
    @GetMapping("sale_detail")
    public String getSaleDetail(@RequestParam("date") String date, @RequestParam("startpage") Integer startpage, @RequestParam("size") Integer size, @RequestParam("keyword") String keyword) {

        //查询ES获取数据集
        HashMap<String, Object> saleDetail = saleDetailService.getSaleDetail(date, startpage, size, keyword);

        HashMap<String, Object> result = new HashMap<>();

        //获取saleDetail中各项数据
        Long total = (Long) saleDetail.get("total");
        List detail = (List) saleDetail.get("detail");

        //获取性别聚合组数据并解析("F"->5,"M"->8)
        Map genderMap = (Map) saleDetail.get("genderMap");
        Long femaleCount = (Long) genderMap.get("F");
        double femaleRatio = Math.round(femaleCount * 1000 / total) / 10D;
        double maleRatio = 100D - femaleRatio;

        Option maleOp = new Option("男", maleRatio);
        Option femaleOp = new Option("女", femaleRatio);

        ArrayList<Option> genderList = new ArrayList<>();
        genderList.add(maleOp);
        genderList.add(femaleOp);

        Stat genderStat = new Stat(genderList, "用户性别占比");

        //获取年龄聚合组数据并解析("10"->5,"12"->8,"25"->3,"32"->5)
        Map ageMap = (Map) saleDetail.get("ageMap");

        Long lower20 = 0L;
        Long start20to30 = 0L;

        for (Object o : ageMap.keySet()) {
            Integer age = (Integer) o;
            Long ageCount = (Long) ageMap.get(o);
            if (age < 20) {
                lower20 += ageCount;
            } else if (age < 30) {
                start20to30 += ageCount;
            }
        }

        double lower20Ratio = Math.round(lower20 * 1000 / total) / 10D;
        double start20to30Ratio = Math.round(start20to30 * 1000 / total) / 10D;
        double upper30Ratio = 100D - lower20Ratio - start20to30Ratio;

        Option lower20Op = new Option("20岁以下", lower20Ratio);
        Option start20to30Op = new Option("20岁到30岁", start20to30Ratio);
        Option upper30Op = new Option("30岁及30岁以上", upper30Ratio);

        ArrayList<Option> ageList = new ArrayList<>();
        ageList.add(lower20Op);
        ageList.add(start20to30Op);
        ageList.add(upper30Op);

        Stat ageStat = new Stat(ageList, "用户年龄占比");

        //创建结合用于存放年龄占比及性别占比
        ArrayList<Stat> stats = new ArrayList<>();
        stats.add(genderStat);
        stats.add(ageStat);

        //添加数据
        result.put("total", total);
        result.put("stat", stats);
        result.put("detail", detail);

        //解析saleDetail,使其成为JSON格式
        return JSON.toJSONString(result);
    }

    private static String getYesterday(String date) {
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
        return sdf.format(new Date(instance.getTimeInMillis()));
    }

}
