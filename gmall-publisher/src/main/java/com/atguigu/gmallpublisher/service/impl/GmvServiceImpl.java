package com.atguigu.gmallpublisher.service.impl;

import com.atguigu.gmallpublisher.mapper.GmvMapper;
import com.atguigu.gmallpublisher.service.GmvService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class GmvServiceImpl implements GmvService {

    @Autowired
    GmvMapper gmvMapper;

    @Override
    public Double getTotal(String date) {
        return gmvMapper.selectOrderAmountTotal(date);
    }

    @Override
    public Map getHoursGmv(String date) {

        List<Map> list = gmvMapper.selectOrderAmountHourMap(date);

        //创建Map用于存放结果数据
        HashMap<String, Double> map = new HashMap<>();

        //遍历list给map添加数据
        for (Map map1 : list) {
            map.put((String) map1.get("CREATE_HOUR"), (Double) map1.get("SUM_AMOUNT"));
        }

        return map;
    }

}
