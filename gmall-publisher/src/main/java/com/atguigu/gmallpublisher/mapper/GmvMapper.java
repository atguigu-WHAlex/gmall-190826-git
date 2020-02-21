package com.atguigu.gmallpublisher.mapper;

import java.util.List;
import java.util.Map;

public interface GmvMapper {

    //获取当天GMV总额
    public Double selectOrderAmountTotal(String date);

    //获取GMV分时统计结果
    public List<Map> selectOrderAmountHourMap(String date);

}
