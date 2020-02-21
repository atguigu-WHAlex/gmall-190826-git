package com.atguigu.gmallpublisher.service;

import java.util.Map;

public interface GmvService {

    //获取GMV总额
    public Double getTotal(String date);

    //获取GMV分时统计结果
    public Map getHoursGmv(String date);
}
