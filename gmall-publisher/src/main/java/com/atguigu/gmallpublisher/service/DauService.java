package com.atguigu.gmallpublisher.service;


import java.util.Map;

public interface DauService {

    //获取总数
    public int getTotal(String date);

    //获取分时统计的数据
    public Map getRealTimeHours(String date);

}
