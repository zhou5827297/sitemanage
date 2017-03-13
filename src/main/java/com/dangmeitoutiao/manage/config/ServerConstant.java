package com.dangmeitoutiao.manage.config;


import com.dangmeitoutiao.manage.util.PropertiesUtils;

import java.util.Properties;

/**
 * 服务配置信息
 */
public class ServerConstant {
    private final static Properties PROPERTIES = PropertiesUtils.loadProperties("config/server.properties");
    /**
     * zookeeper注册中心
     */
    public final static String ZOOKEEPER_SERVER = PROPERTIES.getProperty("zookeeper.server");
    /**
     * spider所在组名
     */
    public final static String ZOOKEEPER_GROUP_NODE = PROPERTIES.getProperty("zookeeper.group_node");
    /**
     * 爬虫配置文件目录
     */
    public final static String SPIDER_SITE_PATH = PROPERTIES.getProperty("spider_site_path");
    /**
     * site配置文件更新标志
     */
    public final static String SPIDER_SITE_UPDATE_FLAG_PATH = PROPERTIES.getProperty("spider_site_update_flag_path");
    /**
     * 执行目录
     */
    public final static String DATA_PATH = PROPERTIES.getProperty("data_path");

}
