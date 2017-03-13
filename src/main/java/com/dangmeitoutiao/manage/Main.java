package com.dangmeitoutiao.manage;

import com.dangmeitoutiao.manage.config.ServerConstant;
import com.dangmeitoutiao.manage.monitor.SpiderServerMonitor;
import org.apache.zookeeper.KeeperException;

/**
 * 启动站点管理
 */
public class Main {

    /**
     * 监控入口
     */
    public static void main(String[] args) {
        SpiderServerMonitor monitor = new SpiderServerMonitor();
        monitor.scheduleFlushSite();
        while (true) {
            try {
                monitor.connectZookeeper(ServerConstant.ZOOKEEPER_SERVER);
                while (true) {
                    monitor.handle();
                }
            } catch (KeeperException ex) {
                ex.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
