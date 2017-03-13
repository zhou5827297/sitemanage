package com.dangmeitoutiao.manage.util;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

/**
 * zk工具包
 */
public class ZookeeperUtil {
    private final static int RETRY = 5;

    /**
     * 设置节点数据
     */
    public static void setData(ZooKeeper zk, String path, byte[] data) throws Exception {
        for (int i = 1; i <= RETRY; i++) {
            try {
                zk.setData(path, data, -1);
                break;
            } catch (KeeperException.ConnectionLossException ex) {
                ex.printStackTrace();
            } catch (Exception ex) {
                throw ex;
            }
        }
    }
}
