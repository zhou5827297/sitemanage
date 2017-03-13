package com.dangmeitoutiao.manage.monitor;

import com.dangmeitoutiao.manage.config.ServerConstant;
import com.dangmeitoutiao.manage.util.FileUtil;
import com.dangmeitoutiao.manage.util.TarUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.springframework.util.FileCopyUtils;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 爬虫服务器，集群监听管理器
 */
public class SpiderServerMonitor implements Watcher {
    private final Logger LOG = LogManager.getLogger(getClass());

    private final static String GROUPNODE = ServerConstant.ZOOKEEPER_GROUP_NODE;
    private static final ScheduledExecutorService SCHEDULEDEXECUTOR = Executors.newSingleThreadScheduledExecutor();
    private final static String EXT = ".tar.gz";
    private volatile Map<String, ServerInfo> serverList = new TreeMap<String, ServerInfo>();
    private ZooKeeper zk;
    private String host;
    private Stat stat = new Stat();

    /**
     * 连接zookeeper服务器
     */
    public void connectZookeeper(String host) throws Exception {
        this.host = host;
        zk = new ZooKeeper(host, 10000, this);
        //查看要检测的服务器集群的根节点是否存在，如果不存在，则创建
        if (null == zk.exists("/" + GROUPNODE, false)) {
            zk.create("/" + GROUPNODE, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        updateServerList();
    }

    /**
     * 定时强制同步svn文件
     */
    public void scheduleFlushSite() {
        SCHEDULEDEXECUTOR.scheduleAtFixedRate(new Runnable() {
            public void run() {
                try {
                    // svn有更新
                    File flagFile = new File(ServerConstant.SPIDER_SITE_UPDATE_FLAG_PATH);
                    if (flagFile.exists()) {
                        restart(ServerConstant.ZOOKEEPER_SERVER);
                        flagFile.delete();
                        LOG.info("schedule update server success ...");
                    }
                } catch (Exception e) {
                    LOG.error("schedule update server error [{}]", e.getMessage());
                }
            }
        }, 30, 5, TimeUnit.MINUTES);
    }

    /**
     * 更新服务器列表信息
     */
    private void updateServerList() throws Exception {
        Map<String, ServerInfo> newServerList = new TreeMap<String, ServerInfo>();
        List<String> subList = zk.getChildren("/" + GROUPNODE, true);
        //根据服务器数目，先均衡配置文件
        loadBalanceSiteFileAndGzip(subList);
        for (String subNode : subList) {
            ServerInfo serverInfo = new ServerInfo();
            serverInfo.setPath("/" + GROUPNODE + "/" + subNode);
            serverInfo.setName(subNode);

            String path = ServerConstant.DATA_PATH + "/" + subNode + EXT;
            byte[] data = FileUtil.getBytes(path);
            zk.setData(serverInfo.getPath(), data, -1);
            //监听节点变化
            zk.getData(serverInfo.getPath(), true, stat);
            serverInfo.setData(data);

            newServerList.put(serverInfo.getPath(), serverInfo);

        }
        // 替换server列表
        serverList = newServerList;
        LOG.info("update server list : [{}]", serverList);
    }

    /**
     * 更新服务器节点的负载数据
     */
    private void updateServerHeart(String serverNodePath) throws Exception {
        ServerInfo serverInfo = serverList.get(serverNodePath);
        if (null != serverInfo) {
            byte[] data = zk.getData(serverInfo.getPath(), true, stat);
            serverInfo.setData(data);
            serverList.put(serverInfo.getPath(), serverInfo);
            LOG.info("update server heart :[{}]", serverInfo);
        }
    }

    /**
     * 均衡爬虫文件，存放到zk节点，待发送给给服务器
     */
    private void loadBalanceSiteFileAndGzip(List<String> subList) throws Exception {
        int serviceSize = subList.size();
        if (serviceSize == 0) {
            return;
        }
        File sitePath = new File(ServerConstant.SPIDER_SITE_PATH);
        if (!sitePath.exists()) {
            sitePath.mkdirs();
        }
        List<File> sites = this.files(new ArrayList<File>(), sitePath);
        if (sites.size() == 0) {
            return;
        }
        File dataPath = new File(ServerConstant.DATA_PATH);
        if (dataPath.exists()) {
            FileUtil.delAllFile(ServerConstant.DATA_PATH);
        }
        dataPath.mkdirs();

        int siteSize = sites.size();
        int one = siteSize / serviceSize;

        int begin = 0;
        int end = 0;
        for (int i = 0; i < serviceSize; i++) {
            begin = one * i;
            end = (one * (i + 1)) - 1;
            if (i == serviceSize - 1) {
                end = siteSize;
            }

            String nodePath = subList.get(i);
            String siteDic = ServerConstant.DATA_PATH + "/" + nodePath;
            File siteDicFile = new File(siteDic);
            if (siteDicFile.exists()) {
                FileUtil.delAllFile(siteDic);
            }
            siteDicFile.mkdir();

            for (int j = begin; j < end; j++) {
                File file = sites.get(j);
                File outFile = new File(siteDic + "/" + file.getName());
                if (outFile.exists()) { //可能会出现重名
                    outFile = new File(siteDic + "/" + System.currentTimeMillis() + file.getName());
                }
                FileCopyUtils.copy(file, outFile);
            }
        }
        for (String nodePath : subList) {
            String siteDic = ServerConstant.DATA_PATH + "/" + nodePath;
            TarUtil.tarGz(new File(siteDic));
        }
    }


    /**
     * 读取所有爬虫配置文件
     */
    private List<File> files(List<File> ret, File file) {
        if (file.isDirectory()) {
            if (!".svn".equals(file.getName())) {
                File[] fs = file.listFiles();
                for (File f : fs) {
                    files(ret, f);
                }
            }
        } else if (file.getName().endsWith(".json")) {
            ret.add(file);
        }
        return ret;
    }

    public void process(WatchedEvent event) {
        LOG.info("listen zookeeper event -----eventType:[{}]，path[{}]", event.getType(), event.getPath());
        //爬虫服务器列表发生改变
        if (event.getType() == EventType.NodeChildrenChanged && event.getPath().equals("/" + GROUPNODE)) {
            try {
                updateServerList();
            } catch (KeeperException.SessionExpiredException ex) {
                LOG.error("zookeeper session expired , ready to restart ... [{}]", ex);
                restart(host);
            } catch (KeeperException.ConnectionLossException ex) {
                LOG.error("zookeeper connection loss ,waiting retry ... [{}]", ex);
                try {
                    Thread.sleep(5000);
                    updateServerList();
                } catch (Exception e) {
                    LOG.error("zookeeper connection loss , retry error ... [{}]", ex);
                }
            } catch (Exception ex) {
                LOG.error("update server error [{}]", ex);
            }
        }
        //数据发生改变
        if (event.getType() == EventType.NodeDataChanged && event.getPath().startsWith("/" + GROUPNODE)) {
            try {
                updateServerHeart(event.getPath());
            } catch (KeeperException.SessionExpiredException ex) {
                LOG.error("zookeeper session expired , ready to restart ... [{}]", ex);
                restart(host);
            } catch (KeeperException.ConnectionLossException ex) {
                LOG.error("zookeeper connection loss ,waiting retry ... [{}]", ex);
                try {
                    Thread.sleep(5000);
                    updateServerHeart(event.getPath());
                } catch (Exception e) {
                    LOG.error("zookeeper connection loss , retry error ... [{}]", ex);
                }
            } catch (Exception ex) {
                LOG.error("update server error [{}]", ex);
            }
        }
    }

    /**
     * 休眠，持续监控
     */
    public void handle() throws InterruptedException {
        while (true) {
            try {
                Thread.sleep(Long.MAX_VALUE);
            } catch (Exception ex) {
                LOG.error(ex.getMessage(), ex);
            }
        }
    }

    /**
     * 关闭于zookeeper服务器的连接
     */
    public void closeZookeeper() {
        if (null != zk) {
            try {
                zk.close();
            } catch (InterruptedException e) {
                LOG.error(e.getMessage(), e);
            }
        }
    }

    /**
     * 重启zk客户端
     */
    public void restart(String host) {
        closeZookeeper();
        try {
            connectZookeeper(host);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    /**
     * 服务器信息
     */
    class ServerInfo {
        private String path;
        private String name;
        private byte[] data;

        public String getPath() {
            return path;
        }

        public void setPath(String path) {
            this.path = path;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public byte[] getData() {
            return data;
        }

        public void setData(byte[] data) {
            this.data = data;
        }

        @Override
        public String toString() {
            return "ServerInfo{" +
                    "name='" + name + '\'' +
                    ", path='" + path + '\'' +
                    ", data=" + data.length +
                    '}';
        }
    }

}
