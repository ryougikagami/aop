package io.streamnative.pulsar.handlers.amqp.utils;

import org.apache.commons.codec.binary.Hex;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class ZkUtils {
    private ZooKeeper mZooKeeper;
    public static void main(String[] args) {
        //初始化log4j，zookeeper否则报错。
//        org.apache.log4j.BasicConfigurator.configure();

        try {
            ZkUtils m = new ZkUtils();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public ZkUtils() throws Exception {
        String ip = "10.155.192.220";
        String addrs = ip + ":2181," + ip + ":2182," + ip + ":2183";

        //连接zookeeper服务器。
        //addrs是一批地址，如果其中某一个服务器挂掉，其他仍可用。
        mZooKeeper = new ZooKeeper(addrs, 300 * 1000, new MyWatcher());

        synchronized (ZkUtils.class) {
            System.out.println("等待连接建立...");
            ZkUtils.class.wait();
        }

        System.out.println("继续执行");

        operation(mZooKeeper);
//        getData(mZooKeeper);
        //mZooKeeper.close();
    }

    private class MyWatcher implements Watcher {
        @Override
        public void process(WatchedEvent event) {
            if (mZooKeeper.getState() == ZooKeeper.States.CONNECTED) {
                System.out.println("连接建立！");

                synchronized (ZkUtils.class) {
                    System.out.println("唤醒主线程的等待!");
                    ZkUtils.class.notifyAll();
                }
            }

            if (event.getState() == Event.KeeperState.SyncConnected) {
                System.out.println("状态:" + Event.KeeperState.SyncConnected);
            }
        }
    }

    public String getData(String node) throws Exception{
        String ip = "10.155.192.220";
        String addrs = ip + ":2181," + ip + ":2182," + ip + ":2183";

        //连接zookeeper服务器。
        //addrs是一批地址，如果其中某一个服务器挂掉，其他仍可用。
        mZooKeeper = new ZooKeeper(addrs, 300 * 1000, new MyWatcher());

        synchronized (ZkUtils.class) {
            System.out.println("等待连接建立...");
            ZkUtils.class.wait();
        }

        System.out.println("继续执行");
        if (mZooKeeper.getState().equals(ZooKeeper.States.CONNECTED)) {
            System.out.println("已经连接到zookeeper服务器");
        } else {
            System.out.println("连接zookeeper服务器失败!");

            return null;
        }

//        String node = "/yqAccount";
//        String data = "yqPwd";

        //检测node节点是否存在。
        Stat stat = mZooKeeper.exists(node, false);

//        if (stat == null) {
//            //创建节点。
//            String result = mZooKeeper.create(node, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
//            System.out.println(result);
//        }

        //获取节点的值。
        byte[] b = mZooKeeper.getData(node, false, stat);
        return new String(b);
    }

    private void operation(ZooKeeper zk) throws Exception {
        if (zk.getState().equals(ZooKeeper.States.CONNECTED)) {
            System.out.println("已经连接到zookeeper服务器");
        } else {
            System.out.println("连接zookeeper服务器失败!");

            return;
        }

        String node = "/yqAccount";
        String data = "yqPwd";

        //检测node节点是否存在。
        Stat stat = zk.exists(node, false);

        if (stat == null) {
            //创建节点。
            String result = zk.create(node, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            System.out.println(result);
        }

        //获取节点的值。
        byte[] b = zk.getData(node, false, stat);
        System.out.println(new String(b));

        zk.close();
    }

    public static String getSHA256Str(String str){
        MessageDigest messageDigest;
        String encdeStr = "";
        try {
            messageDigest = MessageDigest.getInstance("SHA-256");
            byte[] hash = messageDigest.digest(str.getBytes("UTF-8"));
            encdeStr = Hex.encodeHexString(hash);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return encdeStr;
    }
}
