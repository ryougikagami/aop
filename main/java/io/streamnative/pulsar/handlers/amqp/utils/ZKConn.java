package io.streamnative.pulsar.handlers.amqp.utils;

import org.apache.commons.codec.binary.Hex;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * 这个是用户名密码创建的脚本
 */
public class ZKConn {
    private ZooKeeper mZooKeeper;

    public static void main(String[] args) {
        //初始化log4j，zookeeper否则报错。
//        org.apache.log4j.BasicConfigurator.configure();
        String node = args[0];
        String data = args[1];
        System.out.println("node"+node);
        System.out.println("data"+data);
        try {
            ZKConn m = new ZKConn();
            m.operation(m.mZooKeeper,node,data);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public ZKConn() throws Exception {
        String ip = "localhost";
        String addrs = ip + ":2181," + ip + ":2182," + ip + ":2183";
        //连接zookeeper服务器。
        //addrs是一批地址，如果其中某一个服务器挂掉，其他仍可用。
        mZooKeeper = new ZooKeeper(addrs, 300 * 1000, new MyWatcher());

        synchronized (ZKConn.class) {
            System.out.println("等待连接建立...");
            ZKConn.class.wait();
        }

        System.out.println("继续执行");

//        operation(mZooKeeper);
        //mZooKeeper.close();
    }

    private class MyWatcher implements Watcher {
        @Override
        public void process(WatchedEvent event) {
            if (mZooKeeper.getState() == ZooKeeper.States.CONNECTED) {
                System.out.println("连接建立！");

                synchronized (ZKConn.class) {
                    System.out.println("唤醒主线程的等待!");
                    ZKConn.class.notifyAll();
                }
            }

            if (event.getState() == Event.KeeperState.SyncConnected) {
                System.out.println("状态:" + Event.KeeperState.SyncConnected);
            }
        }
    }

    private void operation(ZooKeeper zk,String node,String data) throws Exception {
        if (zk.getState().equals(ZooKeeper.States.CONNECTED)) {
            System.out.println("已经连接到zookeeper服务器");
        } else {
            System.out.println("连接zookeeper服务器失败!");

            return;
        }


        data = getSHA256Str(data);

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

