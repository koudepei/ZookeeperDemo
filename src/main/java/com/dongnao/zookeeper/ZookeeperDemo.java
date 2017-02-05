package com.dongnao.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author parker
 * @date 2016/12/8
 */
public class ZookeeperDemo {

    private static final int SESSION_TIMEOUT=3000;

    public static void main(String[] args) throws NoSuchAlgorithmException {
        ZooKeeper zk=null;
        try {
            zk=new ZooKeeper("192.168.1.104:2181", SESSION_TIMEOUT, new Watcher() {
                @Override    //jdk1.6才支持这里写@Override
                public void process(WatchedEvent event) {
                    System.out.println("触发事件："+event.getType());
                }
            });
            createDemo(zk);
            updateDemo(zk);
            deleteDemo(zk);
            aclDemo(zk);
            watcherDemo(zk);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }finally{
            if(zk!=null){
                try {
                    zk.close();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    private static void createDemo(ZooKeeper zk) throws KeeperException, InterruptedException {
        if(zk.exists("/node_2",true)==null){
             /*
            三种权限范围：OPEN_ACL_UNSAFE ： 对所有用户开放    READ_ACL_UNSAFE ： 只读    CREATOR_ALL_ACL： 创建者可以做任何操作
            四种节点类型：1. 持久化节点 2. 持久化有序节点 3. 临时节点 4. 临时有序节点
            */
            zk.create("/node_2","abc".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            System.out.println(new String("create /node_2:"+zk.getData("/node_2",true,null)));
        }
    }

    private static void aclDemo(ZooKeeper zk) throws KeeperException, InterruptedException, NoSuchAlgorithmException {
        if(zk.exists("/node_3",true)==null){
            /*
            四种自定义schema权限类型： digest模式   world  auth   ip
             */
            ACL acl=new ACL(ZooDefs.Perms.ALL,new Id("digest", DigestAuthenticationProvider.generateDigest("root:root")));
            List<ACL> acls=new ArrayList<ACL>();
            acls.add(acl);
            zk.create("/node_3","abc".getBytes(), acls, CreateMode.PERSISTENT);
            //必须要设置权限类型，才能进行获取节点数据
            //System.out.println(new String(zk.getData("/node_3",true,null)));
        }
        zk.addAuthInfo("digest","root:root".getBytes());
        System.out.println( "获取自定义schema权限类型节点 /node_3:"+new String(zk.getData("/node_3",true,null)));
    }

    private static void updateDemo(ZooKeeper zk) throws KeeperException, InterruptedException {
        zk.setData("/node_2","www".getBytes(),-1); //version -1表示忽略该节点的dataversion版本号，不用乐观锁
        System.out.println("update /node_2:"+new String(zk.getData("/node_2",true,null)));
    }

    private static void deleteDemo(ZooKeeper zk) throws KeeperException, InterruptedException {
        zk.delete("/node_2",-1);
//        System.out.println("delete /node_2:"+new String(zk.getData("/node_2",true,null)));
    }

    private static void watcherDemo(final ZooKeeper zk) throws KeeperException, InterruptedException {
        if(zk.exists("/node_4",true)!=null) {
           zk.delete("/node_4",-1);
        }
        zk.create("/node_4", "abc".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);


        byte[] rsByt = zk.getData("/node_4", new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println("getData触发节点事件："+event.getType()+" path:" + event.getPath());
                try {
                    System.out.println("修改值为:"+new String(zk.getData(event.getPath(),true,null)));
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }, null);
        System.out.println("/node_4 修改前值:"+new String(rsByt));
        zk.setData("/node_4","pksyz".getBytes(),-1);
        System.out.println("/node_4 修改后值:"+new String(zk.getData("/node_4",true,null)));
    }
}
