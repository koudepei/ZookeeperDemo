package com.dongnao.zookeeper.lock;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;

/**
 * （使用zookeeper原生api）实现分布式环境下同步锁的实现
 * @author parker
 * @date 2016/12/15
 */
public class DistributeLockDemo implements Watcher{

    ZooKeeper zk=null; //zookeeper原生api去实现一个分布式锁

    private String root="/locks";

    private String myZonode; //表示当前获取到的锁名称-也就是节点名称

    private String waitNode; // 表示当前等待的节点

    private CountDownLatch latch;

    //server链接字符串
    private static final String CONNECTION_STRING="192.168.1.104:2181,192.168.1.104:2182,192.168.1.104:2183";

    private static final int SESSION_TIMEOUT=5000; //超时时间

    /**
     * 构造函数初始化
     * @param config  表示zookeeper连接串
     */
    public DistributeLockDemo(String config){
        try {
            zk=new ZooKeeper(config,SESSION_TIMEOUT,this);
            Stat stat=zk.exists(root,false); //判断是不是已经存在locks节点，不需要监听root节点
            if(stat==null){ //如果不存在，则创建根节点
                zk.create(root,new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void process(WatchedEvent event) {
        if(this.latch!=null){ //如果计数器不为空话话，释放计数器锁
            this.latch.countDown();
        }
    }

    /**
     * 获取多的方法
     */
    public void lock(){
        if(tryLock()){
            System.out.println("Thread "+Thread.currentThread().getName()+" - hold lock!");
            return;
        }
        try { //等待并获取锁
            waitLock(waitNode,SESSION_TIMEOUT);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    /**
     * 释放锁操作的方法
     */
    public void unlock(){
        System.out.println("UnLock = "+ myZonode);
        try {
            zk.delete(myZonode,-1);
            myZonode=null;
            zk.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }

    }

    private boolean tryLock(){
        String splitStr="lock_"; //lock_0000000001
        try {
            //创建一个有序的临时节点，赋值给myznode
            myZonode=zk.create(root+"/"+splitStr,new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL_SEQUENTIAL);
            System.out.println(myZonode+" 创建成功");
            List<String> subNodes=zk.getChildren(root,false);
            Collections.sort(subNodes); //讲所有的子节点排序
            if(myZonode.equals(root+"/"+subNodes.get(0))){
                //当前客户端创建的临时有序节点是locks下节点中的最小的节点，表示当前的客户端能够获取到锁
                return true;
            }
            //否则的话,监听比自己小的节点 locks/lock_0000000003
            String subMyZnode=myZonode.substring((myZonode.lastIndexOf("/")+1));
            waitNode=subNodes.get(Collections.binarySearch(subNodes,subMyZnode)-1);// 获取比当前节点小的节点
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return false;
    }

    private boolean waitLock(String lower,long waitTime) throws KeeperException, InterruptedException {
        Stat stat=zk.exists(root+"/"+lower,true); //获取节点状态，并添加监听
        if(stat!=null){
            System.out.println("Thread "+Thread.currentThread().getName()+" waiting for"+root+" /"+lower);
            this.latch=new CountDownLatch(1); //实例化计数器，让当前的线程等待
            this.latch.await(waitTime, TimeUnit.MILLISECONDS);
            this.latch=null;
        }
        return true;
    }

    public static void main(String[] args) {
        ExecutorService executorService= Executors.newCachedThreadPool();
        final Semaphore semaphore=new Semaphore(10);
        for(int i=0;i<10;i++){
//            Runnable runnable=()->{ //jdk1.8 lambda的一种用法，在java8里面有。且变量semaphore不需要加final
//                try {
//                    semaphore.acquire();
//                    DistributeLockDemo distributeLockDemo=new DistributeLockDemo(CONNECTION_STRING);
//                    distributeLockDemo.lock();
//                    //业务代码
//                    Thread.sleep(3000);
//                    distributeLockDemo.unlock();
//                    semaphore.release();
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//            };

            Runnable runnable=new Runnable() {
                @Override
                public void run() {
                try {
                    semaphore.acquire();
                    DistributeLockDemo distributeLockDemo=new DistributeLockDemo(CONNECTION_STRING);
                    distributeLockDemo.lock();
                    //业务代码
                    Thread.sleep(3000);
                    distributeLockDemo.unlock();
                    semaphore.release();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                }
            };

            executorService.execute(runnable);
        }
        executorService.shutdown();
    }
}
