package com.dongnao.zookeeper.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.api.transaction.CuratorTransaction;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.Stat;

import java.util.Collection;
import java.util.List;

/**
 * @author parker
 * @date 2016/12/13
 */
public class CuratorDemo {

    private static String CONNECT_STRING="192.168.1.104:2181,192.168.1.104:2182,192.168.1.104:2183";

    private static int SESSION_TIMEOUT=3000;

    public static void main(String[] args) throws Exception {
        //TODO 连接zookeeper
        /*四中重试策略
        1. ExponentialBackoffRetry  衰减重试
2. RetryNTimes 指定最大重试次数
3. RetryOneTime 只重试一次
4. RetryUntilElased 一直重试直到规定时间
         */
        CuratorFramework framework=CuratorFrameworkFactory.
                newClient(CONNECT_STRING,SESSION_TIMEOUT,SESSION_TIMEOUT,new ExponentialBackoffRetry(1000,10));
//        //也可以用如下fluent风格的操作方式构造CuratorFramework
//        CuratorFramework framework=CuratorFrameworkFactory.builder().sessionTimeoutMs(SESSION_TIMEOUT)
//                .connectString(CONNECT_STRING)
//                .connectionTimeoutMs(SESSION_TIMEOUT)
//                .retryPolicy(new ExponentialBackoffRetry(1000,10));
        framework.start();
        System.out.println(framework.getState()); //获取连接状态  成功返回STARTED

//        create(framework);
//        update(framework);
//        delete(framework);
//        transaction(framework);
//        listener(framework);
        listener2(framework);
//        listener3(framework);
        System.in.read();
        System.out.println(framework.getState()); //获取连接状态  正常推出返回 STOPPED
    }

    private static void create(CuratorFramework cf){
        try {
            //creatingParentsIfNeeded 递归创建节点，且可以设置末级节点的值，比ZkClient更方便
            //withMode设置节点类型；inBackground后台异步执行返回值为null
//            String rs=cf.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath("/node_12/node_12_1","abc".getBytes());
            String rs=cf.create().withMode(CreateMode.EPHEMERAL).inBackground().forPath("/node_14/node_14_1","zzy".getBytes());
            System.out.println(rs);
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            cf.close();
        }
    }

    private static void update(CuratorFramework cf) throws Exception {
        Stat stat=cf.setData().forPath("/node_12/node_12_1","xyz".getBytes());
        System.out.println(stat);
        cf.close();
    }

    private static void delete(CuratorFramework cf){
        try {
            if(cf.checkExists().forPath("/node_13")!=null){
                cf.delete().deletingChildrenIfNeeded().forPath("/node_13"); //跟Zookeeper API类似 递归删除的话，则输入父节点
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally{
            cf.close();
        }
    }

    /**
     * 事务  悲观锁
     * @param cf
     */
    private static void transaction(CuratorFramework cf){
        try {
            //事务处理， 事务会自动回滚
            Collection<CuratorTransactionResult> results=cf.inTransaction().create().
                    forPath("/node_20")
                    .and().create().forPath("/node_30")
                    .and().delete().forPath("/node_4").and().commit();//若添加一个已存在的节点或删除一个不存在的节点就会造成回滚
            for(CuratorTransactionResult result:results){
                System.out.println(result.getResultStat()+"->"+result.getForPath()+"->"+result.getType());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    /**
     * Curator提供三种监听方式：
     * 1\new watcher；
     * 2\CuratorListener；
     * 3\pathChildCacheListenner NodeCacheListenner TreeCacheListenner
     * @param cf
     */
    private static void listener(CuratorFramework cf){
        try {
            cf.getData().usingWatcher(new CuratorWatcher() {
                @Override
                public void process(WatchedEvent event) throws Exception {
                    System.out.println("触发事件1"+event.getType());
                }
            }).forPath("/node_3"); //通过CuratorWatcher 去监听指定节点的事件， 只监听一次，很少用此方式！

        } catch (Exception e) {
            e.printStackTrace();
        }finally {
           // cf.close(); //不能关闭，否则无法触发
        }
    }

    /**
     * 全局监听
     * @param cf
     */
    private static void listener2(CuratorFramework cf){
        try {
            cf.getCuratorListenable().addListener(new CuratorListener() {
                @Override
                public void eventReceived(CuratorFramework curatorFramework, CuratorEvent event) throws Exception {
                    System.out.println("触发事件2"+event.getType());
                }
            });

        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            // cf.close(); //不能关闭，否则无法触发
        }
    }

    //比用usingWatch好在可多次监听
    private static void listener3(CuratorFramework cf) throws Exception {
        //子节点监听
        PathChildrenCache childrenCache=new PathChildrenCache(cf,"/node_3",true);
        //NodeCache nodeCache;
        childrenCache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);

        childrenCache.getListenable().addListener(new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                System.out.println(event.getType()+"事件监听3");
            }
        });
    }
}
