package com.dongnao.zookeeper.zkClient;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;

import java.io.IOException;
import java.util.List;

/**
 * @author qingyin
 * @date 2016/12/13
 */
public class ZkClientDemo {

    private static String CONNECT_STRING="192.168.1.104:2181,192.168.1.104:2182,192.168.1.104:2183";

    private static int SESSION_TIMEOUT=3000;

    public static void main(String[] args) {
        //MyZkSerializer扩展标准的序列化，统一读写编码，防止出现乱码
        ZkClient zkClient=new ZkClient(CONNECT_STRING,SESSION_TIMEOUT,SESSION_TIMEOUT,new MyZkSerializer());
        try {
            //可监控当前path的下一级子节点创建和删除操作，下下级及以下节点无法用此监控
            zkClient.subscribeChildChanges("/configuration", new IZkChildListener() {
                @Override
                public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
                    System.out.println("触发事件："+parentPath);
                    for(String str:currentChilds){
                        System.out.println(str);
                    }
                }
            });

//            create(zkClient);
//            update(zkClient);
//            delete(zkClient);
//            subWatch(zkClient);

            System.in.read();//让连接不关闭 等待
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            zkClient.close();
        }
    }
    private static void create(ZkClient zk){
//        zk.create("/node_11/node_11_1/node_11_1_1","abc", CreateMode.PERSISTENT); //创建节点
        zk.createPersistent("/node_11/node_11_1/node_11_1_1",true); //该方法默认递归创建节点，即createParents为true
    }

    private static void update(ZkClient zk){
        zk.writeData("/node_11","zyz");
    }

    private static void delete(ZkClient zk){
//        boolean bool = zk.delete("/node_11/node_11_1/node_11_1_1");//只会删除末级节点
        boolean bool=zk.deleteRecursive("/node_11");//递归删除，path是删除的根节点
        //注意：删除始终返回true
        System.out.println(bool);
    }

    private static void subWatch(ZkClient zk){
        if(!zk.exists("/node_11")) {
            zk.createPersistent("/node_11");
        }

        //数据订阅事件，只能订阅当前path的数据修改和删除情况
        zk.subscribeDataChanges("/node_11", new IZkDataListener() {
            @Override
            public void handleDataChange(String dataPath, Object data) throws Exception {
                System.out.println("触发事件："+dataPath+"->"+data);
            }

            @Override
            public void handleDataDeleted(String dataPath) throws Exception {
                System.out.println("触发删除事件:"+dataPath);
            }
        });

        //如上使用类似如下使用watch，但是比watch更加具体，且zkClient将一次性watcher包装为持久watcher：
        /*
        zk.subscribeDataChanges("/node_11", new Watcher(){
            @Override
            public void process(WatchedEvent event) {
                if(event.getType()== Event.EventType.NodeCreated){

                }else if(event.getType()== Event.EventType.NodeDeleted){

                }else if(event.getType()== Event.EventType.NodeChildrenChanged){

                }else if(event.getType()== Event.EventType.NodeDataChanged){

                }
            }
        });
        */
    }
}
