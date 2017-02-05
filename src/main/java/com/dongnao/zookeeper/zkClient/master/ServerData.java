package com.dongnao.zookeeper.zkClient.master;

import java.io.Serializable;

/**
 * 每台服务器自身属性
 * @author parker
 * @date 2016/12/15
 */
public class ServerData implements Serializable{

    private int serverId;

    private String serverName;

    public int getServerId() {
        return serverId;
    }

    public void setServerId(int serverId) {
        this.serverId = serverId;
    }

    public String getServerName() {
        return serverName;
    }

    public void setServerName(String serverName) {
        this.serverName = serverName;
    }
}
