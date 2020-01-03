package io.github.jianzhiunique.mqproxy.util;

import java.net.InetAddress;

/**
 * 获取机器IP和hostname的工具类
 * only for linux
 */
public class IpUtil {
    public static String getIp() throws Exception {
        InetAddress address = InetAddress.getLocalHost();
        return address.getHostAddress();
    }

    public static String getHostname() throws Exception {
        InetAddress address = InetAddress.getLocalHost();
        return address.getHostName();
    }
}
