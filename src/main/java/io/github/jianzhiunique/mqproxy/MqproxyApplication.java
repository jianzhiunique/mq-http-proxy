package io.github.jianzhiunique.mqproxy;

import io.github.jianzhiunique.mqproxy.util.IpUtil;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.RejectedExecutionHandler;
import io.netty.util.concurrent.SingleThreadEventExecutor;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

@SpringBootApplication
public class MqproxyApplication implements ApplicationRunner {

    // proxy type
    //public static ProxyType proxyType;

    public static String ip;
    public static String hostname;
    public static String port = "8080";
    public static String serviceInfo;
    public static String serviceUrl;

    // is proxy stop
    public final static AtomicBoolean isStop = new AtomicBoolean(false);

    // default task executor group
    public final static DefaultEventExecutorGroup defaultEventExecutorGroup = new DefaultEventExecutorGroup(16, (ThreadFactory) null, 2147483647, new RejectedExecutionHandler() {
        @Override
        public void rejected(Runnable runnable, SingleThreadEventExecutor singleThreadEventExecutor) {
            runnable.run();
        }
    });

    public static void main(String[] args) {
        SpringApplication.run(MqproxyApplication.class, args);
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        // use --proxy.type=consumer to specify proxy type
        /*if (args.containsOption("proxy.type")) {
            if (args.getOptionValues("proxy.type").contains("producer")) {
                proxyType = ProxyType.PRODUCER;
            } else if (args.getOptionValues("proxy.type").contains("consumer")) {
                proxyType = ProxyType.CONSUMER;
            } else {
                throw new Exception("invalid proxy type");
            }
        } else {
            proxyType = ProxyType.PRODUCER;
        }*/

        //init ip hostname
        ip = IpUtil.getIp();
        hostname = IpUtil.getHostname();
        if (args.containsOption("server.port")) {
            port = args.getOptionValues("server.port").get(0);
        }
        serviceUrl = IpUtil.getIp() + ":" + port;
        serviceInfo = hostname + "-" + serviceUrl;
    }
}
