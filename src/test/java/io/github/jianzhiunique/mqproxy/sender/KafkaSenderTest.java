package io.github.jianzhiunique.mqproxy.sender;

import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

class KafkaSenderTest {

    @Test
    void send() throws Exception {
        HashedWheelTimer wheelTimer = new HashedWheelTimer();
        wheelTimer.newTimeout(new TimerTask() {
            @Override
            public void run(Timeout timeout) throws Exception {
                System.out.println("1s");
            }
        }, 1, TimeUnit.SECONDS);

        wheelTimer.newTimeout(new TimerTask() {
            @Override
            public void run(Timeout timeout) throws Exception {
                System.out.println("3s");
            }
        }, 3, TimeUnit.SECONDS);

        wheelTimer.start();


        int i = 1;
        while (i < 1000) {
            Thread.sleep(5);
            i++;
        }


    }
}