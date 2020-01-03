package io.github.jianzhiunique.mqproxy.helper;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class InflightMessage {
    private long nextAvailableTime;
    private long retryTimes;
    private int maxConsumeTimes;
}
