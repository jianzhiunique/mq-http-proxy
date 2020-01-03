package io.github.jianzhiunique.mqproxy.helper;

import lombok.Data;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Data
public class Position {
    private AtomicLong fetchedOffset = new AtomicLong(-1);

    private AtomicLong returnedOffset = new AtomicLong(-1);

    private AtomicLong commitOffset = new AtomicLong(-1);

    private ConcurrentHashMap<String, Long> skipOffset = new ConcurrentHashMap<>();
}
