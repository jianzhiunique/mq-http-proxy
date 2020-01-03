package io.github.jianzhiunique.mqproxy.helper;

import lombok.Data;

/**
 * commit offset range
 */
@Data
public class CommitSection implements Comparable<CommitSection> {
    private long left;
    private long right;
    private String topic;
    private int partition;

    public CommitSection(long left, long right, String topic, int partition) {
        if (left <= right) {
            this.left = left;
            this.right = right;
        } else {
            this.right = left;
            this.left = right;
        }
        this.topic = topic;
        this.partition = partition;
    }

    @Override
    public int compareTo(CommitSection o) {
        if (this.getLeft() > o.getLeft()) {
            return 1;
        } else if (this.getLeft() == o.getLeft()) {
            return 0;
        } else {
            return -1;
        }
    }
}
