package io.github.jianzhiunique.mqproxy.helper;

import lombok.Data;
import lombok.Synchronized;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

@Data
public class UnsafeMessages {
    // committing offsets
    private List<CommitSection> committing = new LinkedList<>();

    // in flight messages and their next available time
    private ConcurrentHashMap<String, InflightMessage> inflight = new ConcurrentHashMap<>();

    @Synchronized
    public void merge() {
        Collections.sort(committing);
        LinkedList<CommitSection> merged = new LinkedList<CommitSection>();
        for (CommitSection commit : committing) {
            // if the list of merged intervals is empty or if the current
            // interval does not overlap with the previous, simply append it.
            if (merged.isEmpty() || merged.getLast().getRight() < commit.getLeft()) {
                merged.add(commit);
            }
            // otherwise, there is overlap, so we merge the current and previous
            // intervals.
            else {
                merged.getLast().setRight(Math.max(merged.getLast().getRight(), commit.getRight()));
            }
        }

        committing = merged;
    }
}
