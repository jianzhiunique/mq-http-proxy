package io.github.jianzhiunique.mqproxy.helper;

import org.junit.jupiter.api.Test;

class UnsafeMessagesTest {

    @Test
    void merge() {
        UnsafeMessages unsafeMessages = new UnsafeMessages();
        CommitSection commitSection;

        commitSection = new CommitSection(1, 2, "test", 1);
        unsafeMessages.getCommitting().add(commitSection);

        commitSection = new CommitSection(4, 7, "test", 1);
        unsafeMessages.getCommitting().add(commitSection);

        commitSection = new CommitSection(6, 9, "test", 1);
        unsafeMessages.getCommitting().add(commitSection);


        commitSection = new CommitSection(11, 8, "test", 1);
        unsafeMessages.getCommitting().add(commitSection);


        commitSection = new CommitSection(1, 2, "test", 1);
        unsafeMessages.getCommitting().add(commitSection);

        commitSection = new CommitSection(1, 1, "test", 1);
        unsafeMessages.getCommitting().add(commitSection);

        unsafeMessages.merge();

        System.out.println(unsafeMessages.getCommitting());

    }

}