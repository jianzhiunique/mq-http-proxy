package io.github.jianzhiunique.mqproxy.fetcher;

import io.github.jianzhiunique.mqproxy.helper.FetchData;
import io.github.jianzhiunique.mqproxy.message.Message;

import java.util.List;

public interface MqFetcher {
    List<Message> get(FetchData fetchData) throws Exception;
}
