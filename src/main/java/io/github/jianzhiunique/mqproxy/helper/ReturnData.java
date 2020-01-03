package io.github.jianzhiunique.mqproxy.helper;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class ReturnData {
    private int code;
    private Object data;
    private String msg;
}
