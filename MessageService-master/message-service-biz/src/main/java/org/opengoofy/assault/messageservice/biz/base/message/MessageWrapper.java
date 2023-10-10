package org.opengoofy.assault.messageservice.biz.base.message;

import cn.hutool.core.util.IdUtil;
import lombok.Data;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.io.Serializable;

/**
 * 消息体包装
 *
 * @author chen.ma
 * @github https://github.com/opengoofy
 */
@Data
@RequiredArgsConstructor
public class MessageWrapper<T> implements Serializable { //Serializable接口表示该类的对象可以被序列化，即可以在网络中传输或者持久化到磁盘
    
    private String uuid = IdUtil.randomUUID();
    
    private String traceId; //追踪标识符
    
    private Long timestamp = System.currentTimeMillis(); //时间戳
    
    @NonNull
    private String keys; //关键字
    
    @NonNull
    private T data;  //消息数据
}
/*
MessageWrapper类提供了一个通用的消息包装结构，
其中包含了消息的唯一标识符、追踪标识符、时间戳、关键字和实际数据。
这种结构可以用于在分布式系统中传递消息，保证消息的唯一性、时序性和追踪性
 */