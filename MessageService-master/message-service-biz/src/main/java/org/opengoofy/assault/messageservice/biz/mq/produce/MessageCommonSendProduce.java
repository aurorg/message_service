/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opengoofy.assault.messageservice.biz.mq.produce;

import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson2.JSON;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.opengoofy.assault.messageservice.biz.common.MessageRocketMQConstants;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import java.util.UUID;

import static org.opengoofy.assault.messageservice.biz.common.MessageRocketMQConstants.MESSAGE_COMMON_TOPIC;

/**
 * 消息发送生产者
 *
 * @author chen.ma
 * @github https://github.com/opengoofy
 */
@Slf4j
@Component
@AllArgsConstructor
public class MessageCommonSendProduce {
    
    private final RocketMQTemplate rocketMQTemplate;

    /**
     * 消息发送，默认使用通用的Topic {@link MessageRocketMQConstants#MESSAGE_COMMON_TOPIC}
     * 该方法用于将消息发送到通用的Topic中。
     *
     * @param messageSendEvent 待发送的消息事件对象
     * @param keys             消息的唯一标识，如果为空则生成一个UUID作为标识
     * @param tag              消息的标签，用于消息的过滤和分类
     */
    public void send(Object messageSendEvent, String keys, String tag) {
        send(messageSendEvent, MESSAGE_COMMON_TOPIC, keys, tag);
    }

    /**
     * 消息发送，可以指定Topic
     * 该方法用于将消息发送到指定的Topic中。
     *
     * @param messageSendEvent 待发送的消息事件对象
     * @param topic            指定的消息Topic
     * @param keys             消息的唯一标识，如果为空则生成一个UUID作为标识
     * @param tag              消息的标签，用于消息的过滤和分类
     */
    public void send(Object messageSendEvent, String topic, String keys, String tag) {
        //如果key为空，生成一个uuid作为消息的唯一标识
        keys = StrUtil.isEmpty(keys) ? UUID.randomUUID().toString() : keys;

        //构建消息对象
        Message<?> message = MessageBuilder
                .withPayload(messageSendEvent)
                .setHeader(MessageConst.PROPERTY_KEYS, keys)
                .build();
        try {
            //构建topic和tag的组合字符串
            String topicAndTag = StrUtil.builder()
                    .append(topic)
                    .append(":")
                    .append(tag)
                    .toString();

            // 同步发送消息，并获取发送结果
            SendResult sendResult = rocketMQTemplate.syncSend(topicAndTag, message, 2000);
            log.info("消息发送结果：{}，消息ID：{}，消息Keys：{}", sendResult.getSendStatus(), sendResult.getMsgId(), keys);
        } catch (Throwable ex) {
            log.error("消息发送失败，消息体：{}", JSON.toJSONString(messageSendEvent), ex);
        }
    }
}
