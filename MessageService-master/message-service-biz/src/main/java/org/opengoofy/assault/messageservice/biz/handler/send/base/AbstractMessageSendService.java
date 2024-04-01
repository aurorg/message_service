package org.opengoofy.assault.messageservice.biz.handler.send.base;

import cn.hutool.core.collection.CollUtil;
import lombok.extern.slf4j.Slf4j;
import org.opengoofy.assault.messageservice.api.dto.MessageSendRequestDTO;
import org.opengoofy.assault.messageservice.biz.base.strategy.AbstractExecuteStrategy;
import org.opengoofy.assault.messageservice.biz.dto.MessagePlatformSendResponseDTO;
import org.opengoofy.assault.messageservice.biz.handler.select.MessageSendChannelSelector;
import org.opengoofy.assault.messageservice.biz.mq.event.MessageSendEvent;

import javax.annotation.Resource;

import static org.opengoofy.assault.messageservice.biz.common.MessageConstants.SMS_MESSAGE_CHANNELS;

/**
 * 抽象消息发送服务
 */
@Slf4j
public abstract class AbstractMessageSendService implements MessageSendService, AbstractExecuteStrategy<MessageSendEvent, MessagePlatformSendResponseDTO> {

    // 注入消息发送渠道选择器
    @Resource
    private MessageSendChannelSelector messageSendChannelSelector;

    // 实现消息发送的具体逻辑，由子类实现
    @Override
    public MessagePlatformSendResponseDTO send(MessageSendEvent messageSendEvent) throws Exception {
        MessageSendRequestDTO messageSendRequest = messageSendEvent.getMessageSendRequest();

        // 如果是短信消息类型
        if (SMS_MESSAGE_CHANNELS.contains(messageSendRequest.getMsgType())) {

            // 执行具体的消息发送逻辑
            MessagePlatformSendResponseDTO executeResp = executeResp(messageSendEvent);

            // 如果发送失败并且有备选短信渠道可选
            if (!executeResp.getSuccess() && CollUtil.isNotEmpty(messageSendEvent.getSmsOptionalChannels())) {
                // 选择发送消息具体实现
                MessageSendService messageSendService = messageSendChannelSelector.select(messageSendEvent);
                // 重新尝试发送
                executeResp = messageSendService.send(messageSendEvent);
            }
            return executeResp;
        }
        // 对于非短信消息，直接执行具体的消息发送逻辑
        return executeResp(messageSendEvent);
    }
}

/*
主要功能是提供一个通用的消息发送逻辑框架。
子类可以继承这个抽象类，并实现 executeResp(messageSendEvent) 方法，从而完成具体消息发送的逻辑。
这样的设计使得消息发送的策略和具体逻辑能够分离，提高了系统的可维护性和扩展性。
 */
