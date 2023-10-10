package org.opengoofy.assault.messageservice.biz.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.opengoofy.assault.framework.starter.distributedid.SnowflakeIdUtil;
import org.opengoofy.assault.messageservice.api.dto.MessageSendRequestDTO;
import org.opengoofy.assault.messageservice.api.dto.MessageSendResponseDTO;
import org.opengoofy.assault.messageservice.biz.common.MessageChainMarkEnum;
import org.opengoofy.assault.messageservice.biz.common.MessageRocketMQConstants;
import org.opengoofy.assault.messageservice.biz.common.MessageTypeEnum;
import org.opengoofy.assault.messageservice.biz.handler.filter.base.AbstractChainContext;
import org.opengoofy.assault.messageservice.biz.mq.consumer.OtherMessageSendConsumer;
import org.opengoofy.assault.messageservice.biz.mq.consumer.SmsVerificationMessageSendConsumer;
import org.opengoofy.assault.messageservice.biz.mq.event.MessageSendEvent;
import org.opengoofy.assault.messageservice.biz.mq.produce.MessageCommonSendProduce;
import org.springframework.stereotype.Service;

import java.util.Objects;

/**
 * 消息发送接口实现
 * 该类负责处理消息发送请求，包括消息合法性验证、构建消息发送事件、消息发送及响应。
 *
 * @author chen.ma
 * @github https://github.com/opengoofy
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class MessageSendServiceImpl implements MessageSendService {
    
    private final AbstractChainContext abstractChainContext;
    private final MessageCommonSendProduce messageCommonSendProduce;
    private final OtherMessageSendConsumer otherMessageSendConsumer;
    private final SmsVerificationMessageSendConsumer smsVerificationMessageSendConsumer;


    /**
     * 处理普通消息的发送请求
     *
     * @param requestParam 消息发送请求DTO，包含了消息内容、接收者、消息类型等信息
     * @return 消息发送响应DTO，包含了消息ID
     */
    @Override
    public MessageSendResponseDTO messageSend(MessageSendRequestDTO requestParam) {
        // 责任链模式验证消息发送入参是否合理
        abstractChainContext.handler(MessageChainMarkEnum.MESSAGE_SEND_FILTER.name(), requestParam);

        // 构建消息发送事件
        MessageSendEvent messageSendEvent = buildMessageSendEvent(requestParam);

        // 通过 RocketMQ 削峰消息发送流程，避免应用负载过大
        messageCommonSendProduce.send(messageSendEvent, messageSendEvent.getMsgId(), getTagByMsgType(requestParam));
        // 返回消息发送响应DTO，包含消息ID
        return new MessageSendResponseDTO(messageSendEvent.getMsgId());
    }


    /**
     * 处理同步消息的发送请求
     *
     * @param requestParam 消息发送请求DTO，包含了消息内容、接收者、消息类型等信息
     * @return 消息发送响应DTO，包含了消息ID
     */
    @Override
    public MessageSendResponseDTO syncMessageSend(MessageSendRequestDTO requestParam) {
        // 责任链模式验证消息发送入参是否合理
        abstractChainContext.handler(MessageChainMarkEnum.MESSAGE_SEND_FILTER.name(), requestParam);
        // 构建消息发送事件
        MessageSendEvent messageSendEvent = buildMessageSendEvent(requestParam);
        // 判断是验证码消息还是其它消息
        if (Objects.equals(requestParam.getMsgType(), MessageTypeEnum.SMS_VERIFICATION_MESSAGE.getType())) {
            // 发送验证码消息
            smsVerificationMessageSendConsumer.onMessage(messageSendEvent);
        } else {
            // 发送其它消息
            otherMessageSendConsumer.onMessage(messageSendEvent);
        }
        // 返回消息发送响应DTO，包含消息ID
        return new MessageSendResponseDTO(messageSendEvent.getMsgId());
    }

    /**
     * 构建消息发送事件
     *
     * @param requestParam 消息发送请求DTO，包含了消息内容、接收者、消息类型等信息
     * @return 消息发送事件对象
     */
    private MessageSendEvent buildMessageSendEvent(MessageSendRequestDTO requestParam) {
        // 通过雪花算法生成唯一的分布式消息ID
        String msgId = SnowflakeIdUtil.nextIdStr();
        // 创建消息发送事件对象，包含了消息发送请求DTO和生成的消息ID
        return MessageSendEvent.builder().messageSendRequest(requestParam).msgId(msgId).build();
    }

    /**
     * 根据消息类型获取消息的Tag
     *
     * @param requestParam 消息发送请求DTO，包含了消息内容、接收者、消息类型等信息
     * @return 消息的Tag
     */
    private String getTagByMsgType(MessageSendRequestDTO requestParam) {
        // 获取消息类型
        Integer msgType = requestParam.getMsgType();
        // 判断消息类型并返回对应的Tag
        if (msgType == MessageTypeEnum.SMS_VERIFICATION_MESSAGE.getType()) {
            // 短信验证码消息的Tag
            return MessageRocketMQConstants.SMS_MESSAGE_VERIFICATION_SEND_TAG;
        }
        // 其它消息的Tag
        return MessageRocketMQConstants.OTHER_MESSAGE_SEND_TAG;
    }
}
