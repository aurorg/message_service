package org.opengoofy.assault.messageservice.biz.mq.consumer;

import com.alibaba.fastjson2.JSON;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.opengoofy.assault.framework.starter.common.toolkit.BeanUtil;
import org.opengoofy.assault.framework.starter.idempotent.annotation.Idempotent;
import org.opengoofy.assault.framework.starter.idempotent.enums.IdempotentSceneEnum;
import org.opengoofy.assault.framework.starter.idempotent.enums.IdempotentTypeEnum;
import org.opengoofy.assault.messageservice.api.dto.MessageSendRequestDTO;
import org.opengoofy.assault.messageservice.biz.common.MessageRocketMQConstants;
import org.opengoofy.assault.messageservice.biz.common.MessageSendStatusEnum;
import org.opengoofy.assault.messageservice.biz.dao.entity.SendRecordDO;
import org.opengoofy.assault.messageservice.biz.dao.entity.SendRecordExtendDO;
import org.opengoofy.assault.messageservice.biz.dao.mapper.SendRecordExtendMapper;
import org.opengoofy.assault.messageservice.biz.dao.mapper.SendRecordMapper;
import org.opengoofy.assault.messageservice.biz.dto.MessagePlatformSendResponseDTO;
import org.opengoofy.assault.messageservice.biz.mq.event.MessageSaveEvent;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.Date;

import static org.opengoofy.assault.messageservice.biz.common.MessageConstants.SMS_MESSAGE_CHANNELS;

/**
 * 公共消息保存消费者，包括：短信、微信、企业微信、邮箱等
 *
 * RocketMQ消息消费者的处理方法，用于处理消息发送成功后的保存操作
 *
 * @author chen.ma
 * @github https://github.com/opengoofy
 */
@Slf4j
@Component
@RequiredArgsConstructor
@RocketMQMessageListener(
        topic = MessageRocketMQConstants.MESSAGE_COMMON_TOPIC, // 指定监听的RocketMQ主题
        selectorExpression = MessageRocketMQConstants.COMMON_MESSAGE_SAVE_TAG,  // 指定消息的标签
        consumerGroup = MessageRocketMQConstants.COMMON_MESSAGE_SAVE_CG // 指定消费者组
)

/**
 * 消息接收及处理方法
 *
 * @param messageSaveEvent 消息保存事件对象
 */
public class CommonMessageSaveConsumer implements RocketMQListener<MessageSaveEvent> {
    
    private final SendRecordMapper sendRecordMapper;
    private final SendRecordExtendMapper sendRecordExtendMapper;
    
    @Idempotent(
            uniqueKeyPrefix = "common_message_save:", // 幂等性键的前缀
            key = "#messageSaveEvent.msgId+'_'+#messageSaveEvent.hashCode()",    // 幂等性键的SpEL表达式
            type = IdempotentTypeEnum.SPEL, // 幂等性键的类型，这里是SpEL表达式
            scene = IdempotentSceneEnum.MQ,  // 幂等性场景，这里是MQ场景
            keyTimeout = 7200L // 幂等性键的过期时间，单位为秒
    )
    @Transactional(rollbackFor = Exception.class)  // 声明事务，确保数据库操作的一致性
    @Override
    public void onMessage(MessageSaveEvent messageSaveEvent) {
        try {
            // 获取消息发送请求对象
            MessageSendRequestDTO messageSendRequest = messageSaveEvent.getMessageSendRequest();

            //获取消息发送平台的响应对象
            MessagePlatformSendResponseDTO platformSendResponse = messageSaveEvent.getMessagePlatformSendResponse();

            // 组装短信发送记录持久层实体 & 短信参数持久层实体
            // 转换消息发送请求对象为发送记录实体
            SendRecordDO sendRecordDO = BeanUtil.convert(messageSendRequest, SendRecordDO.class);
            sendRecordDO.setSender(messageSaveEvent.getCurrentSendChannel()); // 设置消息发送渠道
            sendRecordDO.setMsgId(messageSaveEvent.getMsgId()); // 设置消息ID
            SendRecordExtendDO sendRecordExtendDO = SendRecordExtendDO.builder()
                    .msgId(messageSaveEvent.getMsgId())
                    .msgParam(messageSendRequest.getParamList().toString())
                    .build();  // 构建消息参数的扩展信息实体
            // 调用失败，记录调用三方平台失败信息
            if (platformSendResponse != null && !platformSendResponse.getSuccess()) {
                int failStatus = SMS_MESSAGE_CHANNELS.contains(messageSendRequest.getMsgType()) ? MessageSendStatusEnum.SUBMIT_FAIL.getCode() : MessageSendStatusEnum.SEND_FAIL.getCode();
                sendRecordDO.setStatus(failStatus);// 设置发送状态为失败
                sendRecordDO.setFailInfo(JSON.toJSONString(platformSendResponse)); // 记录失败信息
            } else {
                int successStatus = SMS_MESSAGE_CHANNELS.contains(messageSendRequest.getMsgType()) ? MessageSendStatusEnum.SEND_PROGRESS.getCode() : MessageSendStatusEnum.SEND_SUCCESS.getCode();
                sendRecordDO.setStatus(successStatus);// 设置发送状态为成功
            }
            sendRecordDO.setSendTime(new Date());  // 设置发送时间
            try {
                sendRecordMapper.insert(sendRecordDO);  // 将发送记录实体插入数据库
                sendRecordExtendMapper.insert(sendRecordExtendDO);  // 将消息参数的扩展信息实体插入数据库
            } catch (Exception ex) {
                log.error("保存消息发送&参数记录错误，错误信息：{}", ex.getMessage());
            }
        } catch (Throwable ex) {
            log.error("消息入库流程执行失败", ex);
        }
    }
}
