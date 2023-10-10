package org.opengoofy.assault.messageservice.biz.mq.consumer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.opengoofy.assault.framework.starter.idempotent.annotation.Idempotent;
import org.opengoofy.assault.framework.starter.idempotent.enums.IdempotentSceneEnum;
import org.opengoofy.assault.framework.starter.idempotent.enums.IdempotentTypeEnum;
import org.opengoofy.assault.messageservice.biz.common.MessageRocketMQConstants;
import org.opengoofy.assault.messageservice.biz.mq.event.MessageSendEvent;
import org.springframework.stereotype.Component;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 其它消息发送消费者
 *该类是处理其它类型消息发送的消息消费者
 *
 * @author chen.ma
 * @github https://github.com/opengoofy
 */
@Slf4j
@Component
@RequiredArgsConstructor
@RocketMQMessageListener(
        topic = MessageRocketMQConstants.MESSAGE_COMMON_TOPIC,
        selectorExpression = MessageRocketMQConstants.OTHER_MESSAGE_SEND_TAG,
        consumerGroup = MessageRocketMQConstants.OTHER_MESSAGE_SEND_CG
)
public class OtherMessageSendConsumer extends AbstractMessageSendConsumer implements RocketMQListener<MessageSendEvent> {

    //线程池，用于处理其他类型消息的消费
    private final ThreadPoolExecutor otherMessageConsumeDynamicExecutor;


    /**
     * 消息处理方法，当有其它类型消息到来时，将消息交给线程池异步处理。
     * 方法上使用了@Idempotent注解，确保消息的幂等性。
     *
     * @param messageSendEvent 待处理的消息事件对象
     */
    @Idempotent(
            uniqueKeyPrefix = "other_message_send:",
            key = "#messageSendEvent.msgId+'_'+#messageSendEvent.hashCode()",
            type = IdempotentTypeEnum.SPEL,
            scene = IdempotentSceneEnum.MQ,
            keyTimeout = 7200L
    )
    @Override
    public void onMessage(MessageSendEvent messageSendEvent) {
        //将消息处理任务交给线程池异步处理
        otherMessageConsumeDynamicExecutor.execute(() -> sendMessage(messageSendEvent));
    }
}
