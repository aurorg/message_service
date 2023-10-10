package org.opengoofy.assault.messageservice.biz.mq.consumer;

import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSON;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import lombok.extern.slf4j.Slf4j;
import org.opengoofy.assault.framework.starter.cache.DistributedCache;
import org.opengoofy.assault.messageservice.api.dto.MQCallBackTransferDTO;
import org.opengoofy.assault.messageservice.api.dto.MQCallbackDTO;
import org.opengoofy.assault.messageservice.biz.base.message.MessageWrapper;
import org.opengoofy.assault.messageservice.biz.common.MessageRocketMQConstants;
import org.opengoofy.assault.messageservice.biz.dao.entity.TemplateConfigDO;
import org.opengoofy.assault.messageservice.biz.dao.mapper.TemplateConfigMapper;
import org.opengoofy.assault.messageservice.biz.dto.MessagePlatformSendResponseDTO;
import org.opengoofy.assault.messageservice.biz.handler.select.MessageSendChannelSelector;
import org.opengoofy.assault.messageservice.biz.handler.send.base.MessageSendService;
import org.opengoofy.assault.messageservice.biz.mq.event.MessageSaveEvent;
import org.opengoofy.assault.messageservice.biz.mq.event.MessageSendEvent;
import org.opengoofy.assault.messageservice.biz.mq.produce.MessageCommonSendProduce;

import javax.annotation.Resource;
import java.util.Objects;
import java.util.Optional;

import static org.opengoofy.assault.messageservice.biz.common.MessageCacheConstants.DEFAULT_CACHE_TIMOUT;
import static org.opengoofy.assault.messageservice.biz.common.MessageCacheConstants.MESSAGE_TEMPLATE_CACHE_PREFIX_KEY;
import static org.opengoofy.assault.messageservice.biz.common.MessageRocketMQConstants.CALLBACK_MESSAGE_SEND_TAG_TEMPLATE;
import static org.opengoofy.assault.messageservice.biz.common.MessageRocketMQConstants.MESSAGE_CALLBACK_TOPIC;

/**
 * 消息发送消费者抽象
 *
 * @author chen.ma
 * @github https://github.com/opengoofy
 */

/*
 * 消息发送消费者抽象
 *
 * 该类负责处理消息发送相关的逻辑，包括选择发送通道、发送消息到用户、处理消息发送结果回调和保存发送结果等。
 * 具体的消息发送服务类需要继承该抽象类，实现消息发送的具体逻辑。
 * 该类将消息发送的通用逻辑进行封装，提高了代码的复用性和可维护性。
 */
@Slf4j
public abstract class AbstractMessageSendConsumer {
    
    @Resource
    private MessageCommonSendProduce messageCommonSendProduce;
    @Resource
    private MessageSendChannelSelector messageSendChannelSelector;
    @Resource
    private DistributedCache distributedCache;
    @Resource
    private TemplateConfigMapper templateConfigMapper;


    /**
     * 消息发送入口方法
     * 消息发送，通过抽象类复用消息发送相关代码
     *
     * @param messageSendEvent 包含要发送的消息的信息的事件对象
     */
    public void sendMessage(MessageSendEvent messageSendEvent) {
        MessagePlatformSendResponseDTO sendResponse = null;
        String templateId = messageSendEvent.getMessageSendRequest().getTemplateId();
        try {
            //从缓存中获取短信模版匹配信息
            TemplateConfigDO smsMessageTemplates = distributedCache.safeGet(
                    MESSAGE_TEMPLATE_CACHE_PREFIX_KEY + templateId,
                    TemplateConfigDO.class,
                    () -> {
                        //如果缓存中没有，从数据库中获取并且放入缓存
                        TemplateConfigDO templateConfigDO = templateConfigMapper.selectOne(
                                Wrappers.lambdaQuery(TemplateConfigDO.class).eq(TemplateConfigDO::getTemplateId, templateId)
                        );
                        return templateConfigDO;
                    },
                    new Long(DEFAULT_CACHE_TIMOUT)
            );

            //将短信模版中的可选渠道拆分为列表，以备后续选择具体发送渠道时使用
            messageSendEvent.setSmsOptionalChannels(StrUtil.split(smsMessageTemplates.getChannelIds(), ","));

            // 选择发送消息具体实现
            MessageSendService messageSendService = messageSendChannelSelector.select(messageSendEvent);

            // 根据消息发送器发送消息到用户
            sendResponse = messageSendService.send(messageSendEvent);
        } catch (Throwable ex) {
            log.error("发送消息流程执行失败，消息入参：{}", JSON.toJSONString(messageSendEvent), ex);
        }
        // 通过 MQ 触发客户端消息发送结果回调
        mqAsyncCallback(messageSendEvent, sendResponse);
        // 消息发送后，保存发送结果到数据库。为什么还要发一个 MQ？
        // 1. 这样可以提高短信发送的吞吐量，使验证码短信更快让用户接到
        // 2. 减轻数据库操作压力，因为当前流程是通过线程池执行的，并发压力较大
        mqAsyncSendSaveMessage(messageSendEvent, sendResponse);
    }


    /**
     * 异步触发客户端消息发送结果回调
     *
     * @param messageSendEvent 发送消息的事件对象
     * @param sendResponse     消息发送的结果
     */
    private void mqAsyncCallback(MessageSendEvent messageSendEvent, MessagePlatformSendResponseDTO sendResponse) {
        try {

            //获取消息发送请求中的消息回调配置
            MQCallbackDTO mqCallback = messageSendEvent.getMessageSendRequest().getMqCallback();

            //如果没有消息回调配置，直接返回
            if (mqCallback == null) {
                return;
            }

            //将回调类型转换为小写
            String mqCallBackTypes = mqCallback.getType().toLowerCase();

            //判断是否执行失败
            boolean executeFail = sendResponse == null || sendResponse.getSuccess();

            //判断是否满足回调条件（根据回调类型和消息发送结果判断）
            boolean mqCallBackSendFlag = (Objects.equals(mqCallBackTypes, "all")
                    || (Objects.equals(mqCallBackTypes, "success") && !executeFail)
                    || (Objects.equals(mqCallBackTypes, "fail") && executeFail));

            //如果满足回调消息
            if (mqCallBackSendFlag) {
                //构建消息发送结果回调对象
                MQCallBackTransferDTO mqCallBackTransfer = MQCallBackTransferDTO.builder()
                        .errorMsg(executeFail ? Optional.ofNullable(sendResponse).map(each -> each.getErrMsg()).orElse("Send message process execution failed") : null)
                        .success(!executeFail)
                        .msgId(messageSendEvent.getMsgId())
                        .messageSendRequest(messageSendEvent.getMessageSendRequest())
                        .build();

                //封装消息发送结果回调对象为消息包装体
                MessageWrapper messageWrapper = new MessageWrapper(messageSendEvent.getMsgId(), mqCallBackTransfer);

                //发送消息发送结果回调消息到消息队列中
                messageCommonSendProduce.send(messageWrapper, MESSAGE_CALLBACK_TOPIC, messageWrapper.getKeys(), String.format(CALLBACK_MESSAGE_SEND_TAG_TEMPLATE, mqCallback.getServiceName(), mqCallback.getBizScene()));
            }
        } catch (Throwable ex) {
            log.error("MQ异步回调消息发送结果失败", ex);
        }
    }


    /**
     * 异步发送保存消息结果到数据库
     *
     * @param messageSendEvent 发送消息的事件对象
     * @param sendResponse 消息发送的结果
     */
    private void mqAsyncSendSaveMessage(MessageSendEvent messageSendEvent, MessagePlatformSendResponseDTO sendResponse) {
        try {
            //构建保存消息发送结果的事件对象
            MessageSaveEvent messageSaveEvent = MessageSaveEvent.builder()
                    .messagePlatformSendResponse(sendResponse)
                    .messageSendRequest(messageSendEvent.getMessageSendRequest())
                    .msgId(messageSendEvent.getMsgId())
                    .currentSendChannel(messageSendEvent.getCurrentSendChannel())
                    .build();

            //将保存消息发送结果的事件对象发送到消息队列
            messageCommonSendProduce.send(messageSaveEvent, messageSaveEvent.getMsgId(), MessageRocketMQConstants.COMMON_MESSAGE_SAVE_TAG);
        } catch (Throwable ex) {
            log.error("MQ异步发送保存消息失败", ex);
        }
    }
}
