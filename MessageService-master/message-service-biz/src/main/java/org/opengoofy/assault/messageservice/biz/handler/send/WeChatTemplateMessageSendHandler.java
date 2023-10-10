package org.opengoofy.assault.messageservice.biz.handler.send;

import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.opengoofy.assault.framework.starter.cache.DistributedCache;
import org.opengoofy.assault.messageservice.api.dto.MessageSendRequestDTO;
import org.opengoofy.assault.messageservice.biz.dao.entity.TemplateConfigDO;
import org.opengoofy.assault.messageservice.biz.dao.entity.TemplateConfigParamDO;
import org.opengoofy.assault.messageservice.biz.dao.mapper.TemplateConfigMapper;
import org.opengoofy.assault.messageservice.biz.dao.mapper.TemplateConfigParamMapper;
import org.opengoofy.assault.messageservice.biz.dto.MessagePlatformSendResponseDTO;
import org.opengoofy.assault.messageservice.biz.handler.send.base.AbstractMessageSendService;
import org.opengoofy.assault.messageservice.biz.handler.send.base.MessageSendService;
import org.opengoofy.assault.messageservice.biz.mq.event.MessageSendEvent;
import org.opengoofy.assault.messageservice.biz.remote.WeChatTemplateMessageSendRemoteService;
import org.opengoofy.assault.messageservice.biz.remote.dto.WeChatTemplateMessageRemoteReqDTO;
import org.opengoofy.assault.messageservice.biz.remote.dto.WeChatTemplateMessageRemoteRespDTO;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opengoofy.assault.messageservice.biz.common.MessageCacheConstants.DEFAULT_CACHE_TIMOUT;
import static org.opengoofy.assault.messageservice.biz.common.MessageCacheConstants.MESSAGE_TEMPLATE_CACHE_PREFIX_KEY;
import static org.opengoofy.assault.messageservice.biz.common.MessageCacheConstants.MESSAGE_TEMPLATE_PARAM_CACHE_PREFIX_KEY;
import static org.opengoofy.assault.messageservice.biz.common.MessageTypeEnum.WE_CHART_MESSAGE;

/**
 * 微信模板消息发送组件
 *
 * @author chen.ma
 * @github https://github.com/opengoofy
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class WeChatTemplateMessageSendHandler extends AbstractMessageSendService implements MessageSendService {
    
    private final WeChatTemplateMessageSendRemoteService weChatTemplateMessageSendRemoteService;
    private final DistributedCache distributedCache;
    private final TemplateConfigMapper templateConfigMapper;
    private final TemplateConfigParamMapper templateConfigParamMapper;
    
    @Override
    public String mark() {
        return WE_CHART_MESSAGE.getPlatform();
    }
    
    @Override
    @SneakyThrows
    public MessagePlatformSendResponseDTO executeResp(MessageSendEvent messageSendEvent) {
        // 因为需要有一个认证公众号，才可以开通模板消息，为了避免分支流程扩散，所以涉及账号相关代码，以伪代码提现
        String accessToken = "";
        MessageSendRequestDTO messageSendRequest = messageSendEvent.getMessageSendRequest();

        // 从缓存中获取消息模板配置
        String templateId = messageSendRequest.getTemplateId();
        TemplateConfigDO messageTemplate = distributedCache.safeGet(
                MESSAGE_TEMPLATE_CACHE_PREFIX_KEY + templateId,
                TemplateConfigDO.class,
                () -> {
                    TemplateConfigDO templateConfigDO = templateConfigMapper.selectOne(
                            Wrappers.lambdaQuery(TemplateConfigDO.class).eq(TemplateConfigDO::getTemplateId, templateId)
                    );
                    return templateConfigDO;
                },
                new Long(DEFAULT_CACHE_TIMOUT)
        );

        // 从缓存中获取消息模板参数配置
        List<TemplateConfigParamDO> messageTemplateParams = distributedCache.safeGet(
                MESSAGE_TEMPLATE_PARAM_CACHE_PREFIX_KEY + messageTemplate.getChannelTemplateId(),
                List.class,
                () -> {
                    List<TemplateConfigParamDO> templateConfigParams = templateConfigParamMapper.selectList(
                            Wrappers.lambdaQuery(TemplateConfigParamDO.class)
                                    .eq(TemplateConfigParamDO::getTemplateId, templateId)
                                    .orderByAsc(TemplateConfigParamDO::getRankPlaceholder)
                    );
                    return templateConfigParams;
                },
                new Long(DEFAULT_CACHE_TIMOUT)
        );

        // 构建微信模板消息的数据结构
        Map<String, WeChatTemplateMessageRemoteReqDTO.WeChatTemplateMessageDataDTO> weChatTemplateMessageDataMap = new HashMap<>();
        for (int i = 0; i < messageTemplateParams.size(); i++) {
            TemplateConfigParamDO templateConfigParamDO = messageTemplateParams.get(i);
            weChatTemplateMessageDataMap.put(
                    templateConfigParamDO.getKeyPlaceholder(),
                    WeChatTemplateMessageRemoteReqDTO.WeChatTemplateMessageDataDTO.builder()
                            .value(messageSendRequest.getParamList().get(i))
                            .build()
            );
        }

        // 构建微信模板消息请求对象
        WeChatTemplateMessageRemoteReqDTO remoteRequestParam = WeChatTemplateMessageRemoteReqDTO.builder()
                .template_id(templateId)
                .client_msg_id(messageSendEvent.getMsgId())
                .touser(messageSendRequest.getReceiver())
                .data(weChatTemplateMessageDataMap)
                .build();

        // 调用微信模板消息发送远程服务
        WeChatTemplateMessageRemoteRespDTO remoteResultData;
        try {
            remoteResultData = weChatTemplateMessageSendRemoteService.sendMessage(accessToken, remoteRequestParam);
            // 调用三方服务，因为不确定三方抛出的异常是什么，一定要用 Throwable 捕获
        } catch (Throwable ex) {
            remoteResultData = WeChatTemplateMessageRemoteRespDTO.buildError();
            log.error("调用微信模板消息发送错误", ex);
        }

        // 返回结果对象
        return MessagePlatformSendResponseDTO.builder()
                .success(remoteResultData.isSuccess())
                .code(String.valueOf(remoteResultData.getErrcode()))
                .errMsg(remoteResultData.getErrmsg())
                .build();
    }
}
