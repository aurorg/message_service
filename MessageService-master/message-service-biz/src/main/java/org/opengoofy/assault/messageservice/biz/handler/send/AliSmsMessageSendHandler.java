package org.opengoofy.assault.messageservice.biz.handler.send;

import com.alibaba.fastjson2.JSON;
import com.aliyun.dysmsapi20170525.Client;
import com.aliyun.dysmsapi20170525.models.SendSmsRequest;
import com.aliyun.dysmsapi20170525.models.SendSmsResponse;
import com.aliyun.dysmsapi20170525.models.SendSmsResponseBody;
import com.aliyun.teaopenapi.models.Config;
import com.aliyun.teautil.models.RuntimeOptions;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.opengoofy.assault.messageservice.api.dto.MessageSendRequestDTO;
import org.opengoofy.assault.messageservice.biz.dto.MessagePlatformSendResponseDTO;
import org.opengoofy.assault.messageservice.biz.handler.send.base.AbstractMessageSendService;
import org.opengoofy.assault.messageservice.biz.handler.send.base.MessageSendService;
import org.opengoofy.assault.messageservice.biz.mq.event.MessageSendEvent;
import org.springframework.stereotype.Component;

import java.util.Objects;

import static org.opengoofy.assault.messageservice.biz.common.MessageConstants.SMS_MESSAGE_KEY;

/**
 * 阿里云短信发送组件
 */
@Slf4j
@Component
public class AliSmsMessageSendHandler extends AbstractMessageSendService implements MessageSendService {
    
    /**
     * 调用阿里云发送短信成功返回标识
     */
    private final static String RETURN_SUCCESS_FLAG = "OK";
    
    @Override
    public String mark() {
        return SMS_MESSAGE_KEY + "ALI";
    }
    
    @SneakyThrows
    public MessagePlatformSendResponseDTO executeResp(MessageSendEvent messageSendEvent) {
        // 获取消息发送请求的信息
        MessageSendRequestDTO messageSendRequest = messageSendEvent.getMessageSendRequest();

        // 配置阿里云短信客户端信息
        Config config = new Config()
                .setAccessKeyId("xxx")
                .setAccessKeySecret("xxx")
                .setEndpoint("");
        Client client = new Client(config);

        // 构造短信发送请求
        SendSmsRequest sendSmsRequest = new SendSmsRequest()
                .setOutId(messageSendEvent.getMsgId())// 设置外部流水号
                .setPhoneNumbers(messageSendRequest.getReceiver())// 设置接收短信的手机号
                .setTemplateCode("xxx")// 设置短信模板Code
                .setTemplateParam("xxx") // 设置短信模板参数
                .setSignName("xxx");  // 设置短信签名
        SendSmsResponse sendSmsResponse;
        MessagePlatformSendResponseDTO responseDTO = null;
        try {
            // 发送短信
            sendSmsResponse = client.sendSmsWithOptions(sendSmsRequest, new RuntimeOptions());
            SendSmsResponseBody body = sendSmsResponse.getBody();
            // 判断短信发送是否成功
            if (!Objects.equals(body.getCode(), RETURN_SUCCESS_FLAG)) {
                // 构造短信发送响应
                responseDTO = MessagePlatformSendResponseDTO.builder().code(body.getCode()).errMsg(body.getMessage()).success(false).build();
            }
        } catch (Throwable ex) {
            // 捕获发送短信过程中的异常
            log.error("阿里云短信调用失败，入参：{}，错误信息：{}", JSON.toJSONString(messageSendEvent), ex.getMessage());
            // 构造短信发送响应
            responseDTO = MessagePlatformSendResponseDTO.builder().code("-1").errMsg(ex.getMessage()).success(false).build();
        }
        // 返回短信发送响应
        return responseDTO;
    }
}
