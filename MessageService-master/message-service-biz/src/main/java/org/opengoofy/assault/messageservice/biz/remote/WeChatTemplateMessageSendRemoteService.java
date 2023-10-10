package org.opengoofy.assault.messageservice.biz.remote;

import org.opengoofy.assault.messageservice.biz.remote.dto.WeChatTemplateMessageRemoteReqDTO;
import org.opengoofy.assault.messageservice.biz.remote.dto.WeChatTemplateMessageRemoteRespDTO;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;

/**
 * 微信模板消息发送远程调用服务
 *
 * @author chen.ma
 * @github https://github.com/opengoofy
 */
@FeignClient(value = "weChatTemplateMessageSendRemoteService", url = "https://api.weixin.qq.com")
public interface WeChatTemplateMessageSendRemoteService {
    
    /**
     * 微信模板消息发送
     */
    @PostMapping(value = "/cgi-bin/message/template/send")
    WeChatTemplateMessageRemoteRespDTO sendMessage(@RequestParam("access_token") String access_token, @RequestBody WeChatTemplateMessageRemoteReqDTO requestParam);
}
