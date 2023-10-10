package org.opengoofy.assault.messageservice.web;

import lombok.RequiredArgsConstructor;
import org.opengoofy.assault.framework.starter.convention.result.Result;
import org.opengoofy.assault.framework.starter.web.Results;
import org.opengoofy.assault.messageservice.api.dto.MessageQueryRequestDTO;
import org.opengoofy.assault.messageservice.api.dto.MessageQueryResponseDTO;
import org.opengoofy.assault.messageservice.api.dto.MessageSendRequestDTO;
import org.opengoofy.assault.messageservice.api.dto.MessageSendResponseDTO;
import org.opengoofy.assault.messageservice.biz.service.MessageQueryService;
import org.opengoofy.assault.messageservice.biz.service.MessageSendService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * 消息发送控制层
 *
 * @author chen.ma
 * @github https://github.com/opengoofy
 */
@RestController
@RequiredArgsConstructor
public class MessageSendController {
    
    private final MessageSendService messageSendService;
    private final MessageQueryService messageQueryService;
    
    /**
     * 消息异步发送
     */
    @PostMapping("/api/message-service/v1/message/send")
    public Result<MessageSendResponseDTO> messageSend(@RequestBody MessageSendRequestDTO requestParam) {
        MessageSendResponseDTO result = messageSendService.messageSend(requestParam);
        return Results.success(result);
    }
    
    /**
     * 消息同步发送
     */
    @PostMapping("/api/message-service/v1/sync/message/send")
    public Result<MessageSendResponseDTO> syncMessageSend(@RequestBody MessageSendRequestDTO requestParam) {
        MessageSendResponseDTO result = messageSendService.syncMessageSend(requestParam);
        return Results.success(result);
    }
    
    /**
     * 消息查询
     */
    @GetMapping("/api/message-service/v1/message/query")
    public Result<List<MessageQueryResponseDTO>> messageQuery(MessageQueryRequestDTO requestParam) {
        return Results.success(messageQueryService.messageQuery(requestParam));
    }
}
