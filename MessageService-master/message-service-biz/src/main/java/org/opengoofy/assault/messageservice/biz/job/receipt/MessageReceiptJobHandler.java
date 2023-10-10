package org.opengoofy.assault.messageservice.biz.job.receipt;

import lombok.RequiredArgsConstructor;
import org.opengoofy.assault.framework.starter.convention.result.Result;
import org.opengoofy.assault.framework.starter.web.Results;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.GetMapping;

import java.util.List;

/**
 * 短信回执定时任务
 *
 * @author chen.ma
 * @github https://github.com/opengoofy
 */
@Component
@RequiredArgsConstructor
public class MessageReceiptJobHandler {
    
    private final List<AbstractSmsMessageReceiptTemplate> messageReceiptList;
    
    /**
     * 执行定时任务，此处为了避免多部署个 XXL-Job，以 HTTP 请求代替（正常还是定时任务比如 XXL-Job 执行流程）
     */
    @GetMapping("/api/message-service/job/message-receipt")
    public Result<Void> execute() {
        messageReceiptList.forEach(AbstractSmsMessageReceiptTemplate::execute);
        return Results.success();
    }
}
