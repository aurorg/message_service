package org.opengoofy.assault.messageservice.biz.job.receipt.impl;

import lombok.extern.slf4j.Slf4j;
import org.opengoofy.assault.messageservice.biz.job.receipt.AbstractSmsMessageReceiptTemplate;
import org.opengoofy.assault.messageservice.biz.job.receipt.MessageReceiptDTO;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 腾讯短信回执拉取执行器
 * <p>
 * 引用自腾讯云官方网址 https://cloud.tencent.com/document/product/382/55977
 *
 * @author chen.ma
 * @github https://github.com/opengoofy
 */
@Slf4j
@Component
public class TencentSmsMessageReceiptExecutor extends AbstractSmsMessageReceiptTemplate {
    
    @Override
    protected List<MessageReceiptDTO> listReceipt() {
        return null;
    }
    
    @Override
    protected List<MessageReceiptDTO> convert(List<?> originalList) {
        return null;
    }
}
