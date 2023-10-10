package org.opengoofy.assault.messageservice.biz.job.receipt;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.ObjectUtil;
import com.alibaba.fastjson.JSON;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.opengoofy.assault.messageservice.biz.algorithm.ShardModel;
import org.opengoofy.assault.messageservice.biz.dao.entity.SendRecordDO;
import org.opengoofy.assault.messageservice.biz.dao.mapper.SendRecordMapper;
import org.springframework.beans.factory.DisposableBean;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * 抽象短信回执拉取模板
 *
 * @author chen.ma
 * @github https://github.com/opengoofy
 */
@Slf4j
public abstract class AbstractSmsMessageReceiptTemplate implements DisposableBean {
    
    @Resource
    private ThreadPoolExecutor messageReceiptExecutor;
    
    @Resource
    private SendRecordMapper sendRecordMapper;
    
    private final String TABLE_NAME = "send_record";
    
    private final ReentrantLock reentrantLock = new ReentrantLock();
    
    /**
     * 用于存储未更新回执消息集合
     */
    private final List<MessageReceiptRetry> listReceipts = new ArrayList<>();
    
    /**
     * 获取一批手机号回执
     */
    protected abstract List<MessageReceiptDTO> listReceipt();
    
    /**
     * 三方返回字段转换为标准字段
     */
    protected abstract List<MessageReceiptDTO> convert(List<?> originalList);
    
    @Data
    @AllArgsConstructor
    static class MessageReceiptRetry {
        
        private Integer retryNum;
        
        private List<MessageReceiptDTO> messageReceipts;
    }
    
    public void execute() {
        messageReceiptExecutor.execute(() -> {
            retryFailMessage();
            while (true) {
                // 调用具体实现类获取短信回执记录
                List<MessageReceiptDTO> listReceipt = listReceipt();
                if (CollUtil.isEmpty(listReceipt)) {
                    break;
                }
                /**
                 * 4、取出回执 list 中的 msgId 集合
                 * 5、查询数据库中这些回执集合对应的 count
                 * 6、比对数据库 count 和回执集合 size：不相等则放入常量回执中
                 */
                long count = selectCount(listReceipt);
                if (ObjectUtil.notEqual(listReceipt.size(), count)) {
                    listReceipts.add(new MessageReceiptRetry(0, listReceipt));
                    continue;
                }
                Date receiveTime = listReceipt.get(0).getReceiptTime();
                Set<String> set = ShardModel.calculateRange(TABLE_NAME, DateUtil.offsetDay(receiveTime != null ? receiveTime : new Date(), -5), new Date());
                set.forEach(each -> sendRecordMapper.batchUpdate(listReceipt, each));
            }
        });
    }
    
    /**
     * 避免回执更新时 send_record 表中没有及时存储短信记录
     * 1、先从常量中取出未处理回执 list
     * 2、判断重试次数是否大于 3 次，大于 3 次直接 remove，小于 3 次，批量更新数据库回执信息
     * 3、判断是否全部修改成功，即 size 是否相等，size 相等，直接 remove，不相等，重试次数加 1
     */
    private void retryFailMessage() {
        if (CollUtil.isNotEmpty(listReceipts)) {
            boolean tryLock = reentrantLock.tryLock();
            if (!tryLock) {
                // 获取不到锁，说明已经有一个线程在执行，跳过即可
                return;
            }
            try {
                Iterator<MessageReceiptRetry> iterator = listReceipts.iterator();
                while (iterator.hasNext()) {
                    MessageReceiptRetry receiptRetry = iterator.next();
                    if (receiptRetry.getRetryNum() > 3) {
                        log.warn("短信回执信息重试3次仍未找到发送记录，已清空该批次回执：{}", receiptRetry.getMessageReceipts());
                        iterator.remove();
                        continue;
                    }
                    List<MessageReceiptDTO> messageReceipts = receiptRetry.getMessageReceipts();
                    Set<String> set = ShardModel.calculateRange(TABLE_NAME, DateUtil.offsetDay(messageReceipts.get(0).getReceiptTime(), -1), DateUtil.offsetDay(messageReceipts.get(0).getReceiptTime(), 1));
                    set.forEach(each -> sendRecordMapper.batchUpdate(messageReceipts, each));
                    // 判断条数是否已全部修改，已修改 remove 记录
                    long count = selectCount(messageReceipts);
                    if (ObjectUtil.equal(messageReceipts.size(), count)) {
                        iterator.remove();
                    } else {
                        receiptRetry.setRetryNum(receiptRetry.getRetryNum() + 1);
                    }
                }
            } finally {
                reentrantLock.unlock();
            }
        }
    }
    
    /**
     * 查询数据库中存在的记录数
     */
    private Long selectCount(List<MessageReceiptDTO> listReceipt) {
        Date receiveTime = listReceipt.get(0).getReceiptTime();
        List<String> sendMsgIds = listReceipt.stream().map(each -> each.getMsgId()).collect(Collectors.toList());
        LambdaQueryWrapper<SendRecordDO> queryWrapper = Wrappers.lambdaQuery(SendRecordDO.class)
                .in(SendRecordDO::getMsgId, sendMsgIds)
                .between(SendRecordDO::getCreateTime, DateUtil.offsetDay(receiveTime != null ? receiveTime : new Date(), -5), new Date());
        return sendRecordMapper.selectCount(queryWrapper);
    }
    
    @Override
    public void destroy() {
        retryFailMessage();
        if (CollUtil.isNotEmpty(listReceipts)) {
            log.warn("短信回执部分状态未更新：{}", JSON.toJSONString(listReceipts));
        }
    }
}
