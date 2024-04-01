package org.opengoofy.assault.messageservice.biz.canal;

import com.alibaba.fastjson.JSON;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.common.utils.AddressUtils;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.google.common.collect.Lists;
import jodd.util.ThreadUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.opengoofy.assault.framework.starter.cache.DistributedCache;
import org.opengoofy.assault.messageservice.biz.common.EnableStatusEnum;
import org.opengoofy.assault.messageservice.biz.dao.entity.TemplateConfigDO;
import org.opengoofy.assault.messageservice.biz.dao.mapper.TemplateConfigMapper;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.scripting.support.ResourceScriptSource;
import org.springframework.stereotype.Component;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static org.opengoofy.assault.messageservice.biz.common.MessageCacheConstants.MESSAGE_TEMPLATE_CACHE_PREFIX_KEY;

/**
 * 消息监听
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class MessageCanalClientListener implements CommandLineRunner {
    
    private final DistributedCache distributedCache;
    private final TemplateConfigMapper templateConfigMapper;
    
    private static final int BATCH_SIZE = 1000;
    private static final String SUBSCRIBE = "message_manager\\.template_config";
    private static final String LUA_CANAL_SCRIPT_SOURCE_PATH = "lua/cacheUpdateByCanal.lua";


    //通过 Canal 客户端监听数据库的变动，并处理接收到的消息条目
    @Override
    public void run(String... args) throws Exception {
        Thread linkCanalClientListenerThread = new Thread(() -> {   //创建新线程，启动Canal客户端监听器
            CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress(AddressUtils.getHostIp(), 11111), "example", "", "");
            try {
                connector.connect();
                connector.subscribe(SUBSCRIBE);  //订阅数据库的表
                while (true) {
                    Message message = connector.getWithoutAck(BATCH_SIZE);  //使用连接器获取指定批次大小（BATCH_SIZE1000）的消息
                    long batchId = message.getId();  //获取消息批次的id
                    if (batchId == -1 || message.getEntries().size() == 0) {  //检查批次 ID 是否为 -1 或者消息条目数是否为 0，判断是否没有新的数据变动。
                        ThreadUtil.sleep(1000);  //如果没有新的数据变动，等待一段时间后继续获取消息。
                        continue;
                    }
                    printEntry(message.getEntries());  //调用 printEntry 方法处理消息的条目
                    connector.ack(batchId);  //对接收到的消息进行确认，告知 Canal 服务器已经成功处理该批次的消息
                }
            } finally {
                connector.disconnect();
            }
        });
        linkCanalClientListenerThread.setName("LinkCanalClientListener");
        linkCanalClientListenerThread.start();
    }


    // 处理接收到的 Canal 消息的条目。
    // 跳过事务相关的条目，解析非事务条目的行级别变动，
    // 调用 cacheDelAndUpdate 方法执行相应的缓存删除和更新操作。
    private void printEntry(List<CanalEntry.Entry> entrys) {
        // 如果是事务相关的条目，跳过处理
        for (CanalEntry.Entry entry : entrys) {
            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                continue;
            }
            CanalEntry.RowChange rowChange;
            try {
                // 解析 Canal 消息中的行变动信息
                rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                // 如果解析出错，抛出异常
                throw new RuntimeException("ERROR ## parser of error change-event has an error, data: " + entry, e);
            }
            // 获取行变动的事件类型
            CanalEntry.EventType eventType = rowChange.getEventType();
            // 遍历每一行数据，调用 cacheDelAndUpdate 方法处理缓存的删除和更新
            rowChange.getRowDatasList().forEach(each -> cacheDelAndUpdate(eventType, each));
        }
    }
    
    /**
     * 删除缓存并且更新缓存
     */
    //根据模板 ID 获取缓存键名，然后根据启用状态判断是仅删除缓存还是删除旧缓存并更新为新缓存,最后输出相应的日志信息
    private void cacheDelAndUpdate(CanalEntry.EventType eventType, CanalEntry.RowData rowData) {
        // 如果事件类型不是 UPDATE，不进行处理
        if (eventType != CanalEntry.EventType.UPDATE) {
            return;
        }
        // 从变动前的列数据中找到模板 ID
        Optional<CanalEntry.Column> templateIdOptional = rowData.getBeforeColumnsList().stream().filter(each -> Objects.equals("template_id", each.getName())).findFirst();
        // 如果找不到模板 ID，不进行处理
        if (!templateIdOptional.isPresent()) {
            return;
        }
        // 获取模板 ID 的值
        String templateId = templateIdOptional.get().getValue();

        //构建缓存的键名，将模板 ID 与预定义的前缀进行拼接
        String templateConfigCacheKey = MESSAGE_TEMPLATE_CACHE_PREFIX_KEY + templateId;

        // 从变动后的列数据中找到 enable_status 列，并判断是否为未启用状态
        Optional<CanalEntry.Column> enableStatusOptional = rowData.getAfterColumnsList().stream()
                .filter(each -> Objects.equals("enable_status", each.getName()))
                .filter(each -> Objects.equals(EnableStatusEnum.NOT_ENABLED.getStatus(), each.getValue()))
                .findFirst();
        // 如果 enable_status == EnableStatusEnum.NOT_ENABLED 代表模板未启用，仅删除缓存即可
        if (enableStatusOptional.isPresent()) {
            distributedCache.delete(templateConfigCacheKey);
            // 记录日志，表示成功删除了消息模板缓存
            log.info("Canal监听成功删除消息模板缓存：{}", templateConfigCacheKey);
            return;
        }

        // 执行 Redis Lua 脚本，实现删除旧缓存并更新为新缓存
        DefaultRedisScript redisScript = new DefaultRedisScript();
        redisScript.setScriptSource(new ResourceScriptSource(new ClassPathResource(LUA_CANAL_SCRIPT_SOURCE_PATH)));
        // 获取模板配置信息
        TemplateConfigDO templateConfigDO = templateConfigMapper.selectOne(Wrappers.lambdaQuery(TemplateConfigDO.class).eq(TemplateConfigDO::getTemplateId, templateId));
        // 执行 Redis 脚本，实现缓存的删除和更新操作
        ((RedisTemplate) distributedCache.getInstance()).execute(redisScript, Lists.newArrayList(templateConfigCacheKey), JSON.toJSONString(templateConfigDO));

        // 记录日志，表示成功更新了消息模板缓存
        log.info("Canal监听成功更新消息模板缓存：{}", templateConfigCacheKey);
    }
}
