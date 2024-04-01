/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opengoofy.assault.messageservice.biz.handler.filter.base;

import com.google.common.collect.Maps;
import org.opengoofy.assault.framework.starter.base.ApplicationContextHolder;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.Ordered;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * 抽象责任链上下文
 */
@Component
public final class AbstractChainContext<T> implements CommandLineRunner {

    // 用于存储责任链处理器的容器，键是标识符，值是对应标识符的处理器列表
    private final Map<String, List<AbstractChainHandler>> abstractChainHandlerContainer = Maps.newHashMap();
    
    /**
     * 责任链组件执行
     *
     * @param mark 标识符，用于获取对应的处理器列表
     * @param requestParam 请求参数
     */
    public void handler(String mark, T requestParam) {
        // 获取标识符对应的处理器列表，并按照处理器的顺序逐个执行处理器的handler方法
        abstractChainHandlerContainer.get(mark).stream()
                .sorted(Comparator.comparing(Ordered::getOrder)).forEach(each -> each.handler(requestParam));
    }
    
    @Override
    public void run(String... args) throws Exception {
        // 获取所有类型为AbstractChainHandler的Bean，并按照标识符分组存储在处理器容器中。
        Map<String, AbstractChainHandler> chainFilterMap = ApplicationContextHolder.getBeansOfType(AbstractChainHandler.class);
        chainFilterMap.forEach((beanName, bean) -> {
            List<AbstractChainHandler> abstractChainHandlers = abstractChainHandlerContainer.get(bean.mark());
            if (abstractChainHandlers == null) {
                abstractChainHandlers = new ArrayList();
            }
            // 将新的处理器添加到处理器列表中
            abstractChainHandlers.add(bean);
            // 更新处理器容器中的处理器列表
            abstractChainHandlerContainer.put(bean.mark(), abstractChainHandlers);
        });
    }
}

/*
责任链模式是一种行为设计模式，其中处理器（handler）形成链，并按照顺序依次处理请求。
 */
