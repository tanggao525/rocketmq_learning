package com.tanggao.rocketmq_leaning;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;

@SpringBootTest
public class FirstTest {

    @Test
    public void sendMessage() throws Exception {
        // 创建一个生产者(制定一个组名)
        DefaultMQProducer producer = new DefaultMQProducer("producer_group");

        // 链接nameService
        producer.setNamesrvAddr("127.0.0.1:9876");

        //启动
        producer.start();

        // 创建一个消息
        Message message = new Message("topic1", "value".getBytes());

        // 发送消息
        SendResult send = producer.send(message);

        System.out.println(send);

        producer.shutdown();
    }

    @Test
    public void consumerMessage() throws Exception {
        // 创建一个消费者
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("consumer_group");

        // 连接到namesrv
        consumer.setNamesrvAddr("127.0.0.1:9876");

        // 订阅一个主题 *表示订阅这个主题的所有消息
        consumer.subscribe("topic1", "*");

        // 设置一个监听器
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                // 消费代码写在此处
                System.out.println("消费");
                System.out.println(msgs.get(0).toString());
                System.out.println("上下文:" + context);

                //返回值
                // CONSUME_SUCCESS 消费成功，消息会从mq出队
                // RECONSUME_LATER 消费失败，消息会重新入队
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        // 启动
        consumer.start();

        // 挂起当前线程，持续监听
        System.in.read();
    }
}
