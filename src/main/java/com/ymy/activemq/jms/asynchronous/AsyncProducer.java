package com.ymy.activemq.jms.asynchronous;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQMessageProducer;
import org.apache.activemq.AsyncCallback;

import javax.jms.*;
import java.util.UUID;

/**
 * 异步投递
 */
public class AsyncProducer {
    public static final String ACTIVEMQ_URL = "tcp://192.168.110.132:61616";
    public static final String QUEUE_NAME = "queue_async";

    public static void main(String[] args) throws JMSException {
        //1、按照给定的URL和默认的用户名密码,创建ActiveMQConnectionFactory
        ActiveMQConnectionFactory activeMQConnectionFactory = new ActiveMQConnectionFactory();
        activeMQConnectionFactory.setBrokerURL(ACTIVEMQ_URL);
        activeMQConnectionFactory.setUseAsyncSend(true);   //开启异步投递

        //2、通过ConnectionFactory获取Connection
        Connection connection = activeMQConnectionFactory.createConnection();

        //3、通过Connection创建Session
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        //4、通过session创建Destination(Queue/Topic)
        Queue queue = session.createQueue(QUEUE_NAME);

        //5、通过session创建消息的生产者 这里要用ActiveMQMessageProducer
        ActiveMQMessageProducer activeMQMessageProducer = (ActiveMQMessageProducer) session.createProducer(queue);
        connection.start();

        //6、通过使用MessageProducer生产1条消息发送到MQ的队列中
        TextMessage textMessage = session.createTextMessage("Hello Async"); //消息

        //为消息添加MessageID
        textMessage.setJMSMessageID(UUID.randomUUID().toString().substring(0,6)+" msg callback tip");
        String messageId = textMessage.getJMSMessageID();
        //具有回调功能的发送
        activeMQMessageProducer.send(textMessage, new AsyncCallback() {
            @Override
            public void onSuccess() {
                System.out.println(messageId + " send ok!");
            }
            @Override
            public void onException(JMSException exception) {
                System.out.println(messageId + " send failed(╥﹏╥)o");
            }
        });


        activeMQMessageProducer.close();
        session.close();
        connection.close();

        System.out.println("异步投递消息发送完成！");
    }
}
