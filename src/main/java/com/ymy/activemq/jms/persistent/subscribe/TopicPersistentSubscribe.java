package com.ymy.activemq.jms.persistent.subscribe;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

/**
 * Topic 持久化订阅
 * 先启动一次Subscribe 向MQ注册 之后无论Subscribe是否在线都能收到publish推送的消息
 */
public class TopicPersistentSubscribe {
    public static final String ACTIVEMQ_URL = "tcp://192.168.110.132:61616";
    public static final String TOPIC_PERSISTENT_NAME = "topic_persistent";

    public static void main(String[] args) throws JMSException {

        System.out.println("******我是订阅者zs*******");

        //1、按照给定的URL和默认的用户名密码,创建ActiveMQConnectionFactory
        ActiveMQConnectionFactory activeMQConnectionFactory = new ActiveMQConnectionFactory();
        activeMQConnectionFactory.setBrokerURL(ACTIVEMQ_URL);

        //2、通过ConnectionFactory获取Connection
        Connection connection = activeMQConnectionFactory.createConnection();
        //设置订阅客户端的ID
        connection.setClientID("zs");

        //3、通过Connection创建Session
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        //4、通过session创建Destination(Queue/Topic)
        Topic topic = session.createTopic(TOPIC_PERSISTENT_NAME);
        //Topic的订阅者
        TopicSubscriber topicSubscriber = session.createDurableSubscriber(topic, "remark...");

        //5、开始启动访问
        connection.start();

        //6、Topic的订阅者接收消息
        Message message = topicSubscriber.receive();
        while (null != message){
            TextMessage textMessage = (TextMessage)message;
            System.out.println("订阅者收到的持久化topic："+textMessage.getText());
            message = topicSubscriber.receive(5000);
        }

        //7、关闭资源
        topicSubscriber.close();
        session.close();
        connection.close();

    }
}
