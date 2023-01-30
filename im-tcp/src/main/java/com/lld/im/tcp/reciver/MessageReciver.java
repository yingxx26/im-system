package com.lld.im.tcp.reciver;

import com.alibaba.fastjson.JSONObject;
import com.lld.im.codec.proto.MessagePack;
import com.lld.im.common.constant.Constants;
import com.lld.im.tcp.reciver.process.BaseProcess;
import com.lld.im.tcp.reciver.process.ProcessFactory;
import com.lld.im.tcp.utils.MqFactory;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;

/**
 * @description:  业务监听处理
 * @author: lld
 * @version: 1.0
 */
@Slf4j
public class MessageReciver {

    private static String brokerId;

    private static void startReciverMessage() {
        try {
            Channel channel = MqFactory
                    .getChannel(Constants.RabbitConstants.MessageService2Im + brokerId);
            channel.queueDeclare(Constants.RabbitConstants.MessageService2Im + brokerId,
                    true, false, false, null
            );
            channel.queueBind(Constants.RabbitConstants.MessageService2Im + brokerId,
                    Constants.RabbitConstants.MessageService2Im, brokerId);

            channel.basicConsume(Constants.RabbitConstants
                            .MessageService2Im + brokerId, false,
                    new DefaultConsumer(channel) {
                        @Override
                        public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                            try {
                                String msgStr = new String(body);
                                log.info(msgStr);
                                MessagePack messagePack =
                                        JSONObject.parseObject(msgStr, MessagePack.class);
                                BaseProcess messageProcess = ProcessFactory
                                        .getMessageProcess(messagePack.getCommand());
                                messageProcess.process(messagePack);

                                channel.basicAck(envelope.getDeliveryTag(),false);

                            }catch (Exception e){
                                e.printStackTrace();
                                channel.basicNack(envelope.getDeliveryTag(),false,false);
                            }
                        }
                    }
            );
        } catch (Exception e) {

        }
    }

    public static void init() {
        startReciverMessage();
    }

    public static void init(String brokerId) {
        if (StringUtils.isBlank(MessageReciver.brokerId)) {
            MessageReciver.brokerId = brokerId;
        }
        startReciverMessage();
    }


}
