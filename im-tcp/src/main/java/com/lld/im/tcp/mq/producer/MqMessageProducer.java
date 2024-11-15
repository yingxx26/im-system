package com.lld.im.tcp.mq.producer;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lld.im.codec.proto.Message;
import com.lld.im.common.constant.Constants;
import com.lld.im.tcp.utils.MqFactory;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;

/**
 * @description:  im发送给业务(业务专用发送mq)
 * @author: lld
 * @version: 1.0
 */
@Slf4j
public class MqMessageProducer {

    public static void sendMessage(Message message,Integer command){
        Channel channel = null;
        String channelName = Constants.RabbitConstants.Im2MessageService;

        if(command.toString().startsWith("2")){
            channelName = Constants.RabbitConstants.Im2GroupService;
        }

        try {
            channel = MqFactory.getChannel(channelName);

            JSONObject o = (JSONObject) JSON.toJSON(message.getMessagePack());
            o.put("command",command);
            o.put("clientType",message.getMessageHeader().getClientType());
            o.put("imei",message.getMessageHeader().getImei());
            o.put("appId",message.getMessageHeader().getAppId());
            channel.basicPublish(channelName,"",
                    null, o.toJSONString().getBytes());
        }catch (Exception e){
            log.error("发送消息出现异常：{}",e.getMessage());
        }
    }

}
