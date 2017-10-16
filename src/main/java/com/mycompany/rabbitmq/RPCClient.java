/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.rabbitmq;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeoutException;
public class RPCClient {
    private Connection connection;
    private Channel channel;
    private String requestQueueName = "rpc_queue";
    private String replyQueueName;
    
    public RPCClient() throws IOException, TimeoutException{
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        connection = factory.newConnection();
        channel = connection.createChannel();
        
        replyQueueName = channel.queueDeclare().getQueue();
    }
    
    public String call(String message) throws UnsupportedEncodingException, IOException, InterruptedException{
        String correlation_ID = UUID.randomUUID().toString();
        
        AMQP.BasicProperties props = new AMQP.BasicProperties
                .Builder()
                .correlationId(correlation_ID)
                .replyTo(replyQueueName)
                .build();
        
        channel.basicPublish("",requestQueueName,props, message.getBytes("UTF-8"));
        
        final BlockingQueue<String> response = new ArrayBlockingQueue<String>(1);
        
        channel.basicConsume(replyQueueName, true, new DefaultConsumer(channel){
        
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws UnsupportedEncodingException{
                if(properties.getCorrelationId().equals(correlation_ID)){
                    response.offer(new String(body, "UTF-8"));
                }
            }
        });
            
        return response.take();
    }
    
    public void close() throws IOException {
         connection.close();
    }
    
    public static void main(String[] argv) throws IOException{
        RPCClient fibonacciRpc = null;
        String response = null;
        
        try{
            fibonacciRpc = new RPCClient();
            System.out.print("Fibonacci for 10");
            response = fibonacciRpc.call("10");
            System.out.println("Result "+response);
        }
        catch(IOException | TimeoutException | InterruptedException e){
        }
        finally{
            if(fibonacciRpc != null){
                fibonacciRpc.close();
            }
        }
    }
}
