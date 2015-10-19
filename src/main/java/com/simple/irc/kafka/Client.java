/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.simple.irc.kafka;

import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.Future;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 *
 * @author HP
 */
public class Client {
    public static String ALPHABETS = "abcdefghijklmnopqrstuvwxyz";
    private String key;
    private String nick;
    public boolean isStop = false;
    /**
     * String is the channel name, Consumer is the consumer of topic that 
     * represents the channel.
     */
    private final Map<String, IrcConsumer> channel = new HashMap<>();

    public Client() {
        SecureRandom random = new SecureRandom();
        this.key = new BigInteger(35, random).toString(32);
    }
    
    /**
     * Create producer with some configuration
     * @return 
     */
    public Producer<String, String> createProducer() {
        Properties props = new Properties();
        props.put("metadata.broker.list","localhost:9092");
        props.put("serializer.class","kafka.serializer.StringEncoder");

        Producer<String, String> producer = new Producer<>(new ProducerConfig(props));
        return producer;
    }
    
    public void handleRequest(String request) {
        String[] split = request.split("\\s+");
        switch (split[0]){
                case "/NICK":	
                        if (split.length > 1) {
                            // user inputed a nickname
                            nick(split[1]);
                        } else {
                            // user didn't input a nickname
                            nick("");
                        }
                        break;
                case "/JOIN":	
                        if (split.length > 1) {
                            // user inputed a channel name
                            join(split[1]);
                        } else {
                            // user didn't input a channel name
                            join("");
                        }
                        break;
                case "/LEAVE":
                        if (split.length > 1) {
                            // user inputed a channel name
                            leave(split[1]);
                        } else {
                            // user didn't input a channel name
                            leave("");
                        }
                        break;
                case "/EXIT":
                        exit();
                        break;
                default:
                        // send message
                        handlingMessage(request);
                        break;
        }
    }
    
    public void nick(String nickname) {
        // not including check unique nickname yet
        if (nickname.isEmpty()) {
            // generate random nickname, for now let's make it simple 
            this.nick = this.key;
        } else {
            this.nick = nickname;
        }
        System.out.println("Online as " + this.nick);
    }
    
    public void join(String channelname) {
        String chName;
        if (channelname.isEmpty()) {
            // generate random channelname
            chName = generateString(new Random(), ALPHABETS, 7);
        } else {
            chName = channelname;
        }
        // check whether this already registered to the specified channel
        if (this.channel.containsKey(chName)) { 
            // user already registered to this channel
            System.out.println("You are already registered here.");
            return;
        }
        // set this to be a consumer of specified topic
        // no need to check whether the topic is exist (?)
        IrcConsumer ircConsumer = new IrcConsumer(key, chName);
        new Thread(ircConsumer).start();
        channel.put(chName, ircConsumer);
        System.out.println("Joined channel " + chName);        
    }
    
    public void leave(String channelname) {
        if (channelname.isEmpty()) {
            System.out.println("Please specify a channel name.");
            return;
        }
        // check whether this subscribed to `channelname`
        if (!channel.containsKey(channelname)) {
            System.out.println("You are not registered in " + channelname);
            return;
        }
        // delete channelname from this.channel stop consume from topic `channelname`
        IrcConsumer ircConsumer = channel.get(channelname);
        ircConsumer.consumerConnector.shutdown();
        channel.remove(channelname);
    }
    
    public void exit() {
        // Do we need to destroy all this consumer?
        for (IrcConsumer channelname: channel.values()){
            channelname.consumerConnector.shutdown();
        }
        // terminate this
        System.out.println("Going offline...");
        this.isStop = true;
    }
    
    public void sendMessageToAll(String message) {
        for (String channelname: channel.keySet()) {
            sendMessage(channelname, message);
        }
    }
    
    public void sendMessage(String channelname, String message) {
        // makesure channelname exists in channel 
        if (!channel.containsKey(channelname)) {
            System.out.println("You're not registered to channel " + channelname);
            return;
        }
        // create new Producer for topic `channelname`
        // send message
        Producer<String, String> p = createProducer();
        String messageWithAttributes = "[" + channelname + "] (" + nick + ") " + message;
        KeyedMessage<String, String> messageToChannel = 
                new KeyedMessage<>(channelname, messageWithAttributes);
        p.send(messageToChannel);
        p.close();
    }
    
    public void handlingMessage(String input) {
        if (input.startsWith("@")) {
            String chName = null;
            String msg = null;
            if(input.contains(" ")){
                int idx = input.indexOf(" ");
                chName = input.substring(1, idx);
                msg = input.substring(idx+1);
            }
            sendMessage(chName, msg);
        } else {
            sendMessageToAll(input);
        }
    }
    
    public static String generateString(Random rng, String characters, int length)
    {
        char[] text = new char[length];
        for (int i = 0; i < length; i++)
        {
            text[i] = characters.charAt(rng.nextInt(characters.length()));
        }
        return new String(text);
    }
    
    public static void main(String[] args) {
        // TODO code application logic here
        Client c = new Client();
        Scanner in = new Scanner(System.in);
        while(!c.isStop) {
            String request = in.nextLine();
            c.handleRequest(request);
        }    
    }
}
