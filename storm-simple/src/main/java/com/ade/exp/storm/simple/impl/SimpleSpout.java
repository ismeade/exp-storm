package com.ade.exp.storm.simple.impl;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 * Created by liyang on 2017/5/24.
 */
public class SimpleSpout implements IRichSpout {

    private SpoutOutputCollector collector;

    /**
     * 这是第一个方法，里面接收了三个参数，第一个是创建Topology时的配置，
     * 第二个是所有的Topology数据，第三个是用来把Spout的数据发射给bolt
     * **/
    public void open(Map                  conf,
                     TopologyContext      context,
                     SpoutOutputCollector collector)
    {
        this.collector = collector;
    }

    /**
     * 这是Spout最主要的方法，在这里我们读取文本文件，并把它的每一行发射出去（给bolt）
     * 这个方法会不断被调用，为了降低它对CPU的消耗，当任务完成时让它sleep一下
     * **/
    public void nextTuple() {
        System.out.println("[spout start]");
        for (int i = 0; i < 10; i++) {
            this.collector.emit(new Values(i));
        }
        try {
            System.out.println("[spout sleep]");
            TimeUnit.SECONDS.sleep(5);
        } catch (InterruptedException e) {
            // ignore
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("number"));

    }

    public void close() {
        // TODO Auto-generated method stub
    }

    public void activate() {
        // TODO Auto-generated method stub

    }

    public void deactivate() {
        // TODO Auto-generated method stub

    }

    public void ack(Object msgId) {
        System.out.println("OK:" + msgId);
    }

    public void fail(Object msgId) {
        System.out.println("FAIL:" + msgId);

    }

    public Map<String, Object> getComponentConfiguration() {
        // TODO Auto-generated method stub
        return null;
    }

}
