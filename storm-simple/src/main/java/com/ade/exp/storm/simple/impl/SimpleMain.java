package com.ade.exp.storm.simple.impl;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 *
 * Created by liyang on 2017/5/24.
 */
public class SimpleMain {

    public static void main(String[] args) throws InterruptedException {
        //定义一个Topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("simple-spout", new SimpleSpout(), 1);
        builder.setBolt("simple-bolt1", new SimpleBoltRead()).shuffleGrouping("simple-spout");
        builder.setBolt("simple-bolt2", new SimpleBoltWork(), 2).fieldsGrouping("simple-bolt1", new Fields("read"));
        //配置
        Config conf = new Config();
        conf.setDebug(false);
        //创建一个本地模式cluster
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("simple-topology", conf, builder.createTopology());
        synchronized (SimpleMain.class) {
            SimpleMain.class.wait();
        }
        cluster.killTopology("simple-topology");
        cluster.shutdown();
    }

}
