package System;

import org.apache.spark.api.java.JavaRDD;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.Vector;
/*
public abstract class Worker<MsgT> extends Thread {
    private PriorityQueue<MsgT> eventQue = new PriorityQueue<MsgT>();
    //run, vertexqueue
    private Map<Integer, Vector<Integer>> subgraph;
    private String location;
    private Integer loc_port;
    private ServerSocket ss = null;
    private Combiner<MsgT, Integer> combiner;

    Worker(Map<Integer, Vector<Integer>> partedGraph) {
        eventQue.clear();
        subgraph = partedGraph;
    }

    Worker(){ eventQue.clear();}

    public void loadGraph(Map<Integer, Vector<Integer>> graph){
        subgraph = graph;
    }

    public Map<Integer, Vector<Integer>> getSubgraph(){
        return this.subgraph;
    }

    public void setup(String socketAddress, Integer port)throws IOException {
        location = socketAddress;
        loc_port = port;
        ss = new ServerSocket(loc_port);
    }

    public void setCombiner(Combiner<MsgT> combiner) {
        this.combiner = combiner;
    }

    public Combiner<MsgT> getCombiner() {
        return combiner;
    }

    public Socket getSocket() throws IOException { return ss.accept();}

    public abstract void run();

    public String getLocation() { return this.location;}

    public Integer getLoc_port() { return this.loc_port;}
}
*/

public class Worker<MessageDataType, AnsDataType> extends Thread {
    private Map<Integer, Set<Integer>> graph;
    private Set<Integer> domain;
    private Compute<MessageDataType, AnsDataType> compute;
    private Combiner<MessageDataType, AnsDataType> combine;
    private AnsDataType reference;
    public Worker(Map<Integer, Set<Integer>> graph, Set<Integer> domain, Compute<MessageDataType, AnsDataType> compute, Combiner<MessageDataType, AnsDataType> combine){
        this.graph = graph;
        this.domain = domain;
        this.combine = combine;
        this.compute = compute;
    }
    public Worker(Map<Integer, Set<Integer>> graph, Set<Integer> domain, Combiner<MessageDataType, AnsDataType> combine){
        this.graph = graph;
        this.domain = domain;
        this.combine = combine;
    }
    public void setCombine(Compute<MessageDataType, AnsDataType> compute){
        this.compute = compute;
    }

    public void setReference(AnsDataType reference) {
        this.reference = reference;
    }

    public void run(){
        long st = System.currentTimeMillis();
        MessageDataType data = compute.process(graph, domain, reference);
        long end = System.currentTimeMillis();
        System.out.println(" Worker Thread time: ");
        System.out.println(end-st);
        Message<MessageDataType> msg = new Message<MessageDataType>();
        msg.data = data;
        combine.messageQueue.offer(msg);
    }
}