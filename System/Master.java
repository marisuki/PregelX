package System;

import org.apache.hadoop.fs.Hdfs;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.spark_project.jetty.util.ArrayQueue;

import javax.xml.soap.Node;
import javax.xml.ws.Action;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ThreadPoolExecutor;

import static java.lang.Double.max;
/*
public abstract class Master<MsgT> {
    // BSP ctrl\
    //private SparkConf conf;
    //private JavaSparkContext mastersc;
    private Map<Integer, Vector<Integer>> graph;
    private Map<Integer, Vertex> vertex2id;
    private ArrayQueue<Socket> workerSocketQue;
    public Integer maxPts;
    public String graphDir;
    public String setupDir;

    public Master() {}

    public Master(Integer maxpts, String graphDir, String setupDir) throws IOException {
        maxPts = maxpts;
        this.graphDir = graphDir;
        this.setupDir = setupDir;
        //conf = new SparkConf().setAppName("Master Node").setMaster("local");
        //mastersc = new JavaSparkContext(conf);
    }

    public void singleNodeMultipleThread(int threads, Combiner<MsgT, Integer> combiner) throws FileNotFoundException {
        graphLoader();
        //bsp:

    }

    public void setGraphDir(String graphDir) {
        this.graphDir = graphDir;
    }

    public void setMaxPts(Integer maxPts) {
        this.maxPts = maxPts;
    }

    public void setSetupDir(String setupDir) {
        this.setupDir = setupDir;
    }

    public void multipleNodeMultipleThread() throws FileNotFoundException {
        graphLoader();
    }

    public void initDistributedWorkers(Vector<Worker<MsgT>> workers) throws IOException {
        Scanner sc = new Scanner(new FileInputStream(setupDir));
        int cnt = 0;
        while(sc.hasNext()){
            String tmp = sc.nextLine();
            String[] spl = tmp.split(":");
            String host = spl[0];
            Integer port = Integer.parseInt(spl[1]);
            //Worker<MsgT> worker = new Worker<MsgT>(host, port, null);
            //single node multiple thread
            workers.get(cnt).setup(host, port);
            workerSocketQue.add(workers.get(cnt).getSocket());
            cnt++;
        }
    }

    public void graphLoader() throws FileNotFoundException {
        Scanner sc = new Scanner(new FileInputStream(graphDir));
        //Vector<Vector<Integer>> graph = new Vector<>();
        graph = new HashMap<>();
        while(sc.hasNext()){
            String tmp = sc.nextLine();
            if(tmp.startsWith("#")) continue;
            String[] spl = tmp.split(" ");
            System.out.println(spl.length);
            for(String tmps: spl) System.out.println(tmps);
            Integer from = Integer.parseInt(spl[0]);
            Integer to = Integer.parseInt(spl[spl.length-1]);
            //while(graph.size()<max(from, to)) graph.add(new Vector());
            //if(graph.size()<from) graph.get(from).add(to);
            if(graph.containsKey(from)) graph.get(from).add(to);
            else graph.put(from, new Vector<Integer>(to));
        }
        for(Integer key: graph.keySet()){
            System.out.println(key);
            for(Integer t: graph.get(key)){
                System.out.print(t);
                System.out.print(" ");
            }
            System.out.println();
        }
    }

    public abstract void DetailedGraphGenerator();

    public void graphPartition(){


    }

    public Vector<Map<Integer, Vector<Integer>>> naiveRandomPartition(int parts){
        int pt = (int)Math.ceil(graph.size()/parts);
        Vector<Map<Integer, Vector<Integer>>> ans = new Vector<>();
        int cnt = 0;
        Set<Integer> ks = graph.keySet();
        Map<Integer, Vector<Integer>> tmp = new HashMap<>();
        Map<Integer, Vector<Integer>> hold = new HashMap<>();
        for(Integer key: ks){
            int rd = (int)Math.random()*parts;
            if(hold.containsKey(rd)) hold.get(rd).add(key);
            else hold.put(rd, new Vector<>(key));
        }
        for(Integer key: hold.keySet()){
            tmp = new HashMap<>();
            for(Integer item: hold.get(key)){
                tmp.put(item, graph.get(item));
            }
            ans.add((Map<Integer, Vector<Integer>>) ((HashMap<Integer, Vector<Integer>>) tmp).clone());
        }
        return ans;
    }

    public Vector<Map<Integer, Vector<Integer>>> naiveAveragePartition(int parts){
        int pt = (int)Math.ceil(graph.size()/parts);
        Vector<Map<Integer, Vector<Integer>>> ans = new Vector<>();
        int cnt = 0;
        Map<Integer, Vector<Integer>> tmp = new HashMap<>();
        for(Integer key: graph.keySet()){
            if(cnt%pt==0) {
                ans.add((Map<Integer, Vector<Integer>>) ((HashMap<Integer, Vector<Integer>>) tmp).clone());
                tmp = new HashMap<>();
            }
            cnt ++;
            tmp.put(key, graph.get(key));
        }
        return ans;
    }

    class NodeMessage extends Message<HashMap<Integer, Set<Integer>>> {

        @Override
        public void process() {
            msg_var = 1;
            return;
        }
    }

    public Vector<Set<Integer>> connectedComponents() throws FileNotFoundException {
        if(graph == null) graphLoader();
        int[] point = new int[graph.size()];
        int[] rank = new int[graph.size()];
        for(int i=0;i<graph.size();i++) {point[i]=i; rank[i]=1;}

        class MainWorker extends Worker<NodeMessage>{
            @Override
            public void run() {
                Map<Integer, Vector<Integer>> gra = super.getSubgraph();int maxnum = -1;
                for(Integer key: gra.keySet()){
                    System.out.println(key);
                    for(Integer t: gra.get(key)){
                        System.out.print(t);
                        System.out.print(" ");
                    }
                    System.out.println();
                }
                Set<Integer> points = new HashSet<>();
                for(Integer key: gra.keySet()){
                    points.add(key);
                    points.addAll(gra.get(key));
                }
                Map<Integer, Integer> poii = new HashMap<>();
                Map<Integer, Integer> ranks = new HashMap<>();
                for(Integer i: points) {poii.put(i,i); ranks.put(i, 1);}
                for(Integer from: gra.keySet()){
                    int x = find(poii, from);
                    for(Integer to: gra.get(from)){
                        int y = find(poii, to);
                        if(rank[x]>rank[y]) poii.put(y, x);
                        else poii.put(x,y);
                        if(rank[x]==rank[y]) ranks.put(y, ranks.get(y)+1);
                    }
                }
                HashMap<Integer, Set<Integer>> vis = new HashMap<>();
                for(Integer i: points){
                    int x = poii.get(i);
                    if(!poii.get(i).equals(i)) x = find(poii, i);
                    if(vis.containsKey(x)) vis.get(x).add(i);
                    else vis.put(x, new HashSet<>(i));
                }
                for(Integer key: vis.keySet()){ System.out.println(key); for(Integer ts: vis.get(key)) {System.out.print(ts); System.out.print(" ");}}
                //Combiner<NodeMessage, Integer> comb = super.getCombiner();
                NodeMessage nm = new NodeMessage();
                //nm.data.add((Map<Integer, Vector<Integer>>) ((HashMap<Integer, Vector<Integer>>) vis).clone());
                nm.data.add(vis);
                super.getCombiner().messageQueue.offer(nm);
                return;
            }

            public int find(Map<Integer, Integer> poi, int x){
                while(x!=poi.get(x)) x = poi.get(x);
                return x;
            }

            @Override
            public synchronized void start() {
                System.out.println("[Info] Start Thread.");
                //super.start();
                run();
            }
        }

        class CCCombiner extends Combiner<NodeMessage, Integer> {
            Map<Integer, Integer> valueMp = new HashMap<>();
            Map<Integer, Set<Integer>> ans = new HashMap<>();
            public Vector<Set<Integer>> cc = new Vector<>();
            @Override
            public void combine() {
                System.out.println(messageQueue.size());
                while(!super.messageQueue.isEmpty()){
                    HashMap<Integer, Set<Integer>> tmp = messageQueue.getFirst().data.firstElement();
                    messageQueue.poll();
                    for(Integer key: tmp.keySet()){
                        Integer x = -1;
                        for(Integer poi: tmp.get(key)){
                            if(valueMp.containsKey(poi)){
                                x = valueMp.get(poi);
                                break;
                            }
                        }
                        if(x!=-1){
                            ans.get(x).addAll(tmp.get(key));
                        }
                        else{
                            for(Integer poi: tmp.get(key)) {valueMp.put(poi, key);}
                            ans.put(key, new HashSet<Integer>(tmp.get(key)));
                        }
                    }
                }
                for(Integer key: ans.keySet()) cc.add(ans.get(key));
            }

            public Vector<Set<Integer>> getCc() {
                return cc;
            }
        }

        CCCombiner combine = new CCCombiner();

        Vector<Map<Integer, Vector<Integer>>> initParted = naiveAveragePartition(maxPts);
        //Vector<MainWorker> hold = new Vector<>();
        for(int i=0;i<initParted.size();i++){
            MainWorker worker = new MainWorker();
            worker.loadGraph(initParted.get(i));
            worker.setCombiner(combine);
            worker.start();
        }
        combine.combine();
        return combine.getCc();
    }
}
*/

public class Master<MessageDataType, AnsDataType> {
    AnsDataType ans;
    BSP<MessageDataType, AnsDataType> bsp;
    Map<Integer, Set<Integer>> graph;
    Set<Integer> points;
    int threads;
    public Master(String graphDirection, int threads) throws FileNotFoundException {
        //this.bsp = bsps;
        this.threads = threads;
        graphLoader(graphDirection);
    }

    public Map<Integer, Set<Integer>> getGraph() {
        return graph;
    }

    private void graphLoader(String dir) throws FileNotFoundException {
        Scanner sc = new Scanner(new FileInputStream(dir));
        graph = new HashMap<>();
        points = new HashSet<>();
        while(sc.hasNext()){
            String tmp = sc.nextLine();
            if(tmp.startsWith("#")) continue;
            String[] spl = tmp.split("\t");
            //System.out.println(spl.length);
            //for(String tmps: spl) System.out.println(tmps);
            Integer from = Integer.parseInt(spl[0]);
            Integer to = Integer.parseInt(spl[spl.length-1]);
            //while(graph.size()<max(from, to)) graph.add(new Vector());
            //if(graph.size()<from) graph.get(from).add(to);
            if(!graph.containsKey(from)) graph.put(from, new HashSet<Integer>());
            graph.get(from).add(to);
            points.add(from);
            points.add(to);
        }
        /*
        for(Integer key: graph.keySet()){
            System.out.print(key);
            System.out.println(":");
            for(Integer t: graph.get(key)){
                System.out.print(t);
                System.out.print(" ");
            }
            System.out.println();
        }*/
    }

    public Set<Integer> getPoints() {
        return points;
    }

    public Vector<Map<Integer, Set<Integer>>> naiveRandomPartition(){
        int pt = (int)Math.ceil(graph.size()/threads);
        Vector<Map<Integer, Set<Integer>>> ans = new Vector<>();
        int cnt = 0;
        Set<Integer> ks = graph.keySet();
        Map<Integer, Set<Integer>> tmp = new HashMap<>();
        Map<Integer, Set<Integer>> hold = new HashMap<>();
        for(Integer key: ks){
            int rd = (int)Math.random()*threads;
            if(!hold.containsKey(rd)) hold.put(rd, new HashSet<>());
            hold.get(rd).add(key);
        }
        for(Integer key: hold.keySet()){
            tmp = new HashMap<>();
            for(Integer item: hold.get(key)){
                tmp.put(item, graph.get(item));
            }
            ans.add((Map<Integer, Set<Integer>>) ((HashMap<Integer, Set<Integer>>) tmp).clone());
        }
        return ans;
    }

    public Vector<Map<Integer, Set<Integer>>> naiveAveragePartition(){
        int pt = (int)Math.ceil(graph.size()/threads)+1;
        Vector<Map<Integer, Set<Integer>>> ans = new Vector<>();
        int cnt = 0;
        Map<Integer, Set<Integer>> tmp = new HashMap<>();
        for(Integer key: graph.keySet()){
            if(cnt%pt==0) {
                ans.add((Map<Integer, Set<Integer>>) ((HashMap<Integer, Set<Integer>>) tmp).clone());
                tmp = new HashMap<>();
            }
            cnt ++;
            tmp.put(key, graph.get(key));
        }
        for(Map<Integer, Set<Integer>> m: ans){
            for(Integer k: m.keySet())
                System.out.println(k);
            System.out.println();
        }
        return ans;
    }

    @Action
    public Vector<Map<Integer, Set<Integer>>> CCBasedPartition(){
        return null;
    }

    public void run(BSP<MessageDataType, AnsDataType> bsps){
        this.bsp = bsps;
        bsp.cyclic(graph);
        ans = bsp.getAns();
    }

    public AnsDataType getAns() {
        return ans;
    }
}