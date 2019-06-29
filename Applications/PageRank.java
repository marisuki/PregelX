package Applications;

import System.Master;
import System.Message;
import System.Compute;
import System.Combiner;
import System.BSP;

import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

public class PageRank {
    public static void main(String[] argv) throws FileNotFoundException {
        int threads = 500;
        Master<Map<Integer, Double>, Map<Integer, Double>> master =
                new Master<Map<Integer, Double>, Map<Integer, Double>>("./data/web-Google.txt", threads);

        class ComputeSSSP extends Compute<Map<Integer, Double>, Map<Integer, Double>>{
            @Override
            public Map<Integer, Double> process(Map<Integer, Set<Integer>> graph, Set<Integer> domain, Map<Integer, Double> var) {
                Map<Integer, Double> tmp = new HashMap<>();
                for(Integer k: domain){
                    for(Integer t: graph.get(k)){
                        if(tmp.containsKey(t)){
                            tmp.put(t, var.get(k)*1.0/graph.get(k).size()*1.0 + tmp.get(t));
                        }
                        else tmp.put(t, var.get(k)*1.0/graph.get(k).size()*1.0);
                    }
                }
                return tmp;
            }
        }

        Combiner<Map<Integer, Double>, Map<Integer, Double>> combiner = new Combiner<Map<Integer, Double>, Map<Integer, Double>>() {
            @Override
            public Map<Integer, Double> combine() {
                Map<Integer, Double> ans = new HashMap<>();
                while(!messageQueue.isEmpty()){
                    Map<Integer, Double> tmp = messageQueue.getFirst().data;
                    messageQueue.poll();
                    for(Integer k: tmp.keySet()){
                        ans.put(k, 0.5 * tmp.get(k) + ans.getOrDefault(k, 0.5));
                    }
                }
                return ans;
            }
        };

        Vector<Map<Integer, Set<Integer>>> partedGraph = master.naiveAveragePartition();
        Map<Integer, Set<Integer>> graph = master.getGraph();
        Vector<Set<Integer>> nodeSet = new Vector<>();
        Set<Integer> points = master.getPoints();
        for(Map<Integer, Set<Integer>> p: partedGraph) nodeSet.add(p.keySet());
        Vector<Compute<Map<Integer, Double>, Map<Integer, Double>>> compute = new Vector<>();
        for(int i=0;i<threads;i++) compute.add(new ComputeSSSP());
        BSP<Map<Integer, Double>, Map<Integer, Double>> bsp = new BSP<Map<Integer, Double>, Map<Integer, Double>>(combiner, compute, nodeSet, graph) {
            @Override
            public void init() {
                ans = new HashMap<>();
                int inf = 0x3f3f3f3f;
                for(Integer i:points) ans.put(i, 0.0);
                for(Integer i: points) active.put(i, true);
            }

            @Override
            public void printResult() {
                return;/*
                for(Integer k: ans.keySet()){
                    System.out.print(k);
                    System.out.print(": ");
                    System.out.println(ans.get(k));
                }*/
            }

            @Override
            public Boolean UpdateActiveMap(Map<Integer, Double> pre, Map<Integer, Double> now) {
                boolean ans = false;
                System.out.println(pre);
                System.out.println(now);
                for(Integer k: pre.keySet()){
                    if(now.containsKey(k)){
                        if(Math.abs(pre.get(k) - now.get(k)) > 1e-2){
                            ans = true;
                        }
                    }
                    else now.put(k, pre.get(k));
                    //ans = true;
                }
                return ans;
            }
        };
        master.run(bsp);
        Map<Integer, Double> ans = bsp.getAns();




        for(Integer k: ans.keySet()){
            System.out.print(k);
            System.out.print(": ");
            System.out.println(ans.get(k));
        }
    }
}
