package Applications;

import System.Master;
import System.Message;
import System.Compute;
import System.Combiner;
import System.BSP;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

public class SSSP {
    public static void main(String[] argv) throws FileNotFoundException {
        int threads = 50;
        Master<Map<Integer, Integer>, Map<Integer, Integer>> master =
                new Master<Map<Integer, Integer>, Map<Integer, Integer>>("./data/web-Google.txt", threads);

        class ComputeSSSP extends Compute<Map<Integer, Integer>, Map<Integer, Integer>>{
            @Override
            public Map<Integer, Integer> process(Map<Integer, Set<Integer>> graph, Set<Integer> domain, Map<Integer, Integer> var) {
                Map<Integer, Integer> tmp = new HashMap<>();
                for(Integer k: domain){
                    for(Integer t: graph.get(k)){
                        if(var.get(t)>var.get(k)+1){
                            tmp.put(t, var.get(k)+1);
                        }
                        else{
                            tmp.put(t, var.get(t));
                        }
                    }
                }
                return tmp;
            }
        }

        Combiner<Map<Integer, Integer>, Map<Integer, Integer>> combiner = new Combiner<Map<Integer, Integer>, Map<Integer, Integer>>() {
            @Override
            public Map<Integer, Integer> combine() {
                Map<Integer, Integer> ans = new HashMap<>();
                while(!messageQueue.isEmpty()){
                    Map<Integer, Integer> tmp = messageQueue.getFirst().data;
                    messageQueue.poll();
                    for(Integer k: tmp.keySet()){
                        if(!ans.containsKey(k)){
                            ans.put(k, tmp.get(k));
                        }
                        else{
                            ans.put(k, Math.min(tmp.get(k), ans.get(k)));
                        }
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
        Vector<Compute<Map<Integer, Integer>, Map<Integer, Integer>>> compute = new Vector<>();
        for(int i=0;i<threads;i++) compute.add(new ComputeSSSP());
        BSP<Map<Integer, Integer>, Map<Integer, Integer>> bsp = new BSP<Map<Integer, Integer>, Map<Integer, Integer>>(combiner, compute, nodeSet, graph) {
            @Override
            public void init() {
                ans = new HashMap<>();
                int inf = 0x3f3f3f3f;
                for(Integer i:points) ans.put(i, inf);
                for(Integer i: points) active.put(i, true);
                ans.put(0, 0);
                for(Integer t: graph.get(0)) ans.put(t, 1);
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
            public Boolean UpdateActiveMap(Map<Integer, Integer> pre, Map<Integer, Integer> now) {
                boolean ans = false;
                System.out.println(pre);
                System.out.println(now);
                for(Integer k: pre.keySet()){
                    if(now.containsKey(k) && pre.get(k) > now.get(k)){
                        ans = true;
                        active.put(k, true);
                        pre.put(k, now.get(k));
                    }
                    else{
                        active.put(k, false);
                    }
                }
                return ans;
            }
        };

        master.run(bsp);
        Map<Integer, Integer> ans = bsp.getAns();
        for(Integer k: ans.keySet()){
            System.out.print(k);
            System.out.print(": ");
            System.out.println(ans.get(k));
        }

    }
}
