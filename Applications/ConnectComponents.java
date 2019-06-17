package Applications;

import System.Master;
import System.Message;
import System.Compute;
import System.Combiner;
import System.BSP;

import java.io.FileNotFoundException;
import java.util.*;
 /*
public class ConnectComponents {
   public static void main(String[] argv) throws FileNotFoundException {
        class NodeMessage extends Message<HashMap<Integer, Vector<Integer>>> {

            @Override
            public void process() {
                msg_var = 1;
                return;
            }
        }

        class CCMaster extends Master<NodeMessage>{

            public CCMaster() {
                super();
            }

            @Override
            public void DetailedGraphGenerator() {
                return;
            }
        }
        CCMaster master = new CCMaster();
        master.setGraphDir("./data/testCC.txt");
        master.setMaxPts(5);
        //master.setSetupDir();
        Vector<Set<Integer>> ans = master.connectedComponents();
        int cnt = 0;
        for(Set<Integer> tmp: ans){
            System.out.println("CC" + String.valueOf(cnt++));
            for(Integer i: tmp){
                System.out.print(i);
                System.out.print("\t");
            }
            System.out.println();
        }
    }
}
*/

 public class ConnectComponents {
     public static void main(String[] argv) throws FileNotFoundException {
         int threads = 3;
         Master<Map<Integer, Integer>, Vector<Set<Integer>>> master =
                 new Master<Map<Integer, Integer>, Vector<Set<Integer>>>("./data/testCC.txt", threads);
         //Compute<Map<Integer, Integer>> compute = new Compute<Map<Integer, Integer>>()
         class ComputeCC extends Compute<Map<Integer, Integer>, Vector<Set<Integer>>> {
             @Override
             public Map<Integer, Integer> process(Map<Integer, Set<Integer>> graph, Set<Integer> domain, Vector<Set<Integer>> var) {
                 Map<Integer, Integer> poi = new HashMap<>();
                 Map<Integer, Integer> rank = new HashMap<>();
                 for(Integer k: domain){
                     poi.put(k,k);
                     rank.put(k, 0);
                     for(Integer t: graph.get(k)){
                         poi.put(t,t);
                         rank.put(t, 0);
                     }
                 }
                 for(Integer k: domain){
                     int x = find(poi, k);
                     //poi.put(k, x);
                     for(Integer t: graph.get(k)){
                         int y = find(poi, t);

                         if(rank.get(x) < rank.get(y)){
                             poi.put(y, x);
                             //poi.put(t, x);
                         }
                         else{
                             poi.put(x, y);
                             //poi.put(t, y);
                         }
                         if(rank.get(x).equals(rank.get(y))) rank.put(y, rank.get(y)+1);
                     }
                 }
                 Map<Integer, Integer> recoll = new HashMap<>();
                 for(Integer k: poi.keySet()){
                     recoll.put(k, find(poi, k));
                 }
                 return recoll;
             }

             private int find(Map<Integer, Integer> poi, int x){
                 while(x!=poi.get(x)) x = poi.get(x);
                 return x;
             }
         }
         Combiner<Map<Integer, Integer>, Vector<Set<Integer>>> combiner  = new Combiner<Map<Integer, Integer>, Vector<Set<Integer>>>() {
             @Override
             public Vector<Set<Integer>> combine() {
                 System.out.println("Combining");
                 System.out.println(messageQueue.size());
                 Vector<Set<Integer>> ans = new Vector<>();
                 Map<Integer, Integer> msgmp = new HashMap<>();
                 Map<Integer, Set<Integer>> mip = new HashMap<>();
                 int cnt = 0;
                 while(!messageQueue.isEmpty()){
                     Message<Map<Integer, Integer>> msg = messageQueue.getFirst();
                     Map<Integer, Integer> data = msg.data;
                     messageQueue.poll();
                     Map<Integer, Set<Integer>> tmp = new HashMap<>();
                     int mk = -1;
                     for(Integer k: data.keySet()){
                         if(!tmp.containsKey(data.get(k))) tmp.put(data.get(k), new HashSet<>());
                         tmp.get(data.get(k)).add(k);
                     }
                     boolean flag = false;
                     for(Integer k: tmp.keySet()){
                         for(Integer t: tmp.get(k)){
                             if(msgmp.containsKey(t)){
                                 mip.get(msgmp.get(t)).addAll(tmp.get(k));
                                 flag = true;
                                 break;
                             }
                         }
                         if(!flag){
                             for(Integer t:tmp.get(k)){
                                 msgmp.put(t, k);
                             }
                             mip.put(k, tmp.get(k));
                         }
                     }
                 }
                 for(Integer k: mip.keySet()){
                     ans.add(mip.get(k));
                 }
                 System.out.println("Done compute");
                 return ans;
             }
         };
         Vector<Map<Integer, Set<Integer>>> partedGraph = master.naiveAveragePartition();
         Vector<Set<Integer>> nodeSet = new Vector<>();
         Map<Integer, Set<Integer>> graph = master.getGraph();
         for(Map<Integer, Set<Integer>> p: partedGraph) nodeSet.add(p.keySet());
         Vector<Compute<Map<Integer, Integer>, Vector<Set<Integer>>>> compute = new Vector<>();
         for(int i=0; i< threads;i++) compute.add(new ComputeCC());
         BSP<Map<Integer, Integer>, Vector<Set<Integer>>> bsp = new BSP<Map<Integer, Integer>, Vector<Set<Integer>>>(combiner, compute, nodeSet,graph) {
             @Override
             public Boolean UpdateActiveMap(Vector<Set<Integer>> pre, Vector<Set<Integer>> now) {
                 return false;
             }

             @Override
             public void init() {

             }

             @Override
             public void printResult(){
                 int cnt = 0;
                 for(Set<Integer> s: getAns()){
                     System.out.println(cnt++);
                     for(Integer t: s){
                         System.out.print(t);
                         System.out.print(" ");
                     }
                     System.out.println();
                 }
             }
         };
         master.run(bsp);
         Vector<Set<Integer>> ans = master.getAns();
         int cnt = 0;
         for(Set<Integer> s: ans){
             System.out.print("CC: ");
             System.out.println(cnt++);
             for(Integer i: s){
                 System.out.print(i);
                 System.out.print(' ');
             }
             System.out.println();
         }
         return;
     }
 }