package System;

import javafx.util.Pair;
import scala.Int;

import java.util.*;

public abstract class BSP<MessageDataType, AnsValueType> {
    public Map<Integer, Boolean> active = new HashMap<>();
    //Vector<Compute> pendingThread = new Vector<Compute>();
    public Vector<Compute<MessageDataType, AnsValueType>> compute;
    Combiner<MessageDataType, AnsValueType> combine;
    Vector<Set<Integer>> partResult;
    public AnsValueType ans;
    public Map<Integer, Set<Integer>> graph;

    public BSP(Combiner<MessageDataType, AnsValueType> comb, Vector<Compute<MessageDataType, AnsValueType>> comp, Vector<Set<Integer>> nodeSet, Map<Integer, Set<Integer>> graph){
        combine = comb;
        compute = comp;
        for(Set<Integer> key: nodeSet) {
            for(Integer item: key)
                active.put(item, true);
        }
        partResult = nodeSet;
        this.graph = graph;
    }

    public abstract void init();


    public void cyclic(Map<Integer, Set<Integer>> graph){
        init();
        Boolean stat = true;
        Vector<Set<Integer>> pending = partResult;
        //long start = System.currentTimeMillis();
        while(stat){
            for(int i=0;i<pending.size();i++){
                Worker<MessageDataType, AnsValueType> work = new Worker<MessageDataType, AnsValueType>(graph, partResult.get(i), compute.get(i), combine);
                work.setReference(ans);
                work.run();
            }
            AnsValueType tmp = combine.combine();
            stat = UpdateActiveMap(ans, tmp);
            ans = tmp;
            pending = checkActive();
            printResult();
        }
        //long end = System.currentTimeMillis();
        //System.out.println("Total Time:");
        //System.out.println(end-start);
    }

    public abstract void printResult();

    public AnsValueType getAns() {
        return ans;
    }

    public abstract Boolean UpdateActiveMap(AnsValueType pre, AnsValueType now);

    private Vector<Set<Integer>> checkActive(){
        Vector<Set<Integer>> checkP = new Vector<>();
        for(Set<Integer> s: partResult){
            Set<Integer> tmp = new HashSet<>();
            for(Integer t: s){
                if(active.get(t))
                    tmp.add(t);
            }
            checkP.add(tmp);
        }
        return checkP;
    }
}
