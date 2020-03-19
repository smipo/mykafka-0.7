package kafka.examples;

import kafka.cluster.Partition;
import kafka.utils.Utils;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

public class Test {

    public static void main(String[] args) {
        SortedSet<Partition> topicPartitionsList = Utils.getTreeSetSet();
        List<Partition> list = new ArrayList<>();
        Partition p1 = new Partition(1,1);
        Partition p2 = new Partition(2,2);
        topicPartitionsList.add(p1);
        topicPartitionsList.add(p2);
        list.add(p1); list.add(p2);
        Partition[] pd = new Partition[list.size()];
        Partition[] p = list.toArray(pd);
    }
}
