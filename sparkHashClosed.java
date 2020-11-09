// << +1 || >> -1
import java.math.BigInteger;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.io.*;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;

import scala.Tuple2;

public class  sparkHashClosed<K,V> implements Serializable{
	
    static class Bucket<K, V> implements Serializable {
    	    	
    	private class Node{
    	K key;
		V value;
		
		public Node (K key, V value) {
			this.key = key;
			this.value = value;
			}
		
		public String toString() {
			return "("+key.toString()+":"+value.toString()+")";
			}
    	}
    	
        private LinkedList<Node> bucket = new LinkedList<Node>();// We create the bucket
        private List<K> keyset = new ArrayList<K>(); //list of keys

        public V del(K key) {
        	
        	if (bucket == null || !keyset.contains(key))	return null; // if the bucket is null or doesn't contain the key, just return null
        	//else delete
        	ListIterator<Node> it = bucket.listIterator();

    		while(it.hasNext())//let's look for the element
    		{
    			Node n = it.next();
    			if( n.key.equals(key)) {// If we find it, return the value and eliminate it frmo the list and del its key from keyset
    				V val = n.value;
    				it.remove();
    				keyset.remove(key);//removes key entry
    				return val;
    			}
    		}
        	
        	
        	return null;
        	
        }
        
        public void set(K key, V value) {
        	//Maybe we do not need this function
        	add(key,value);
        }
    
        public void add(K key, V value) {
            // put data in bucket and create keyset
        	
            if (keyset.contains(key)) {
               	 del(key); //if key exists already, case of set, then delete the key and its entry on keylist and the element
            }
            keyset.add(key);
            bucket.add(new Node(key,value));
        }

        public V get(K key) {
        	ListIterator<Node> it = bucket.listIterator();//iterates over the linked list of Nodes
			boolean find = false;
			V value = null;
			while(it.hasNext() && !find) { //Try to find the key
				Node n = it.next();
				if(n.key.equals(key)) {//We've found it!
					value = n.value;
					find = true;
					}
				}
            return value;
        }

    }
    
    private int nBuckets;
    private AtomicInteger size = new AtomicInteger(0);
    private List<Bucket<K, V>> bucketlist ;

    public sparkHashClosed(int nBuckets) {
    	this.nBuckets = nBuckets;
    	this.bucketlist = new ArrayList<Bucket<K,V>>(this.nBuckets);
    	for(int i=0; i<this.nBuckets; i++) { 
			this.bucketlist.add(null);
    	}
    }
    
     private int hashCode(K key) {
         return key.hashCode();
    }
     
     private int getIndex(K key) {
    	int hashCode = hashCode(key);
		return ((hashCode % nBuckets)+nBuckets)%nBuckets; 
    }

     public Bucket<K, V> getBucket(K key) { // get bucket based on hashcode(key)
        int index = getIndex(key);
        Bucket<K, V> b = bucketlist.get(index);
        return b;
    }

     public V getValue(K key) {
        int index = getIndex(key);
        Bucket<K, V> b = bucketlist.get(index);
        if(b == null) return null;
        V answer = b.get(key);
        return answer;
    }

     public void put(K key, V value) {
        Bucket<K, V> b = getBucket(key);
        if(b==null) {//if bucket was not initialized before, we initialize it!
        	b = new Bucket<K,V>();
        }
        if(!b.keyset.contains(key)) { size.incrementAndGet();}//we check if the key exists already before adding it!
        b.add(key,value); //add this element to the bucket
        }
            
     public V remove(K key) {
    	 Bucket<K,V> b = getBucket(key);
    	 if(b!=null && b.keyset.contains(key)) { //if b is not null and contains the key, we reduce the size and return the deleted item
    			 size.decrementAndGet();
    			 V deleted= b.del(key);
    			 return deleted;
    		 }
    	 return null; // if b is null or if key doesn't exist in the bucket;
    		 
    	 }
     

    public static void main(String[] args) throws FileNotFoundException {
    	
    	
    	SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkFileSumApp");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sparkHashClosed<String,String> eh = new sparkHashClosed<String,String>(4);
        JavaRDD<String> textFile = sc.textFile("911.csv");

        //Assign DataSet to Java Pair RDD with Keys and values
        JavaPairRDD<String,String> lines = textFile.mapToPair(x -> new Tuple2(x.split(",")[2], x.split(",")[4]));

        //here we measure the time that 605000 data need to insert and then we measure the access time.
        List<Tuple2<String, String>> temp = lines.take(605000);
        JavaRDD<Tuple2<String, String>> first_lines = sc.parallelize(temp);

        long start = System.nanoTime();
        first_lines.map(new Function<Tuple2<String, String>, String>() {
			@Override
			public String call(Tuple2<String, String> v1) throws Exception {
				eh.put((String)v1._1,(String)v1._2);
				return null;
			}
        });

        long finish = System.nanoTime();
        System.out.println(finish - start);

        JavaRDD<Long> times_access = first_lines.map(new Function<Tuple2<String,String>,Long>(){
			@Override
			public Long call(Tuple2<String, String> v1) throws Exception {
				eh.put((String)v1._1,(String)v1._2);
				Long start = System.nanoTime();
				eh.getValue((String)v1._1);
				Long finish = System.nanoTime();
				return finish-start;
			}
        });
        Long total_accesstime = times_access.reduce((a, b) -> a+b);
        System.out.println(total_accesstime);
        sc.close();
    }
        
}




