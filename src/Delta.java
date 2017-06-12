import java.util.*;

public class Delta {
    public static List<Integer> sortedmap2deltalist (SortedMap<Integer, Integer> docid_freq_map) {
    	List<Integer> delta_freq_list = new ArrayList<Integer> ();
    	Integer current_docid = 0;
    	for (Integer docid : docid_freq_map.keySet()) {
    		delta_freq_list.add(docid - current_docid);
    		delta_freq_list.add(docid_freq_map.get(docid));
    		current_docid = docid;
    	}
    	return delta_freq_list;
    }
    
    public static SortedMap<Integer, Integer> deltalist2sortedmap (List<Integer> delta_freq_list) {
    	SortedMap<Integer, Integer> docid_freq_map = new TreeMap<Integer, Integer> ();
    	Integer current_docid = 0;
    	for (int i=0; i+1<delta_freq_list.size(); i+=2) {
    		Integer docid = delta_freq_list.get(i) + current_docid;
    		Integer freq = delta_freq_list.get(i+1);
    		docid_freq_map.put(docid, freq);
    		current_docid = docid; 
    	}
    	return docid_freq_map;
    }  
        
    public static void main(String[] args) throws Exception {
    	//Testing
        SortedMap<Integer, Integer> docid_freq_map = new TreeMap<Integer, Integer> ();
        docid_freq_map.put(345, 7); docid_freq_map.put(346, 8); docid_freq_map.put(375, 1); docid_freq_map.put(445, 9);
        System.out.println( Delta.sortedmap2deltalist(docid_freq_map) );
        System.out.println( Delta.deltalist2sortedmap( Delta.sortedmap2deltalist(docid_freq_map) ) );      
    }
}
