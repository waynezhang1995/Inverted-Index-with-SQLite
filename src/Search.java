import java.sql.*;
import java.util.*;

public class Search {
	public int N = 21578; //For the reuters collection
	public String[] qword_array;
	public Map<String, Double> qword_tfidf_map;
	public double qmagnitude;

	//result
	Map<Integer, Map<String, Integer>> docid_word_freq_map;
	Map<Integer, Map<String, Double>> docid_word_tfidf_map;
	Map<Integer, DocInfo> docid_info_map;
	Map<Integer,Double> docid_score_map;

	public Search(String querystring) throws Exception
	{
		this.qword_array = querystring.split(" ");
		qword_tfidf_map = new HashMap<String, Double>();

		Map<String, Integer> qword_freq_map = new HashMap<String, Integer>();
		int qmaxf;
		Map<String, Integer> qword_df_map = new HashMap<String, Integer>();

		for(String word : this.qword_array) {
			if ( !qword_freq_map.containsKey(word) )
				qword_freq_map.put( word, 1 );
			else
				qword_freq_map.put( word, qword_freq_map.get(word)+1 );
		}
		qmaxf = Index.maxf(qword_freq_map);

		for(String word : this.qword_array)
			qword_df_map.put(word, Index.retrieveDf(word));

		//creating qword_tfidf_map
		for (String word : this.qword_array) {
			int f = qword_freq_map.get(word);
			int maxf = qmaxf;
			int df = qword_df_map.get(word);
			this.qword_tfidf_map.put(word, Index.tfidf(f, maxf, df, N));
		}

		this.qmagnitude = Index.magnitude(qword_freq_map, qword_df_map, N);
	}


    public Map<Integer, Map<String, Integer>> search() throws Exception
    {
    	docid_word_freq_map = new HashMap<Integer, Map<String, Integer>>();

        //Connecting to database
        Connection conn = ConnectionManager.getConnection();
        PreparedStatement stmt = conn.prepareStatement("SELECT docidfreqbytestream FROM postings WHERE word=?");

        /* TO-DO ...
         * ...*/

				for (String word : qword_array) {
					stmt.setString(1, word);
					ResultSet rset = stmt.executeQuery();
					if (rset.next()) {
						byte[] bytestream = rset.getBytes(1);
						SortedMap<Integer, Integer> docid_freq_map = Delta.deltalist2sortedmap( VB.VBDECODE(bytestream) );
						for (int docid : docid_freq_map.keySet()) {
							if (docid_word_freq_map.get(docid) == null) {
								docid_word_freq_map.put(docid, new HashMap<String, Integer>());
							docid_word_freq_map.get(docid).put(word, docid_freq_map.get(docid));
							}
						}
					}
				}
        ConnectionManager.returnConnection(conn);
    	return docid_word_freq_map;
    }

    //Only the tfidf's for the documents words mentioned in the query matter.
    public Map<Integer, Map<String, Double>> computeDocsTFIDFs() throws Exception
    {
    	docid_word_tfidf_map = new HashMap<Integer, Map<String, Double>>();
    	docid_info_map = new HashMap<Integer, DocInfo>();

        for (Integer docid : docid_word_freq_map.keySet())
        	docid_info_map.put(docid, Index.retrieveDocInfo(docid) );

        for (Integer docid : docid_word_freq_map.keySet())
        {
        	docid_word_tfidf_map.put(docid, new HashMap<String,Double>()); //Initialize for docid

        	for (String word : docid_word_freq_map.get(docid).keySet())
        	{
        		int f = docid_word_freq_map.get(docid).get(word);
            	int maxf = docid_info_map.get(docid).maxf;
            	int df = Index.retrieveDf(word);
            	double tfidf = Index.tfidf(f, maxf, df, N);
            	//System.out.println(docid+":"+word+"   " + "f="+f+", "+  "maxf="+maxf+", "+  "df="+df+", "+  "tfidf="+tfidf);
            	docid_word_tfidf_map.get(docid).put(word, tfidf);
        	}
        }

    	return docid_word_tfidf_map;
    }

    //Only the relevant part of docvector is filled.
    public static double dotproduct(Map<String,Double> docvec, Map<String,Double> queryvec) {
    	double dotproduct = 0;
    	for (String word : queryvec.keySet()) {
    		if (docvec.containsKey(word))
    			dotproduct += queryvec.get(word) * docvec.get(word);
    	}
    	return dotproduct;
    }


    //compute cosine score of each doc w.r.t. query
    //The result is a docid-score map.
    public Map<Integer,Double> score() throws Exception {
    	docid_score_map = new HashMap<Integer,Double>();

    	//TO-DO...
			for (int docid : docid_word_freq_map.keySet()) {
				double docMagnitude = docid_info_map.get(docid).magnitude;
				double cos_score = dotproduct(docid_word_tfidf_map.get(docid), qword_tfidf_map)/(docMagnitude * qmagnitude);
				docid_score_map.put(docid, cos_score);
			}

    	return docid_score_map;
    }

    public Map<Integer,Double> compute() throws Exception {
    	this.search();
    	computeDocsTFIDFs();
    	return score();
    }

    public SortedMap<Double, Set<Integer>> sortResult(Map<Integer,Double> docid_score_map) throws Exception {
    	SortedMap<Double, Set<Integer>> score_docidset_map =
    			new TreeMap<Double, Set<Integer>>(Collections.reverseOrder());
    	for(Integer docid : docid_score_map.keySet()) {
    		Double score = docid_score_map.get(docid);
    		if (!score_docidset_map.containsKey(score))
    			score_docidset_map.put(score, new HashSet<Integer>());
    		score_docidset_map.get(score).add(docid);
    	}
    	return score_docidset_map;
    }

    public static void main(String[] args) throws Exception {
    	//Testing
    	long startTime = System.currentTimeMillis();

    	Search mySearch = new Search("ryoka hydroelectric");
    	System.out.println( mySearch.compute().size() );
    	System.out.println( mySearch.sortResult( mySearch.compute() ) );

    	System.out.println("Time elapsed (sec) = " + (System.currentTimeMillis() - startTime) / 1000.0);
    }
}
