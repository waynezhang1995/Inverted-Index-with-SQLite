import java.io.*;
import java.sql.*;
import java.util.*;

public class Index {
	static Connection conn;
	static PreparedStatement stmt_insert_into_postings;
	static PreparedStatement stmt_select_all_from_postings;
	static PreparedStatement stmt_insert_into_docmag;
	static PreparedStatement stmt_select_df_from_postings;
	static PreparedStatement stmt_select_docinfo_from_docmag;
	static SortedMap<String, SortedMap<Integer, Integer>> dictionary;
	static {
		try {
			conn = ConnectionManager.getConnection();
			conn.setAutoCommit(false);
			stmt_insert_into_postings = conn
					.prepareStatement("INSERT INTO postings(word, df, docidfreqbytestream) VALUES(?,?,?)");
			stmt_select_all_from_postings = conn.prepareStatement("SELECT word, df, docidfreqbytestream FROM postings");
			stmt_insert_into_docmag = conn
					.prepareStatement("INSERT INTO docmag(docid, vectormagnitude, maxf) VALUES(?,?,?)");
			stmt_select_df_from_postings = conn.prepareStatement("SELECT df FROM postings WHERE word=?");
			stmt_select_docinfo_from_docmag = conn
					.prepareStatement("SELECT vectormagnitude, maxf FROM docmag WHERE docid=?");
		} catch (Exception e) {
			System.out.println(e);
		}
		;
	}

	//This method outputs word-docid pairs in file worddoclistfilename
	//It removes stop-words.
	public static void fillWordList(String dirPath, String stopwordsfilename, String worddoclistfilename)
			throws Exception {
		// Reading stopwords
		dictionary = new TreeMap<>();
		BufferedReader in_stopwords = new BufferedReader(new FileReader(new File(stopwordsfilename)));
		ArrayList<String> stopWords = new ArrayList<>();
		String read_line = in_stopwords.readLine();
		stopWords.add(read_line);
		while ((read_line = in_stopwords.readLine()) != null) {
			stopWords.add(read_line);
		}

		// Opening the word-doc list output file
		// Reading documents from the directory (folder) provided
		// As we read, we write word-docid pairs to the file.
		final File html_folder = new File(dirPath);
		BufferedWriter out_html = new BufferedWriter(new FileWriter(worddoclistfilename, true));
		for (final File fileEntry : html_folder.listFiles()) { // Read files in the given directory one by one
			BufferedReader in_html = new BufferedReader(new FileReader(new File("./html/" + fileEntry.getName())));
			String filename = fileEntry.getName().substring(0, fileEntry.getName().length() - 5); // Extract DocID
			Integer docID = Integer.parseInt(filename);
			in_html.readLine(); // Skip first two lines
			in_html.readLine();
			while ((read_line = in_html.readLine()) != null) {
				read_line = read_line.replaceAll("[-\"$'()&]", ""); // Filter out special characters
				for (String word : read_line.split("\\s+")) {
					if (!stopWords.contains(word) && word.length() != 0) { // No a stop word
						/**
						 * Creating a posting list using Java SortMap Class
						 * Format: Term, [DocID, TF, 1th gap, TF, 2th gap, TF]
						 * Example: hello, [1, 23,4, 56]
						 * Reference: Slide: 07-Inverted Indexes page 10
						 */
						if (!dictionary.containsKey(word)) {
							dictionary.put(word, new TreeMap<Integer, Integer>());
						}
						SortedMap<Integer, Integer> posting_list = dictionary.get(word);
						if (!posting_list.containsKey(docID)) {
							posting_list.put(docID, 1);
						} else {
							Integer tf = posting_list.get(docID);
							posting_list.put(docID, tf + 1);
						}
						out_html.write(word + "-" + filename); // Output to word_doc_list.txt
						out_html.newLine();
					}
				}
			}
			in_html.close();
		}
		in_stopwords.close();
		out_html.close();
	}

	//It fills the postings table.
	public static void fillWordTable(String sortedWordDocListFilename) throws Exception {

		// TODO: using sorted_word_doc_list.txt in this function

		for (Map.Entry<String, SortedMap<Integer, Integer>> entry : dictionary.entrySet()) {
			String term = entry.getKey(); // Word
			SortedMap<Integer, Integer> posting_list = entry.getValue(); // Posing list associated with the term

			stmt_insert_into_postings.setString(1, term);
			stmt_insert_into_postings.setInt(2, posting_list.size());
			stmt_insert_into_postings.setBytes(3, VB.VBENCODE(Delta.sortedmap2deltalist(posting_list)));
			stmt_insert_into_postings.execute();

			conn.commit();
		}
		ConnectionManager.returnConnection(conn);
	}

	//It reads the postmt_select_docinfo_from_docmagstings table and creates the doc_word_freq_df_list file.
	//The code is complete.
	public static void fillDocWordFreqDfList(String DocWordFreqDfListFilename) throws Exception {
		//Creating the DocWordFreqDfList output file
		BufferedWriter out = new BufferedWriter(new FileWriter(DocWordFreqDfListFilename));

		ResultSet rset = stmt_select_all_from_postings.executeQuery();
		while (rset.next()) {
			String word = rset.getString(1);
			System.out.println(word);
			Integer df = rset.getInt(2);
			byte[] bytestream = rset.getBytes(3);
			Map<Integer, Integer> docid_freq_map = Delta.deltalist2sortedmap(VB.VBDECODE(bytestream));
			for (Integer docid : docid_freq_map.keySet()) {
				out.write(docid + " " + word + " " + docid_freq_map.get(docid) + " " + df + "\n");
			}
		}

		out.close();
		ConnectionManager.returnConnection(conn);
	}

	//It fills the document magnitude table.
	public static void fillDocMagTable(String sortedDocWordDfListFilename, int N /*total number of docs*/)
			throws Exception {

	    //Done by Zhuoli
		/*
		BufferedWriter out = new BufferedWriter(new FileWriter(sortedDocWordDfListFilename));


		//set up communication msg
		stmt_insert_into_docmag.setInt(1, docid);
		stmt_insert_into_docmag.setDouble(2, Index.magnitude(word_freq_map, word_df_map, N));
		stmt_insert_into_docmag.setInt(3, Index.maxf(word_freq_map) );
		stmt_insert_into_docmag.execute();
		//commit and send the msg
		conn.commit();
		ConnectionManager.returnConnection(conn);
		*/
	}

	public static int maxf(Map<String, Integer> word_freq_map) {
		int maxf_res = 0;
		for (String word : word_freq_map.keySet()) {
			if (maxf_res < word_freq_map.get(word))
				maxf_res = word_freq_map.get(word);
		}
		return maxf_res;
	}

	public static double tfidf(int f, int maxf, int df, int N) {
		double tf = f / (double) maxf;
		double idf = Math.log(N / (double) df);
		return tf * idf;
	}

	public static double magnitude(Map<String, Integer> word_freq_map, Map<String, Integer> word_df_map, int N)
			throws Exception {

		double magsquare = 0;
		// Done by Zhuoli
		// find max of words
		int maxfreq=maxf(word_freq_map);
		double [] wij= new double[word_freq_map.size()];
		// compute vector q
		int iterator=0;
		for (String word : word_freq_map.keySet()) {
			//compute wif from f and df
			wij[iterator]=tfidf(word_freq_map.get(word),maxfreq,word_df_map.get(word),N);
			iterator++;
		}
		//computer magsqure buy adding square of each wij
		for(int i=0;i<wij.length;i++){
			magsquare+=wij[i]*wij[i];
		}
		return Math.sqrt(magsquare);
	}

	public static double magnitude(Map<String, Integer> word_freq_map, int N) throws Exception {
		Map<String, Integer> word_df_map = new HashMap<String, Integer>();
		for (String word : word_freq_map.keySet())
			word_df_map.put(word, Index.retrieveDf(word));

		return Index.magnitude(word_freq_map, word_df_map, N);
	}

	public static int retrieveDf(String word) throws Exception {
		int df = 0;
		stmt_select_df_from_postings.setString(1, word);
		ResultSet rset = stmt_select_df_from_postings.executeQuery();
		if (rset.next()) {
			df = rset.getInt(1);
		}
		return df;
	}

	public static DocInfo retrieveDocInfo(Integer docid) throws Exception {
		DocInfo docinfo = new DocInfo();

		stmt_select_docinfo_from_docmag.setInt(1, docid);
		//TO-DO ...
		//Done by Zhuoli
		//in stmt_select_docinfo_from_docmag, 1=vectormagnitude, 2=maxf
		ResultSet rset = stmt_select_docinfo_from_docmag.executeQuery();
		if (rset.next()){
			docinfo.maxf=rset.getInt(2);
			docinfo.magnitude=rset.getDouble(1);
		}
		return docinfo;
	}

	public static void main(String[] args) throws Exception {
		long startTime = System.currentTimeMillis();

		fillWordList("html", //directory with documents
				"stopwords.txt", //stop-words file
				"word_doc_list.txt" //output text file of word-docid pairs.
									//we will sort this file with respect to word (alphabeticaly),
									//then docid (numerically)
		);
		//Mac and Linux users please remove ".exe" from sort
		Runtime.getRuntime().exec("sort -k1,1 -k2n,2 word_doc_list.txt -o sorted_word_doc_list.txt").waitFor();
		fillWordTable("sorted_word_doc_list.txt");

		//This will output (docid, word, freq, df) tuples to a text file, doc_word_freq_df.txt, by reading the index table we just built
		fillDocWordFreqDfList("doc_word_freq_df.txt");
		//Then, we sort this file with respect to docid (numerically), then word (alphabetically)
		Runtime.getRuntime().exec("sort -k1n,1 -k2,2 doc_word_freq_df.txt -o sorted_doc_word_freq_df.txt").waitFor();
		//Finally, we fill in the document magnitude table
		fillDocMagTable("sorted_doc_word_freq_df.txt", 19025);

		System.out.println("Time elapsed (sec) = " + (System.currentTimeMillis() - startTime) / 1000.0);
	}
}
