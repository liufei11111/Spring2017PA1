package cs276.assignments;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

public class Query {

  // Term id -> position in index file
  private static Map<Integer, Long> posDict = new TreeMap<Integer, Long>();
  // Term id -> document frequency
  private static Map<Integer, Integer> freqDict = new TreeMap<Integer, Integer>();
  // Doc id -> doc name dictionary
  private static Map<Integer, String> docDict = new TreeMap<Integer, String>();
  // Term -> term id dictionary
  private static Map<String, Integer> termDict = new TreeMap<String, Integer>();
  // Index
  private static BaseIndex index = null;


  /*
   * Write a posting list with a given termID from the file
   * You should seek to the file position of this specific
   * posting list and read it back.
   * */
  private static PostingList readPosting(FileChannel fc, int termId)
      throws IOException {
    try {
      Long location = posDict.get(termId);
      fc = fc.position(location);
      PostingList result = index.readPosting(fc);
//      System.err.println("location: "+location +", PostingList: "+result);
      return result;
    } catch (Throwable throwable) {
      throw new IOException(throwable);
    }
  }

  public static void main(String[] args) throws IOException {
    /* Parse command line */
    if (args.length != 2) {
      System.err.println("Usage: java Query [Basic|VB|Gamma] index_dir");
      return;
    }

    /* Get index */
    String className = "cs276.assignments." + args[0] + "Index";
    try {
      Class<?> indexClass = Class.forName(className);
      index = (BaseIndex) indexClass.newInstance();
    } catch (Exception e) {
      System.err
          .println("Index method must be \"Basic\", \"VB\", or \"Gamma\"");
      throw new RuntimeException(e);
    }

    /* Get index directory */
    String input = args[1];
    File inputdir = new File(input);
    if (!inputdir.exists() || !inputdir.isDirectory()) {
      System.err.println("Invalid index directory: " + input);
      return;
    }

    /* Index file */
    RandomAccessFile indexFile = new RandomAccessFile(new File(input,
        "corpus.index"), "r");

    String line = null;
    /* Term dictionary */
    BufferedReader termReader = new BufferedReader(new FileReader(new File(
        input, "term.dict")));
    while ((line = termReader.readLine()) != null) {
      String[] tokens = line.split("\t");
      termDict.put(tokens[0], Integer.parseInt(tokens[1]));
    }
    termReader.close();

    /* Doc dictionary */
    BufferedReader docReader = new BufferedReader(new FileReader(new File(
        input, "doc.dict")));
    while ((line = docReader.readLine()) != null) {
      String[] tokens = line.split("\t");
      docDict.put(Integer.parseInt(tokens[1]), tokens[0]);
    }
    docReader.close();

    /* Posting dictionary */
    BufferedReader postReader = new BufferedReader(new FileReader(new File(
        input, "posting.dict")));
    while ((line = postReader.readLine()) != null) {
      String[] tokens = line.split("\t");
      posDict.put(Integer.parseInt(tokens[0]), Long.parseLong(tokens[1]));
      freqDict.put(Integer.parseInt(tokens[0]),
          Integer.parseInt(tokens[2]));
    }
    postReader.close();

    /* Processing queries */
    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

    /* For each query */
    while ((line = br.readLine()) != null) {
      Query query = new Query();
      List<String> queryResult = query.conjunctionBinaryQuery(indexFile, line);
      if (queryResult == null){
        System.out.println("no results found");
      }else{
        Collections.sort(queryResult);
        for (String str : queryResult) {
          System.out.println(str);
        }
      }
    }
    br.close();
    indexFile.close();
  }

  private List<String> conjunctionBinaryQuery(RandomAccessFile indexFile, String query)
      throws IOException {
    String[] strs = query.split("\\s+");
    Set<String> unique = new HashSet(Arrays.asList(strs));
    ArrayList<List<Integer>> toMergeLists = new ArrayList<>(unique.size());
    for (String str : unique){
      PostingList postingList = lookupOneStr(str,indexFile);
      if (postingList != null){
        toMergeLists.add(postingList.getList());
      }else{
        // early exit if one of the words is non-existent
        return null;
      }
    }
    Collections.sort(toMergeLists, new Comparator<List<Integer>>() {
      @Override
      public int compare(List<Integer> o1, List<Integer> o2) {
        return o1.size() - o2.size();
      }
    });
    if (toMergeLists.size() == 0){
      return null;
    }else if (toMergeLists.size() == 1 ){
      return translateDocIdToDocName(toMergeLists.get(0));
    }else {
      List<Integer> docIds = mergeIntersection(toMergeLists);
      return translateDocIdToDocName(docIds);
    }
  }

  private List<String> translateDocIdToDocName(List<Integer> docIds) {
    if (docIds == null){
      return null;
    }
    List<String> docNames = new ArrayList<>();
    for (Integer docId : docIds){
      docNames.add(docDict.get(docId));
    }
    return docNames;
  }

  private List<Integer> mergeIntersection(ArrayList<List<Integer>> toMergeLists) {
    List<Integer> shortestList = null;
    List<Integer> secondShort = null;
    int nextShortCand = 1;
    while(nextShortCand < toMergeLists.size()){
      shortestList = toMergeLists.get(0);
      secondShort = toMergeLists.get(nextShortCand++);
//      System.err.println("Shortest: "+listToStr(shortestList));
//      System.err.println("Second short: "+listToStr(secondShort));
      List<Integer> tmp = bimergeIntersect(shortestList,secondShort);
//      System.err.println("After bimerge: "+listToStr(tmp));
      if (tmp.size() == 0){
        return null;
      }else{
        toMergeLists.set(0,tmp);
      }
    }
    return toMergeLists.get(0);
  }
  private String listToStr(List<Integer> list){
    String postingsString = "";
    for ( Integer posting : list){
      postingsString +=" "+posting;
    }
    return postingsString;
  }
  private List<Integer> bimergeIntersect(List<Integer> shortestList, List<Integer> secondShort) {
//    //TODO debug this section
//    TreeSet<Integer> set = new TreeSet<>(shortestList);
//    set.retainAll(secondShort);
//    return new LinkedList<Integer>(set);
    List<Integer> merged = new LinkedList<>();
    if (shortestList.size()==0){
      return merged;
    }else{

      Integer docId1 = null;
      Integer docId2 = null;
      Iterator<Integer> itr1 = shortestList.iterator();
      Iterator<Integer> itr2 = secondShort.iterator();
      while((itr1.hasNext() || docId1 != null) && (itr2.hasNext() || docId2 != null)){
        docId1 = docId1 == null ? itr1.next(): docId1;
        docId2 = docId2 == null ? itr2.next(): docId2;
        if (docId1.equals(docId2)){
          merged.add(docId1);
          docId1 = null;
          docId2 = null;
        }else if (docId1 < docId2){
          docId1 = null;
        }else{
          docId2 = null;
        }
      }
      return merged;
    }
  }

  private PostingList lookupOneStr(String str, RandomAccessFile indexFile) throws IOException {
    Integer termId = termDict.get(str);
    if (termId == null){
      return null;
    }
    FileChannel fc = indexFile.getChannel();
    return readPosting(fc,termId);
  }
}
