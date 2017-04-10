package cs276.assignments;

import cs276.util.Pair;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.LinkedList;
import java.util.List;

public class Index {

  // Term id -> (position in index file, doc frequency) dictionary
  private static Map<Integer, Pair<Long, Integer>> postingDict
    = new TreeMap<Integer, Pair<Long, Integer>>();
  // Doc name -> doc id dictionary
  private static Map<String, Integer> docDict
    = new TreeMap<String, Integer>();
  // Term -> term id dictionary
  private static Map<String, Integer> termDict
    = new TreeMap<String, Integer>();
  // Block queue
  private static LinkedList<File> blockQueue
    = new LinkedList<File>();

  // Total file counter
  private static int totalFileCount = 0;
  // Document counter
  private static int docIdCounter = 0;
  // Term counter
  private static int wordIdCounter = 0;
  // Index
  private static BaseIndex index = null;


  /*
   * Write a posting list to the given file
   * You should record the file position of this posting list
   * so that you can read it back during retrieval
   *
   * */
  private static void writePosting(FileChannel fc, PostingList posting)
      throws IOException {
    writePosting(fc,posting,false);
  }
  private static void writePosting(FileChannel fc, PostingList posting, boolean isLastRound)
      throws IOException {
    try {
      // make sure that the doc id's are increasing
      if (isLastRound){
        Collections.sort(posting.getList());
      }
      index.writePosting(fc, posting);
    } catch (Throwable throwable) {
      throw new IOException(throwable);
    }
  }
  private static class RAFIterator {
    FileChannel fc;
    public RAFIterator(FileChannel fc){
      this.fc = fc;
    }
    public boolean hasNext() throws IOException {
      return this.fc.position()<this.fc.size();
    }
    public PostingList  next() throws Throwable {
      return index.readPosting(this.fc);
    }
  }
  public static void main(String[] args) throws Throwable {
    /* Parse command line */
    if (args.length != 3) {
      System.err
          .println("Usage: java Index [Basic|VB|Gamma] data_dir output_dir");
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

    /* Get root directory */
    String root = args[1];
    File rootdir = new File(root);
    if (!rootdir.exists() || !rootdir.isDirectory()) {
      System.err.println("Invalid data directory: " + root);
      return;
    }

    /* Get output directory */
    String output = args[2];
    File outdir = new File(output);
    if (outdir.exists() && !outdir.isDirectory()) {
      System.err.println("Invalid output directory: " + output);
      return;
    }

    if (!outdir.exists()) {
      if (!outdir.mkdirs()) {
        System.err.println("Create output directory failure");
        return;
      }
    }

    /* A filter to get rid of all files starting with .*/
    FileFilter filter = new FileFilter() {
      @Override
      public boolean accept(File pathname) {
        String name = pathname.getName();
        return !name.startsWith(".");
      }
    };

    /* BSBI indexing algorithm */
    File[] dirlist = rootdir.listFiles(filter);

    /* For each block */
    for (File block : dirlist) {
      File blockFile = new File(output, block.getName());
      blockQueue.add(blockFile);
      // both term id and doc id list will be sorted. TreeMap sorts the term id first and doc id will be sorted upon write
      Map<Integer, Set<Integer>> termIdToDocIdMap = new TreeMap<>();
      File blockDir = new File(root, block.getName());
      File[] filelist = blockDir.listFiles(filter);

      /* For each file */
      for (File file : filelist) {
        ++totalFileCount;
        String fileName = block.getName() + "/" + file.getName();
        Integer currDocId = docIdCounter;
        docDict.put(fileName, docIdCounter++);

        BufferedReader reader = new BufferedReader(new FileReader(file));
        String line;
        while ((line = reader.readLine()) != null) {
          String[] tokens = line.trim().split("\\s+");
          for (String token : tokens) {
            // update term dic and mapping between termId and docId list
            Integer tokenId = updateTermDic(token);
            // within updateTermIdToDocIdMap PostingList has sorted docId
            updateTermIdToDocIdMap(termIdToDocIdMap, tokenId, currDocId);
          }
        }
        reader.close();
      }

      /* Sort and output */
      if (!blockFile.createNewFile()) {
        System.err.println("Create new block failure.");
        return;
      }

      RandomAccessFile bfc = new RandomAccessFile(blockFile, "rw");
      // each record is [termId_freq_list of docId's]
      for (Map.Entry<Integer, Set<Integer>> entry : termIdToDocIdMap.entrySet()) {
        // already sorted
        LinkedList<Integer> tempList = new LinkedList<>(entry.getValue());
        writePosting(bfc.getChannel(),new PostingList(entry.getKey(), tempList));
      }


      bfc.close();
    }

    /* Required: output total number of files. */
    System.out.println(totalFileCount);

    /* Merge blocks */
    while (true) {
      if (blockQueue.size() <= 1)
        break;

      File b1 = blockQueue.removeFirst();
      File b2 = blockQueue.removeFirst();

      File combfile = new File(output, b1.getName() + "+" + b2.getName());
      if (!combfile.createNewFile()) {
        System.err.println("Create new block failure.");
        return;
      }

      RandomAccessFile bf1 = new RandomAccessFile(b1, "r");
      RandomAccessFile bf2 = new RandomAccessFile(b2, "r");
      RandomAccessFile mf = new RandomAccessFile(combfile, "rw");
      FileChannel combinedFC = mf.getChannel();
      mergePostingLists(bf1.getChannel(),bf2.getChannel(),combinedFC, blockQueue.size()==0);
      // starting from the beginning of the file again
      combinedFC.position(0);
      while(combinedFC.position() < combinedFC.size()){
        Long startingPosition = combinedFC.position();
        PostingList tmp = index.readPosting(combinedFC);
        postingDict.put(tmp.getTermId(),new Pair<>(startingPosition,tmp.getList().size()));

      }
      bf1.close();
      bf2.close();
      mf.close();
      b1.delete();
      b2.delete();
      blockQueue.add(combfile);
    }

    /* Dump constructed index back into file system */
    File indexFile = blockQueue.removeFirst();
    indexFile.renameTo(new File(output, "corpus.index"));

    BufferedWriter termWriter = new BufferedWriter(new FileWriter(new File(
        output, "term.dict")));
    for (String term : termDict.keySet()) {
      termWriter.write(term + "\t" + termDict.get(term) + "\n");
    }
    termWriter.close();

    BufferedWriter docWriter = new BufferedWriter(new FileWriter(new File(
        output, "doc.dict")));
    for (String doc : docDict.keySet()) {
      docWriter.write(doc + "\t" + docDict.get(doc) + "\n");
    }
    docWriter.close();

    BufferedWriter postWriter = new BufferedWriter(new FileWriter(new File(
        output, "posting.dict")));
    for (Integer termId : postingDict.keySet()) {
      postWriter.write(termId + "\t" + postingDict.get(termId).getFirst()
          + "\t" + postingDict.get(termId).getSecond() + "\n");
    }
    postWriter.close();
  }

  private static void  mergePostingLists(FileChannel b1List, FileChannel b2List,FileChannel b12List, boolean isLastRound)
      throws Throwable {
    //TODO improve to no additional data structure just merge with initial block sorted and merge on the way
    List<PostingList> merged = new LinkedList<>();
    RAFIterator b1Itr = new RAFIterator(b1List);
    RAFIterator b2Itr = new RAFIterator(b2List);
    PostingList b1Temp = null;
    PostingList b2Temp = null;
    while((b1Itr.hasNext() || b1Temp != null) && (b2Itr.hasNext() || b2Temp != null)){
      b1Temp = (b1Temp == null ? b1Itr.next(): b1Temp);
      b2Temp = (b2Temp == null ? b2Itr.next(): b2Temp);
      if (b1Temp.getTermId() == b2Temp.getTermId()){
//        HashSet<Integer> duplicateRemovalSet = new HashSet<>();
//        duplicateRemovalSet.addAll(b1Temp.getList());
//        duplicateRemovalSet.addAll(b1Temp.getList());
//        b1Temp.getList().clear();
//        b1Temp.getList().addAll(duplicateRemovalSet);
        // Since there is no duplicate with our block solution. We do not need to check.
        b1Temp.getList().addAll(b2Temp.getList());
        b2Temp = null;
      }else{
        if (b1Temp.getTermId() > b2Temp.getTermId()){
          writePosting(b12List,b2Temp,isLastRound);
          b2Temp = null;
        }else{
          writePosting(b12List,b1Temp,isLastRound);
          b1Temp = null;
        }
      }
    }
    if (b1Temp != null){
      writePosting(b12List,b1Temp,isLastRound);
      while(b1Itr.hasNext()){
        writePosting(b12List,b1Itr.next(),isLastRound);
      }
    }
    if (b2Temp != null){
      merged.add(b2Temp);
      while(b2Itr.hasNext()){
        writePosting(b12List,b2Itr.next(),isLastRound);
      }
    }
  }

  private static List<PostingList> parsePostingList(RandomAccessFile bf1) {
    List<PostingList> b1List = new LinkedList<>();
    try {
      FileChannel fc = bf1.getChannel();
      while (fc.position() < fc.size()) {
        b1List.add(index.readPosting(fc));
      }

    } catch (Throwable e) {
      e.printStackTrace();
    }
    return b1List;
  }

  private static void updateTermIdToDocIdMap(Map<Integer, Set<Integer>> termIdToDocIdMap, Integer tokenId,
      Integer currDocId) {
    if (!termIdToDocIdMap.containsKey(tokenId)){
      // only need to be unique not ordered. only sort in the end
      termIdToDocIdMap.put(tokenId, new HashSet<Integer>());
    }
    termIdToDocIdMap.get(tokenId).add(currDocId);
  }

  private static Integer updateTermDic(String token) {
    Integer tokenId = null;
    // take care of tokenId
    if (termDict.containsKey(token)){
      tokenId = termDict.get(token);
    }else{
      tokenId = wordIdCounter++;
      termDict.put(token, tokenId);
    }
    return tokenId;
  }

}
