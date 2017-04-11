package cs276.assignments;

import java.util.List;
import java.util.LinkedList;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class VBIndex implements BaseIndex {
  private static final int INT_SIZE = 4;
  private static final int MOD = 128;

  private void encodeGap(int gap, List<Byte> encoded_gap) {
    int counter = 0;
    while (gap % MOD != 0) {
      Byte encoded_gap_part;
      if (counter == 0) {
        encoded_gap_part = (byte)(MOD + gap % MOD);
      } else {
        encoded_gap_part = (byte)(gap % MOD);
      }
      encoded_gap.add(0, encoded_gap_part);
      counter++;
      gap = gap / MOD;
    }
  }

  @Override
  public PostingList readPosting(FileChannel fc) throws Throwable {
    ByteBuffer buffer = ByteBuffer.allocate(INT_SIZE * 2);
    int numOfBytesRead;
    try {
      numOfBytesRead = fc.read(buffer);
      if (numOfBytesRead == -1) return null;
    } catch (IOException e) {
      throw e;
    }
    buffer.rewind();
    int termId = buffer.getInt();
    int encoded_size = buffer.getInt();
    ByteBuffer bufferForDocIds = ByteBuffer.allocate(encoded_size);
    try {
      numOfBytesRead = fc.read(bufferForDocIds);
      if (numOfBytesRead == -1) return null;
    } catch (IOException e) {
      throw e;
    }
    bufferForDocIds.rewind();
    List<Integer> postings = new LinkedList<>();
    int posting = 0;
    for (int i = 0; i < encoded_size; i++) {
      Byte each_byte = bufferForDocIds.get();
      posting = posting * MOD + each_byte % MOD;
      if (each_byte / MOD != 0) {
        postings.add(posting);
        posting = 0;
      }
    }
    return new PostingList(termId,postings);
  }

  @Override
  public void writePosting(FileChannel fc, PostingList p) throws Throwable {
    List<Byte> encoded_posting = new LinkedList<Byte>();
    int pre_doc_id = 0;
    for (int id : p.getList()) {
      int gap = id - pre_doc_id;
      List<Byte> encoded_gap = new LinkedList<Byte>();
      encodeGap(gap, encoded_gap);
      for (Byte encoded_gap_part : encoded_gap) {
        encoded_posting.add(encoded_gap_part);
      }
    }
    ByteBuffer buffer = ByteBuffer.allocate(INT_SIZE * 2 + encoded_posting.size());
    buffer.putInt(p.getTermId());
    buffer.putInt(encoded_posting.size());
    for (Byte each_byte : encoded_posting) {
      buffer.put(each_byte);
    }
    buffer.flip();
    try {
      fc.write(buffer);
    } catch (IOException e) {
      throw e;
    }
  }
}
