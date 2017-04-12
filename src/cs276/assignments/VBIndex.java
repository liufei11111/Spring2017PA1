package cs276.assignments;

import java.util.List;
import java.util.LinkedList;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class VBIndex implements BaseIndex {
  private static final int INT_SIZE = 4;
  private static final int MASK = 127;
  private static final int MOD = 128;
  private static final int MOD_BITS = 7;

  private void encodeGap(int gap, List<Byte> encoded_gap) {
    if (gap == 0) {
      encoded_gap.add((byte)MOD);
      return;
    }
    int counter = 0;
    while ((gap & MASK) != 0) {
      Byte encoded_gap_part;
      if (counter == 0) {
        encoded_gap_part = (byte)(MOD | (gap & MASK));
      } else {
        encoded_gap_part = (byte)(gap & MASK);
      }
      encoded_gap.add(0, encoded_gap_part);
      counter++;
      gap = gap >> MOD_BITS;
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
    int gap = 0;
    int last_posting = 0;
    for (int i = 0; i < encoded_size; i++) {
      Byte each_byte = bufferForDocIds.get();
      gap = gap << MOD_BITS | (each_byte & MASK);
      if ((each_byte & MOD) != 0) {
        postings.add(last_posting + gap);
        last_posting = last_posting + gap;
        gap = 0;
      }
    }
    return new PostingList(termId,postings);
  }

  @Override
  public void writePosting(FileChannel fc, PostingList p) throws Throwable {
    int bb = 0;
    if (p.getTermId() == 12) {
      bb = 0;
    }
    List<Byte> encoded_posting = new LinkedList<Byte>();
    int pre_doc_id = 0 + bb;
    for (int id : p.getList()) {
      int gap = id - pre_doc_id;
      List<Byte> encoded_gap = new LinkedList<Byte>();
      encodeGap(gap, encoded_gap);
      for (Byte encoded_gap_part : encoded_gap) {
        encoded_posting.add(encoded_gap_part);
      }
      pre_doc_id = id;
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
