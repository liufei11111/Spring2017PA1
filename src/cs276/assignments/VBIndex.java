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
  private static final int MAX_ENCODE_LENGTH = 5;

  private int encodeGap(int gap, Byte[] encoded_gap) {
    if (gap == 0) {
      encoded_gap[0] = (byte)MOD;
      return 1;
    }
    int counter = 0;
    while (gap != 0) {
      Byte encoded_gap_part;
      if (counter == 0) {
        encoded_gap_part = (byte)(MOD | (gap & MASK));
      } else {
        encoded_gap_part = (byte)(gap & MASK);
      }
      encoded_gap[counter] =  encoded_gap_part;
      counter++;
      gap = gap >> MOD_BITS;
    }
    for (int i = 0; i <= counter / 2 - 1; i++) {
      Byte temp = encoded_gap[i];
      encoded_gap[i] = encoded_gap[counter - 1 - i];
      encoded_gap[counter - 1 - i] = temp;
    }
    return counter;
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
    return new PostingList(termId, postings);
  }

  @Override
  public void writePosting(FileChannel fc, PostingList p) throws Throwable {
    List<Byte> encoded_posting = new LinkedList<Byte>();
    int pre_doc_id = 0;
    for (int id : p.getList()) {
      int gap = id - pre_doc_id;
      Byte[] encoded_gap = new Byte[MAX_ENCODE_LENGTH];
      int encoded_gap_length = encodeGap(gap, encoded_gap);
      for (int i = 0; i < encoded_gap_length; i++) {
        encoded_posting.add(encoded_gap[i]);
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
