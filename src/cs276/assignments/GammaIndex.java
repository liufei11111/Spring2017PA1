package cs276.assignments;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.LinkedList;
import java.util.List;

public class GammaIndex implements BaseIndex {
  private static final int INT_SIZE = 4;
  private static final int MAX_ENCODING_BITS = 64;

  private int gammaEncodeGap(int gap, boolean[] encoded_gap) {
    gap = gap + 1;
    int counter = 0;
    while(gap > 1) {
      if (gap % 2 == 0) {
        encoded_gap[counter] = false;
      } else {
        encoded_gap[counter] = true;
      }
      gap = gap / 2;
      counter++;
    }
    encoded_gap[counter] = false;
    for (int i = counter + 1; i < 2 * counter + 1; i++) {
      encoded_gap[i] = true;
    }
    for (int i = 0; i < counter; i++) {
      boolean temp = encoded_gap[i];
      encoded_gap[i] = encoded_gap[2 * counter - i];
      encoded_gap[2 * counter - i] = temp;
    }
    counter = 2 * counter + 1;
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
    int last_posting = 0;
    Byte each_byte = 0;
    int b = 0;
    int i = 0;
    boolean read_length_mode = true;
    int gap = 0;
    int length = 0;
    int processing_length = 0;
    while(true) {
      int c = b % 8;
      int bit_mask = 1 << (7 - c);
      if (c == 0) {
        i++;
        if (i > encoded_size) {
          break;
        }
        each_byte = bufferForDocIds.get();
      }
      boolean concern_bit = false;
      if ((bit_mask & each_byte) != 0) {
        concern_bit = true;
      }
      if (read_length_mode) {
        if (concern_bit) {
          length = length + 1;
        } else {
          if (length > 0) {
            read_length_mode = false;
            gap = 1 << length;
            processing_length = 0;
          } else {
            read_length_mode = true;
            gap = 0;
            length = 0;
            processing_length = 0;
            postings.add(last_posting + gap);
            last_posting = last_posting + gap;
          }
        }
      } else {
        if (concern_bit) {
          gap = gap + (1 << (length - 1 - processing_length));
        }
        processing_length++;
        if (processing_length == length) {
          read_length_mode = true;
          gap = gap - 1;
          length = 0;
          processing_length = 0;
          postings.add(last_posting + gap);
          last_posting = last_posting + gap;
          gap = 0;
        }
      }
      b++;
    }
    return new PostingList(termId, postings);
  }

  @Override
  public void writePosting(FileChannel fc, PostingList p) throws Throwable {
    List<Byte> encoded_posting = new LinkedList<Byte>();
    int pre_doc_id = 0;
    int index = 0;
    byte part_bytes = 0;
    for (int id : p.getList()) {
      int gap = id - pre_doc_id;
      boolean[] encoded_gap = new boolean[MAX_ENCODING_BITS];
      int encoded_gap_length = gammaEncodeGap(gap, encoded_gap);
      for (int i = 0; i < encoded_gap_length; i++) {
        part_bytes = (byte)(part_bytes * 2 + (encoded_gap[i] ? 1 : 0));
        if (index % 8 == 7) {
          encoded_posting.add(part_bytes);
          part_bytes = 0;
        }
        index++;
      }
      pre_doc_id = id;
    }
    if (index % 8 != 0) {
      for (int i = 0; i < 8 - index % 8; i++) {
        part_bytes = (byte)(part_bytes * 2 + 1);
      }
      encoded_posting.add(part_bytes);
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
