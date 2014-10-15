package edu.snu.cms.bdcs.assignment;

import com.microsoft.reef.io.data.loading.api.DataSet;
import com.microsoft.reef.io.network.util.Utils;
import com.microsoft.reef.task.Task;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by yunseong on 10/14/14.
 */
public class LineCountingTask implements Task {

  private static final Logger LOG = Logger.getLogger(LineCountingTask.class.getName());

  private final DataSet<LongWritable, Text> dataSet;

  @Inject
  public LineCountingTask(final DataSet<LongWritable, Text> dataSet) {
    this.dataSet = dataSet;
  }

  @Override
  public byte[] call(final byte[] memento) throws Exception {
    LOG.log(Level.FINER, "LineCounting task started");
    int numEx = 0;
    for (final Utils.Pair<LongWritable, Text> keyValue : dataSet) {
      LOG.log(Level.FINEST, "Read line: {0}", keyValue);
      ++numEx;
    }
    LOG.log(Level.FINER, "LineCounting task finished: read {0} lines", numEx);
    return Integer.toString(numEx).getBytes();
  }
}
