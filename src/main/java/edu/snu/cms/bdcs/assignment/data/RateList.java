package edu.snu.cms.bdcs.assignment.data;

import com.microsoft.reef.io.data.loading.api.DataSet;
import com.microsoft.reef.io.network.util.Pair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 *
 */
public class RateList {
  private static final Logger LOG = Logger.getLogger(RateList.class.getName());

  private final DataSet<LongWritable, Text> dataSet;
  private final Parser<String> parser;
  private final Map<Integer, Map<Integer, Long>> colRates = new HashMap<>();
  private final Map<Integer, Map<Integer, Long>> rowRates = new HashMap<>();

  private int maxUid = 0;
  private int maxIid = 0;
  @Inject
  public RateList(final DataSet<LongWritable, Text> dataSet, final Parser<String> parser) {
    this.dataSet=dataSet;
    this.parser=parser;
  }

  public Map<Integer, Map<Integer, Long>> getColRate() {
    if(rowRates.isEmpty()) {
      loadData();
    }
    return colRates;
  }

  public Map<Integer, Map<Integer, Long>> getRowRate() {
    if(colRates.isEmpty()) {
      loadData();
    }
    return rowRates;
  }

  // TODO Do we need this?
  public int getMaxUid() {
    return maxUid;
  }
  public int getMaxIid() {
    return maxIid;
  }

  private void loadData() {
    for (final Pair<LongWritable, Text> ratePair : dataSet) {
      final Rate rate = parser.parse(ratePair.second.toString());
      final int uid = rate.getUserId();
      final int iid = rate.getItemId();
      final Long r = rate.getRate();
      updateMax(uid, iid);

      if(!rowRates.containsKey(uid)) {
        rowRates.put(uid, new HashMap());
      }
      rowRates.get(uid).put(iid, r);

      if(!colRates.containsKey(iid)) {
        colRates.put(iid, new HashMap());
      }
      colRates.get(iid).put(uid, r);
    }
  }

  private void updateMax(int uid, int iId) {
    if(maxUid < uid)
      maxUid = uid;

    if(maxIid < iId)
      maxIid = iId;
  }
}
