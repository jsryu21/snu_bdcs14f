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
  private final Map<Integer, Map<Integer, Byte>> iRates = new HashMap<>();
  private final Map<Integer, Map<Integer, Byte>> uRates = new HashMap<>();

  private int maxUid = 0;
  private int maxIid = 0;

  @Inject
  public RateList(final DataSet<LongWritable, Text> dataSet, final Parser<String> parser) {
    this.dataSet=dataSet;
    this.parser=parser;
  }

  // TODO Convert Rate matrix to be sparse matrix
  public Map<Integer, Map<Integer, Byte>> getItemRate() {
    if(iRates.isEmpty()) {
      loadData();
    }
    return iRates;
  }

  public Map<Integer, Map<Integer, Byte>> getUserRate() {
    if(uRates.isEmpty()) {
      loadData();
    }
    return uRates;
  }

  public int getMaxUid() {
    return maxUid;
  }
  public int getMaxIid() {
    return maxIid;
  }

  private void loadData() {
    /*
     * Load data so that make two replicated version of R
     * (One grouped by user, the other grouped by item)
     */
    int nU = 0, nI = 0;
    for (final Pair<LongWritable, Text> ratePair : dataSet) {
      final Rate rate = parser.parse(ratePair.second.toString());
      final int uid = rate.getUserId();
      final int iid = rate.getItemId();
      final Byte r = rate.getRate();
      updateMax(uid, iid);

      if(!uRates.containsKey(uid)) {
        uRates.put(uid, new HashMap());
        nU++;
      }
      uRates.get(uid).put(iid, r);

      if(!iRates.containsKey(iid)) {
        iRates.put(iid, new HashMap());
        nI++;
      }
      iRates.get(iid).put(uid, r);
    }
    LOG.info("Num of user : "+nU+" / Num of Item : "+nI);
  }

  private void updateMax(int uid, int iId) {
    if(maxUid < uid)
      maxUid = uid;

    if(maxIid < iId)
      maxIid = iId;
  }

  public void addUserData(Map<Integer, Map<Integer, Byte>> newData) {
    // Not to waste memory (of course I already do a lot)
    if (!uRates.isEmpty())
      uRates.clear();
    uRates.putAll(newData);
  }

  public void addItemData(Map<Integer, Map<Integer, Byte>> newData) {
    // Not to waste memory (of course I already do a lot)
    if (!iRates.isEmpty())
      iRates.clear();
    iRates.putAll(newData);
  }

}
