package edu.snu.cms.bdcs.assignment.data.ymusic;

import edu.snu.cms.bdcs.assignment.data.Rate;

/**
 * Rate object used for Yahoo music data set
 */
public final class MusicRate implements Rate {
  private int userId;
  private int musicId;
  private long rate;

  public MusicRate(int userId, int musicId, long rate) {
    this.userId = userId;
    this.musicId = musicId;
    this.rate = rate;
  }

  @Override
  public long getRate() {
    return this.rate;
  }

  @Override
  public int getUserId() {
    return this.userId;
  }

  @Override
  public int getItemId() {
    return this.musicId;
  }

  @Override
  public String toString() {
    return String.format("User : %d / Music : %d / Rate : %d", userId, musicId, rate);
  }
}
