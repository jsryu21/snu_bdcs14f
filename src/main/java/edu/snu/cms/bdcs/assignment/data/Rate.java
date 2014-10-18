package edu.snu.cms.bdcs.assignment.data;

/**
 * It represent a rate data
 */
public interface Rate {
  /**
   * @return The rate value
   */
  public byte getRate();

  /**
   * @return The user id who rated this rate value
   */
  public int getUserId();

  /**
   * @return The item which has this rate value
   */
  public int getItemId();
}
