package edu.snu.cms.bdcs.assignment.data;

/**
 * Parses the input into Rate object
 */
public interface Parser<T> {
  public Rate parse(final T input);
}
