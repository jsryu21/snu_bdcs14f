package edu.snu.cms.bdcs.assignment.data.ymusic;

import edu.snu.cms.bdcs.assignment.data.Parser;
import edu.snu.cms.bdcs.assignment.data.Rate;

import javax.inject.Inject;

/**
 * Parse the Yahoo music data from the input
 */
public final class MusicDataParser implements Parser<String> {
  @Inject
  public MusicDataParser() {
  }

  @Override
  public Rate parse(String input) {
    final String[] split = input.split("\t");
    return new MusicRate(new Integer(split[0]), new Integer(split[1]), new Byte(split[2]));
  }
}