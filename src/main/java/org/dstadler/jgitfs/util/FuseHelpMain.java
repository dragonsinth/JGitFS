package org.dstadler.jgitfs.util;

import net.fusejna.FuseException;
import net.fusejna.util.FuseFilesystemAdapterFull;

/** Helper class to list all valid FUSE options. */
public class FuseHelpMain {
  public static void main(final String... args) {
    try {
      new FuseFilesystemAdapterFull() {
        @Override protected String[] getOptions() {
          return new String[] {"-h"};
        }
      }.mount(".");
    } catch (FuseException ignored) {
    }
  }
}
