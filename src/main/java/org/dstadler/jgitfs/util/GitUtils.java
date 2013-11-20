package org.dstadler.jgitfs.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;

/**
 * Utilities for checking if pathes match certain patterns to
 * decide at which level in the filesystem we are currently as
 * well as other utilities and constants.
 *
 * @author cwat-dstadler
 */
public class GitUtils {
	public final static String COMMIT_SLASH = "/commit/";
	public final static int COMMIT_SLASH_LENGTH = COMMIT_SLASH.length();

	public final static String BRANCH_SLASH = "/branch/";
	public final static String REMOTE_SLASH = "/remote/";
	public final static String TAG_SLASH = "/tag/";

	private final static int SHA1_LENGTH = 40;

	public final static long UID = getUID();
	public final static long GID = getGID();
	
	private final static Pattern TAG_PATTERN = Pattern.compile("/tag/.+");
	private final static Pattern BRANCH_PATTERN = Pattern.compile("/branch/.+");
	private final static Pattern REMOTE_PATTERN = Pattern.compile("/remote/.+");
	private final static Pattern COMMIT_SUB_PATTERN = Pattern.compile("/commit/[a-f0-9]{2}");

	public static boolean isTagDir(final String path) {
		return TAG_PATTERN.matcher(path).matches() && !path.endsWith(".hidden");
	}

	public static boolean isBranchDir(final String path) {
		return BRANCH_PATTERN.matcher(path).matches() && !path.endsWith(".hidden");
	}

	public static boolean isRemoteDir(final String path) {
		return REMOTE_PATTERN.matcher(path).matches() && !path.endsWith(".hidden");
	}
	
	public static boolean isCommitSub(final String path) {
		return COMMIT_SUB_PATTERN.matcher(path).matches();
	}

	public static boolean isCommitDir(final String path) {
		// 8 for /commit/, 40 + 1 for commitish plus one slash
		return path.startsWith(COMMIT_SLASH) && path.length() == (COMMIT_SLASH_LENGTH + SHA1_LENGTH + 1);
	}

	public static boolean isCommitSubDir(final String path) {
		// 8 for /commit/, 40 + 2 for commitish plus two slashes
		return path.startsWith(COMMIT_SLASH) && path.length() > (COMMIT_SLASH_LENGTH + SHA1_LENGTH + 2) && !path.endsWith(".hidden");
	}

	public static long getUID() {
	    return getID(true);
	}

	public static long getGID() {
		return getID(false);
	}

	private static long getID(boolean user) {
		String userName = System.getProperty("user.name");
		try {
		    String command = "id -" + (user ? "u " : "g ") +userName;
		    Process child = Runtime.getRuntime().exec(command);

		    // Get the input stream and read from it
		    InputStream inputStream = child.getInputStream();
		    try  {
		    	String string = IOUtils.toString(inputStream).trim();
		    	System.out.println("Found user/group id: " + string);
		    	return Long.parseLong(string);
		    } finally {
		    	inputStream.close();
		    }
		} catch (IOException e) {
			System.out.println("Could not read user/group information for user " + userName);
			e.printStackTrace();
			return 0;
		}
	}
}
