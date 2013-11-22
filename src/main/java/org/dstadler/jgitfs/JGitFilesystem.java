package org.dstadler.jgitfs;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import net.fusejna.DirectoryFiller;
import net.fusejna.ErrorCodes;
import net.fusejna.FuseException;
import net.fusejna.FuseFilesystem;
import net.fusejna.StructFuseFileInfo.FileInfoWrapper;
import net.fusejna.StructStat.StatWrapper;
import net.fusejna.types.TypeMode.NodeType;
import net.fusejna.util.FuseFilesystemAdapterFull;
import org.apache.commons.lang3.StringUtils;
import org.dstadler.jgitfs.util.GitUtils;
import org.dstadler.jgitfs.util.JGitHelper;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevTree;

import com.google.common.base.Charsets;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.io.ByteStreams;
import com.google.common.io.Resources;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.UncheckedExecutionException;

/**
 * Implementation of the {@link FuseFilesystem} interfaces to
 * provide a view of branches/tags/commits of the given
 * Git repository.
 *
 * @author dominik.stadler
 */
public class JGitFilesystem extends FuseFilesystemAdapterFull implements Closeable {
	private static final long CACHE_TIMEOUT = 60 * 1000;	// one minute

	private long lastLinkCacheCleanup = System.currentTimeMillis();

	private final JGitHelper jgitHelper;

	/**
	 * static set of directories to handle them quickly in getattr().
	 */
	private static Set<String> DIRS = new HashSet<String>();
	static {
		DIRS.add("/");
		DIRS.add("/branch");
		DIRS.add("/commit");
		DIRS.add("/tree");
		DIRS.add("/remote");
		DIRS.add("/tag");
	}

	private static final String README_MD;
	static {
		try {
			README_MD = Resources.toString(JGitFilesystem.class.getResource("README.md"),
					Charsets.US_ASCII);
		} catch (IOException e) {
			throw new AssertionError(e);
		}
	}

	private final byte[] readmeMdText;

	/**
	 * Construct the filesystem and create internal helpers.
	 *
	 * @param gitDir The directory where the Git repository can be found.
	 * @param enableLogging If fuse-jna should log details about file/directory accesses
	 * @throws IOException If opening the Git repository fails.
	 */
	public JGitFilesystem(String gitDir, boolean enableLogging) throws IOException {
		super();

		// disable verbose logging
		log(enableLogging);

		jgitHelper = new JGitHelper(gitDir);

		readmeMdText = String.format(README_MD, jgitHelper.getGitDir()).getBytes(Charsets.US_ASCII);
	}

	@Override
	public int getattr(final String path, final StatWrapper stat)
	{
		// known entries and directories beneath /commit are always directories
		if(DIRS.contains(path)) {
			//stat.uid(GitUtils.UID);
			//stat.gid(GitUtils.GID);
			stat.setMode(NodeType.DIRECTORY, true, false, true);
			return 0;
		} else if (GitUtils.isBranchDir(path)) {
			return getattrRef("refs/heads/" + StringUtils.removeStart(path, GitUtils.BRANCH_SLASH), stat);
		} else if (GitUtils.isTagDir(path)) {
			return getattrRef("refs/tags/" + StringUtils.removeStart(path, GitUtils.TAG_SLASH), stat);
		} else if (GitUtils.isRemoteDir(path)) {
			return getattrRef("refs/remotes/" + StringUtils.removeStart(path, GitUtils.REMOTE_SLASH), stat);
		} else if (GitUtils.isCommitDir(path)) {
			String commit = jgitHelper.readCommit(path);
			String file = jgitHelper.readCommitPath(path);
			try {
				RevCommit revCommit = jgitHelper.getCommit(commit);
				if (revCommit != null) {
					stat.ctime(revCommit.getCommitTime());
					stat.mtime(revCommit.getCommitTime());
					if (jgitHelper.readType(revCommit.getTree(), file, stat)) {
						return 0;
					}
				}
				return -ErrorCodes.ENOENT();
			} catch (Exception e) {
				throw new IllegalStateException("Error reading type of path " + path + ", commit " + commit + " and file " + file, e);
			}
		} else if (GitUtils.isTreeDir(path)) {
			String tree = jgitHelper.readTree(path);
			String file = jgitHelper.readTreePath(path);
			try {
				RevTree revTree = jgitHelper.getTree(tree);
				if (revTree != null) {
					if (jgitHelper.readType(revTree, file, stat)) {
						return 0;
					}
				}
				return -ErrorCodes.ENOENT();
			} catch (Exception e) {
				throw new IllegalStateException("Error reading type of path " + path + ", tree " + tree + " and file " + file, e);
			}
		} else if ("/README.md".equals(path)) {
			stat.size(readmeMdText.length);
			stat.setMode(NodeType.FILE, true, false, false);
			return 0;
		}

		// all others are reported as "not found"
		return -ErrorCodes.ENOENT();
	}

	private int getattrRef(String ref, StatWrapper stat) {
		try {
			String commit = jgitHelper.getRefCommit(ref);
			if (commit != null) {
				// Exact match, it's a branch.
				stat.setMode(NodeType.SYMBOLIC_LINK, true, true, true);
				return 0;
			}
			if (jgitHelper.hasRefs(ref)) {
				// A directory containing branches.
				stat.setMode(NodeType.DIRECTORY, true, false, true);
				return 0;
			}
			return -ErrorCodes.ENOENT();
		} catch (Exception e) {
			throw new IllegalStateException("Error reading branches", e);
		}
	}

	@Override
	public int read(final String path, final ByteBuffer buffer, final long size, final long offset, final FileInfoWrapper info) {
		if ("/README.md".equals(path)) {
			long remaining = readmeMdText.length - offset;
			if (remaining <= 0) {
				return 0;
			}
			int len = (int) Math.min(size, remaining);
			buffer.put(readmeMdText, (int) offset, len);
			return len;
		}
		try {
			final RevTree revTree;
			final String file;
			if (GitUtils.isCommitDir(path)) {
				String commit = jgitHelper.readCommit(path);
				file = jgitHelper.readCommitPath(path);
				RevCommit revCommit = jgitHelper.getCommit(commit);
				revTree = (revCommit != null) ? revCommit.getTree() : null;
			} else if (GitUtils.isTreeDir(path)) {
				String tree = jgitHelper.readTree(path);
				file = jgitHelper.readTreePath(path);
				revTree = jgitHelper.getTree(tree);
			} else {
				return -ErrorCodes.ENOENT();
			}

			if (revTree == null || file.isEmpty()) {
				return -ErrorCodes.ENOENT();
			}

			InputStream openFile = jgitHelper.openFile(revTree, file);
			if (openFile == null) {
				return -ErrorCodes.ENOENT();
			}
			try {
				// skip until we are at the offset
				ByteStreams.skipFully(openFile, offset);

				byte[] arr = new byte[8096];
				long remaining = size;
				long total = 0;
				while (remaining > 0) {
					int attemptToRead = (int) Math.min(arr.length, remaining);
					int read = ByteStreams.read(openFile, arr, 0, attemptToRead);
					buffer.put(arr, 0, read);
					total += read;
					remaining -= read;
					if (read < attemptToRead) {
						// Reached EOF.
						break;
					}
				}
				return Ints.saturatedCast(total);
			} finally {
				openFile.close();
			}
		} catch (Exception e) {
			throw new IllegalStateException("Error reading contents of path " + path, e);
		}
	}

	@Override
	public int readdir(final String path, final DirectoryFiller filler) {
		if(path.equals("/")) {
			// populate top-level directory with all supported sub-directories
			filler.add("/branch");
			filler.add("/commit");
			filler.add("/remote");
			filler.add("/tag");
			filler.add("/tree");
			filler.add("/README.md");

			// TODO: implement later
//			filler.add("/stash");
//			filler.add("/index");	- use DirCache?
//			filler.add("/workspace"); - use WorkingTreeIterator?
//			filler.add("/git") => symbolic link to the source dir
//			filler.add("/notes"); - notes	
//			filler.add("/perfile/branch"); - history per file
//			filler.add("/perfile/commit"); - history per file
//			filler.add("/perfile/remote"); - history per file
//			filler.add("/perfile/tag"); - history per file

			return 0;
		} else if (path.equals("/commit")) {
			// Do not list commits.
			// consider: LRU list of recently-accessed for completion?
			return 0;
		} else if (path.equals("/tree")) {
			// Do not list trees.
			// consider: LRU list of recently-accessed for completion?
			return 0;
		} else if (GitUtils.isCommitDir(path)) {
			// handle listing the root dir of a commit or a file beneath that
			String commit = jgitHelper.readCommit(path);
			String dir = jgitHelper.readCommitPath(path);

			try {
				RevCommit revCommit = jgitHelper.getCommit(commit);
				if (revCommit == null) {
					return -ErrorCodes.ENOENT();
				}
				return readdir(revCommit.getTree(), dir, filler);
			} catch (Exception e) {
				throw new IllegalStateException("Error reading elements of path " + path + ", commit " + commit + " and directory " + dir, e);
			}
		} else if (GitUtils.isTreeDir(path)) {
			// handle listing the root dir of a tree or a file beneath that
			String tree = jgitHelper.readTree(path);
			String dir = jgitHelper.readTreePath(path);

			try {
				return readdir(jgitHelper.getTree(tree), dir, filler);
			} catch (Exception e) {
				throw new IllegalStateException("Error reading elements of path " + path + ", tree " + tree + " and directory " + dir, e);
			}
		} else if (GitUtils.isBranchDir(path)) {
			try {
				String parent = StringUtils.removeStart(path, "/branch/");
				HashSet<String> seen = new HashSet<String>();
				List<String> items = jgitHelper.getBranches();
				for(String item : items) {
					if (!item.startsWith(parent)) {
						continue;
					}
					item = StringUtils.removeStart(item, parent + '/');
					item = StringUtils.substringBefore(item, "/");
					if (!seen.contains(item)) {
						filler.add(item);
						seen.add(item);
					}
				}
			} catch (Exception e) {
				throw new IllegalStateException("Error reading branches", e);
			}

			return 0;
		} else if (GitUtils.isTagDir(path)) {
			try {
				String parent = StringUtils.removeStart(path, "/tag/");
				HashSet<String> seen = new HashSet<String>();
				List<String> items = jgitHelper.getTags();
				for(String item : items) {
					if (!item.startsWith(parent)) {
						continue;
					}
					item = StringUtils.removeStart(item, parent + '/');
					item = StringUtils.substringBefore(item, "/");
					if (!seen.contains(item)) {
						filler.add(item);
						seen.add(item);
					}
				}
			} catch (Exception e) {
				throw new IllegalStateException("Error reading tags", e);
			}

			return 0;
		} else if (GitUtils.isRemoteDir(path)) {
			try {
				String parent = StringUtils.removeStart(path, "/remote/");
				HashSet<String> seen = new HashSet<String>();
				List<String> items = jgitHelper.getRemotes();
				for(String item : items) {
					if (!item.startsWith(parent)) {
						continue;
					}
					item = StringUtils.removeStart(item, parent + '/');
					item = StringUtils.substringBefore(item, "/");
					if (!seen.contains(item)) {
						filler.add(item);
						seen.add(item);
					}
				}
			} catch (Exception e) {
				throw new IllegalStateException("Error reading tags", e);
			}

			return 0;
		} else if (path.equals("/tag")) {
			try {
				HashSet<String> seen = new HashSet<String>();
				List<String> items = jgitHelper.getTags();
				for(String item : items) {
					item = StringUtils.substringBefore(item, "/");
					if (!seen.contains(item)) {
						filler.add(item);
						seen.add(item);
					}
				}
			} catch (Exception e) {
				throw new IllegalStateException("Error reading tags", e);
			}

			return 0;
		} else if (path.equals("/branch")) {
			try {
				HashSet<String> seen = new HashSet<String>();
				List<String> items = jgitHelper.getBranches();
				for(String item : items) {
					item = StringUtils.substringBefore(item, "/");
					if (!seen.contains(item)) {
						filler.add(item);
						seen.add(item);
					}
				}
			} catch (Exception e) {
				throw new IllegalStateException("Error reading branches", e);
			}

			return 0;
		} else if (path.equals("/remote")) {
			try {
				HashSet<String> seen = new HashSet<String>();
				List<String> items = jgitHelper.getRemotes();
				for(String item : items) {
					item = StringUtils.substringBefore(item, "/");
					if (!seen.contains(item)) {
						filler.add(item);
						seen.add(item);
					}
				}
			} catch (Exception e) {
				throw new IllegalStateException("Error reading remotes", e);
			}

			return 0;
				 		}
		throw new IllegalStateException("Error reading directories in path " + path);
	}

	private int readdir(RevTree revTree, String dir, DirectoryFiller filler) throws IOException {
		if (revTree != null) {
			List<String> items = jgitHelper.readElementsAt(revTree, dir);
			if (items != null) {
				if (items == Collections.<String> emptyList()) {
					return -ErrorCodes.ENOTDIR();
				}
				for (String item : items) {
					filler.add(item);
				}
				return 0;
			}
		}
		return -ErrorCodes.ENOENT();
	}

	private static final byte[] SENTINEL = new byte[0];

	/**
	 * A cache for symlinks from branches/tags to commits, this is useful as queries for symlinks
	 * are done very often as each access to a file on a branch also requires the symlink to the
	 * actual commit to be resolved. This cache greatly improves the speed of these accesses.
	 *
	 * This makes use of the Google Guava LoadingCache features to automatically populate
	 * entries when they are missing which makes the usage of the cache very simple.
	 */
  private LoadingCache<String, byte[]> linkCache = CacheBuilder.newBuilder()
      .maximumSize(1000)
      .expireAfterWrite(1, TimeUnit.MINUTES)
      .build(new CacheLoader<String, byte[]>() {
				@Override
				public byte[] load(String path) {
					byte[] bytes = doLoad(path);
					return (bytes != null) ? bytes : SENTINEL;
				}

				private byte[] doLoad(String path) {
					try {
						final String commitLink;
						final String partialPath;
						if (GitUtils.isBranchDir(path)) {
							partialPath = StringUtils.removeStart(path, GitUtils.BRANCH_SLASH);
							commitLink = jgitHelper.getBranchHeadCommit(partialPath);
						} else if (GitUtils.isTagDir(path)) {
							partialPath = StringUtils.removeStart(path, GitUtils.TAG_SLASH);
							commitLink = jgitHelper.getTagHeadCommit(partialPath);
						} else if (GitUtils.isRemoteDir(path)) {
							partialPath = StringUtils.removeStart(path, GitUtils.REMOTE_SLASH);
							commitLink = jgitHelper.getRemoteHeadCommit(partialPath);
						} else if (GitUtils.isCommitDir(path)) {
							String commit = jgitHelper.readCommit(path);
							String file = jgitHelper.readCommitPath(path);
							RevCommit revCommit = jgitHelper.getCommit(commit);
							if (revCommit == null || file.isEmpty()) {
								return null;
							}
							return jgitHelper.readSymlink(revCommit.getTree(), file);
						} else if (GitUtils.isTreeDir(path)) {
							String tree = jgitHelper.readTree(path);
							String file = jgitHelper.readTreePath(path);
							RevTree revTree = jgitHelper.getTree(tree);
							if (revTree == null || file.isEmpty()) {
								return null;
							}
							return jgitHelper.readSymlink(revTree, file);
						} else {
							return null;
						}

						if (commitLink == null) {
							return null;
						}
						StringBuilder target = new StringBuilder("..");
						for (char c : partialPath.toCharArray()) {
							if (c == '/') {
								target.append("/..");
							}
						}
						target.append(GitUtils.COMMIT_SLASH);
						target.append(commitLink);

						return target.toString().getBytes();
					} catch (Exception e) {
						throw new IllegalStateException("Error reading commit of tag/branch-path " + path, e);
					}
				}
			});

	@Override
	public int readlink(String path, ByteBuffer buffer, long size) {
		// ensure that we evict caches sometimes, Google Guava does not make guarantees that
		// eviction happens automatically in a mostly read-only cache
		if(System.currentTimeMillis() > (lastLinkCacheCleanup + CACHE_TIMEOUT)) {
			System.out.println("Perform manual cache maintenance for " + jgitHelper.toString() + " after " + ((System.currentTimeMillis() - lastLinkCacheCleanup)/1000) + " seconds");
			lastLinkCacheCleanup = System.currentTimeMillis();
			linkCache.cleanUp();
		}

		// use the cache to speed up access, symlinks are always queried even for sub-path access, so we get lots of requests for these!
		byte[] cachedCommit;
		try {
			cachedCommit = linkCache.get(path);
			if (cachedCommit == null || cachedCommit == SENTINEL) {
				return -ErrorCodes.ENOENT();
			}
			// buffer overflow checks are done by the calls to put() itself per javadoc,
			// currently we will throw an exception to the outside, experiment showed that we support 4097 bytes of path-length on 64-bit Ubuntu this way
			buffer.put(cachedCommit);
			// zero-byte is appended by fuse-jna itself

			// returning the size as per readlink(2) spec causes fuse errors: return cachedCommit.length;
			return 0;
		} catch (UncheckedExecutionException e) {
			throw new IllegalStateException("Error reading commit of tag/branch-path " + path, e);
		} catch (ExecutionException e) {
			throw new IllegalStateException("Error reading commit of tag/branch-path " + path, e);
		}
	}

	/**
	 * Free up resources held for the Git repository and unmount the FUSE-filesystem.
	 *
	 * @throws IOException If an error ocurred while closing the Git repository or while unmounting the filesystem.
	 */
	@Override
	public void close() throws IOException {
		jgitHelper.close();

		try {
			unmount();
		} catch (FuseException ignored) {
			// ignore, will mask active exception
		}
	}

  @Override protected String[] getOptions() {
    String options = ""
        + "uid=" + GitUtils.UID
        + ",gid=" + GitUtils.GID
        + ",allow_other"
        + ",default_permissions"
        + ",kernel_cache"
        + ",entry_timeout=10"
        + ",negative_timeout=10"
        + ",attr_timeout=10";
    return new String[] {"-r", "-o", options};
  }
}
