package org.dstadler.jgitfs.util;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import net.fusejna.StructStat.StatWrapper;
import net.fusejna.types.TypeMode.NodeType;

import org.apache.commons.lang3.StringUtils;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.errors.MissingObjectException;
import org.eclipse.jgit.lib.AnyObjectId;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.FileMode;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectLoader;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevObject;
import org.eclipse.jgit.revwalk.RevTree;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.storage.file.FileRepositoryBuilder;
import org.eclipse.jgit.treewalk.TreeWalk;

import com.google.common.base.Charsets;
import com.google.common.io.ByteStreams;
import com.google.common.io.InputSupplier;

/**
 * Helper class which encapsulates access to the actual Git repository by
 * using JGit internally, but providing plain object/data as results, i.e.
 * no JGit objects should be necessary as part of the API.
 *
 * @author cwat-dstadler
 */
public class JGitHelper implements Closeable {
	private final Repository repository;
	private final Git git;
	private final File gitDir;

	/**
	 * Construct the helper with the given directory as Git repository.
	 *
	 * @param pGitDir A Git repository, either the root-dir or the .git directory directly.
	 * @throws IllegalStateException If the .git directory is not found
	 * @throws IOException If opening the Git repository fails
	 */
	public JGitHelper(String pGitDir) throws IOException {
		if(!pGitDir.endsWith(".git")) {
			pGitDir = pGitDir + "/.git";
		}
		gitDir = new File(pGitDir).getCanonicalFile();
		if(!gitDir.isDirectory()) {
			throw new IllegalStateException("Could not find git repository at " + gitDir);
		}

		System.out.println("Using git repo at " + gitDir);
		FileRepositoryBuilder builder = new FileRepositoryBuilder();
		repository = builder.setGitDir(gitDir)
		  .readEnvironment() // scan environment GIT_* variables
		  .findGitDir() // scan up the file system tree
		  .build();
		git = new Git(repository);
	}

	public File getGitDir() {
		return gitDir;
	}

	/**
	 * For a path to a commit, i.e. something like "/commit/0123456..." return the
	 * actual commit-id, i.e. 0123456...
	 *
	 * @param path The full path including the commit-id
	 * @return The resulting commit-id
	 */
	public String readCommit(String path) {
		String commit = StringUtils.removeStart(path, GitUtils.COMMIT_SLASH);
		return StringUtils.substring(commit, 0, 40);
	}

	/**
	 * For a path to a file/directory inside a commit like "/commit/0123456.../somedir/somefile", return
	 * the actual file-path, i.e. "somedir/somefile"
	 *
	 * @param path The full path including the commit-id
	 * @return The extracted path to the directory/file
	 */
	public String readCommitPath(final String path) {
		String file = StringUtils.removeStart(path, GitUtils.COMMIT_SLASH);
		return StringUtils.substring(file, 40 + 1);	// cut away commitish and slash
	}

	/**
	 * For a path to a tree, i.e. something like "/tree/0123456..." return the
	 * actual tree-id, i.e. 0123456...
	 *
	 * @param path The full path including the tree-id
	 * @return The resulting tree-id
	 */
	public String readTree(String path) {
		String commit = StringUtils.removeStart(path, GitUtils.TREE_SLASH);
		return StringUtils.substring(commit, 0, 40);
	}

	/**
	 * For a path to a file/directory inside a tree like "/tree/0123456.../somedir/somefile", return
	 * the actual file-path, i.e. "somedir/somefile"
	 *
	 * @param path The full path including the tree-id
	 * @return The extracted path to the directory/file
	 */
	public String readTreePath(final String path) {
		String file = StringUtils.removeStart(path, GitUtils.TREE_SLASH);
		return StringUtils.substring(file, 40 + 1);	// cut away commitish and slash
	}

	public RevCommit getCommit(String commit) throws IOException {
		try {
			RevWalk revWalk = new RevWalk(repository);
			RevObject revObject = revWalk.parseAny(ObjectId.fromString(commit));
			if (revObject instanceof RevCommit) {
				return (RevCommit) revObject;
			}
			return null;
		} catch (MissingObjectException e) {
			return null;
		}
	}

	public RevTree getTree(String tree) throws IOException {
		try {
			RevWalk revWalk = new RevWalk(repository);
			RevObject revObject = revWalk.parseAny(ObjectId.fromString(tree));
			if (revObject instanceof RevTree) {
				return (RevTree) revObject;
			}
			return null;
		} catch (MissingObjectException e) {
			return null;
		}
	}

	/**
	 * Populate the StatWrapper with the necessary values like mode, uid, gid and type of file/directory/symlink.
	 *
	 * @param tree the tree to start from
	 * @param path The path to the file/directory
	 * @param stat The StatWrapper instance to populate
	 *
	 * @throws IllegalStateException If the path or the commit cannot be found or an unknown type is encountered
	 * @throws IOException If access to the Git repository fails
	 * @return true if the object could be found
	 */
	public boolean readType(RevTree tree, String path, StatWrapper stat) throws IOException {
		// shortcut for root-path
		if (path.isEmpty()) {
			stat.setMode(NodeType.DIRECTORY, true, false, true);
			return true;
		}

		// now read the file/directory attributes
		TreeWalk treeWalk = TreeWalk.forPath(repository, path, tree);
		if (treeWalk == null) {
			if (path.equals(".gittree")) {
				// Special hidden file.
				stat.size(41);
				stat.setMode(NodeType.FILE, true, false, true);
				return true;
			}
			if (path.endsWith("/.gittree")) {
				// Special hidden file only if within a valid tree.
				treeWalk = TreeWalk.forPath(repository, StringUtils.removeEnd(path, "/.gittree"), tree);
				if (treeWalk != null && treeWalk.isSubtree()) {
					stat.size(41);
					stat.setMode(NodeType.FILE, true, false, true);
					return true;
				}
			}
			return false;
		}
		FileMode fileMode = treeWalk.getFileMode(0);
		if(fileMode.equals(FileMode.EXECUTABLE_FILE) ||
				fileMode.equals(FileMode.REGULAR_FILE)) {
			stat.size(
					treeWalk.getObjectReader().getObjectSize(treeWalk.getObjectId(0), Constants.OBJ_BLOB));
			stat.setMode(NodeType.FILE, true, false, fileMode.equals(FileMode.EXECUTABLE_FILE));
			return true;
		} else if(fileMode.equals(FileMode.TREE)) {
			stat.size(
					treeWalk.getObjectReader().getObjectSize(treeWalk.getObjectId(0), Constants.OBJ_TREE));
			stat.setMode(NodeType.DIRECTORY, true, false, true);
			return true;
		} if(fileMode.equals(FileMode.SYMLINK)) {
			stat.setMode(NodeType.SYMBOLIC_LINK, true, true, true);
			return true;
		}

		throw new IllegalStateException("Found unknown FileMode in Git for tree '" + tree + "' and path '" + path + "': " + fileMode.getBits());
	}

	/**
	 * Read the target file for the given symlink as part of the given tree.
	 *
	 * @param tree The tree from which we read the data
	 * @param path the path to the symlink
	 * @return the target of the symlink, relative to the directory of the symlink itself
	 * @throws IOException If an error occurs while reading from the Git repository
	 * @throws IllegalArgumentException If the given path does not denote a symlink
	 * @return null if the path cannot be found
	 */
	public byte[] readSymlink(final RevTree tree, final String path) throws IOException {
		// shortcut for root-path
		if (path.isEmpty()) {
			return null;
		}

		// read the file/directory attributes
		TreeWalk treeWalk = TreeWalk.forPath(repository, path, tree);
		if (treeWalk == null) {
			return null;
		}
		FileMode fileMode = treeWalk.getFileMode(0);
		if(!fileMode.equals(FileMode.SYMLINK)) {
			throw new IllegalArgumentException("Had request for symlink-target which is not a symlink, tree '" + tree + "' and path '" + path + "': " + fileMode.getBits());
		}

		return ByteStreams.toByteArray(new InputSupplier<InputStream>() {
			@Override public InputStream getInput() throws IOException {
				return openFile(tree, path);
			}
		});
	}

	/**
	 * Retrieve the contents of the given file as-of the given commit.
	 *
	 * @param tree The tree from which we read the data
	 * @param path The path to the file/directory
	 *
	 * @return An InputStream which can be used to read the contents of the file or null if the file could not be opened.
	 *
	 * @throws IllegalStateException If the path or the commit cannot be found or does not denote a file
	 * @throws IOException If access to the Git repository fails
	 */
	public InputStream openFile(RevTree tree, String path) throws IOException {
		TreeWalk treeWalk = TreeWalk.forPath(repository, path, tree);
		if (treeWalk == null) {
			if (path.equals(".gittree")) {
				// Special hidden symlink.
				return new ByteArrayInputStream(
						(tree.getId().getName() + '\n').getBytes(Charsets.US_ASCII));
			}
			if (path.endsWith("/.gittree")) {
				// Special hidden symlink only if within a valid tree.
				treeWalk = TreeWalk.forPath(repository, StringUtils.removeEnd(path, "/.gittree"), tree);
				if (treeWalk != null && treeWalk.isSubtree()) {
					return new ByteArrayInputStream(
							(treeWalk.getObjectId(0).getName() + '\n').getBytes(Charsets.US_ASCII));
				}
			}
			return null;
		}

		if((treeWalk.getFileMode(0).getBits() & FileMode.TYPE_FILE) == 0) {
			return null;
		}

		// then open the file for reading.
		ObjectId objectId = treeWalk.getObjectId(0);
		ObjectLoader loader = repository.open(objectId);

		// finally open an InputStream for the file contents
		return loader.openStream();
	}

	/**
	 * Return all remote branches.
	 *
	 * @return A list of branch-names
	 * @throws IOException If accessing the Git repository fails
	 */
	public List<String> getBranches() throws IOException {
		return getRefs("refs/heads");
	}

	/**
	 * Return the commit-id for the given branch.
	 *
	 * @param branch The branch to read data for, should start with refs/heads/...
	 * @return A commit-id if found or null if not found.
	 * @throws IOException If accessing the Git repository fails
	 */
	public String getBranchHeadCommit(String branch) throws IOException {
		return getRefCommit("refs/heads/" + branch);
	}

	/**
	 * Return all remote branches.
	 *
	 * @return A list of remote branch-names
	 * @throws IOException If accessing the Git repository fails
	 */
	public List<String> getRemotes() throws IOException {
		return getRefs("refs/remotes");
	}

	/**
	 * Return the commit-id for the given remote branch.
	 *
	 * @param branch The remote branch name to read data for, should start with refs/remotes/...
	 * @return A commit-id if found or null if not found.
	 * @throws IOException If accessing the Git repository fails
	 */
	public String getRemoteHeadCommit(String branch) throws IOException {
		Ref ref = git.getRepository().getRefDatabase().getRef("refs/remotes/" + branch);
		if (ref == null) {
			return null;
		}
		return ref.getObjectId().getName();
	}

	/**
	 * Return all tags.
	 *
	 * @return A list of tag-names
	 * @throws IOException If accessing the Git repository fails
	 */
	public List<String> getTags() throws IOException {
		return getRefs("refs/tags");
	}

	/**
	 * Return the commit-id for the given tag.
	 *
	 * @param tag The tag to read data for, should start with refs/tags/...
	 * @return A commit-id if found or null if not found.
	 * @throws IOException If accessing the Git repository fails
	 */
	public String getTagHeadCommit(String tag) throws IOException {
		Ref ref = git.getRepository().getRef("refs/tags/" + tag);
		if (ref == null) {
			return null;
		}
		return ref.getObjectId().getName();
	}

	/**
	 * Return the commit-id for the given branch.
	 *
	 * @param refName The ref to read data for
	 * @return A commit-id if found or null if not found.
	 * @throws IOException If accessing the Git repository fails
	 */
	public String getRefCommit(String refName) throws IOException {
		Ref ref = git.getRepository().getRef(refName);
		if (ref == null) {
			return null;
		}
		return ref.getObjectId().getName();
	}

	/**
	 * Return all refs matching the given prefix.
	 *
	 * @return A list of remote branch-names
	 * @throws IOException If accessing the Git repository fails
	 */
	public List<String> getRefs(String prefix) throws IOException {
		Map<String, Ref> refMap = git.getRepository().getRefDatabase().getRefs(prefix + '/');
		ArrayList<String> result = new ArrayList<String>(refMap.keySet());
		Collections.sort(result);
		return result;
	}


	public boolean hasRefs(String prefix) throws IOException {
		// NOT faster: git.getRepository().getRefDatabase().isNameConflicting(prefix);
		Map<String, Ref> refMap = git.getRepository().getRefDatabase().getRefs(prefix + '/');
		return refMap.size() > 0;
	}

	/**
	 * Free resources held in thie instance, i.e. by releasing the Git repository resources held internally.
	 * 
	 * The instance is not useable after this call any more.
	 */
	@Override
	public void close() throws IOException {
		repository.close();
	}

	/**
	 * Retrieve directory-entries based on a tree and a given directory in that tree.
	 *
	 * @param tree The tree to show the path as-of
	 * @param path The path underneath the commit-id to list
	 * @return A list of file, directory and symlink elements underneath the given path, or null if the path cannot be found
	 * @throws IllegalStateException If the path or the commit cannot be found or does not denote a directory
	 * @throws IOException If access to the Git repository fails
	 */
	public List<String> readElementsAt(RevTree tree, String path) throws IOException {
		AnyObjectId toWalk;
		if (path.isEmpty()) {
			// shortcut for root-path
			toWalk = tree;
		} else {
			// try to find a specific subtree
			TreeWalk treeWalk = TreeWalk.forPath(repository, path, tree);
			if(treeWalk == null) {
				return null;
			}
			if((treeWalk.getFileMode(0).getBits() & FileMode.TYPE_TREE) == 0) {
				return Collections.emptyList();
			}
			toWalk = treeWalk.getObjectId(0);
		}

		TreeWalk dirWalk = new TreeWalk(repository);
		dirWalk.addTree(toWalk);
		dirWalk.setRecursive(false);
		List<String> items = new ArrayList<String>();
		while(dirWalk.next()) {
			items.add(dirWalk.getPathString());
		}
		return items;
	}

	@Override
	public String toString() {
			// just return toString() from Repository as it prints out the git-directory
			return repository.toString();
	}
}
