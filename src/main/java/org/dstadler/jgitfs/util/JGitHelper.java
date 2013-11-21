package org.dstadler.jgitfs.util;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
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
import org.eclipse.jgit.errors.IncorrectObjectTypeException;
import org.eclipse.jgit.errors.MissingObjectException;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.FileMode;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectIdSubclassMap;
import org.eclipse.jgit.lib.ObjectLoader;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevObject;
import org.eclipse.jgit.revwalk.RevTree;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.storage.file.FileRepositoryBuilder;
import org.eclipse.jgit.treewalk.TreeWalk;

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

	/**
	 * Construct the helper with the given directory as Git repository.
	 *
	 * @param pGitDir A Git repository, either the root-dir or the .git directory directly.
	 * @throws IllegalStateException If the .git directory is not found
	 * @throws IOException If opening the Git repository fails
	 */
	public JGitHelper(String pGitDir) throws IOException {
		String gitDir = pGitDir;
		if(!gitDir.endsWith(".git")) {
			gitDir = gitDir + "/.git";
		}
		if(!new File(gitDir).exists()) {
			throw new IllegalStateException("Could not find git repository at " + gitDir);
		}

		System.out.println("Using git repo at " + gitDir);
		FileRepositoryBuilder builder = new FileRepositoryBuilder();
		repository = builder.setGitDir(new File(gitDir))
		  .readEnvironment() // scan environment GIT_* variables
		  .findGitDir() // scan up the file system tree
		  .build();
		git = new Git(repository);
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
	public String readPath(final String path) {
		String file = StringUtils.removeStart(path, GitUtils.COMMIT_SLASH);
		return StringUtils.substring(file, 40 + 1);	// cut away commitish and slash
	}

	/**
	 * Populate the StatWrapper with the necessary values like mode, uid, gid and type of file/directory/symlink.
	 *
	 * @param commit The commit-id as-of which we read the data
	 * @param path The path to the file/directory
	 * @param stat The StatWrapper instance to populate
	 *
	 * @throws IllegalStateException If the path or the commit cannot be found or an unknown type is encountered
	 * @throws IOException If access to the Git repository fails
	 * @return true if the object could be found
	 */
	public boolean readType(String commit, String path, StatWrapper stat) throws IOException {
		RevCommit revCommit;
		try {
			revCommit = buildRevCommit(commit);
		} catch (MissingObjectException e) {
			return false;
		} catch (IncorrectObjectTypeException e) {
			return false;
		}

		if (path.length() == 0) {
			// The top-level commit directory itself.
			stat.setMode(NodeType.DIRECTORY, true, false, true);
			return true;
		}

		// and using commit's tree find the path
		RevTree tree = revCommit.getTree();
		//System.out.println("Having tree: " + tree + " for commit " + commit);

		// set time and user-id/group-id
		stat.ctime(revCommit.getCommitTime());
		stat.mtime(revCommit.getCommitTime());
		//stat.uid(GitUtils.UID);
		//stat.gid(GitUtils.GID);

		// now read the file/directory attributes
		TreeWalk treeWalk = TreeWalk.forPath(repository, path, tree);
		if (treeWalk == null) {
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

		throw new IllegalStateException("Found unknown FileMode in Git for commit '" + commit + "' and path '" + path + "': " + fileMode.getBits());
	}

	/**
	 * Read the target file for the given symlink as part of the given commit.
	 *
	 * @param commit the commit-id as-of which we read the symlink
	 * @param path the path to the symlink
	 * @return the target of the symlink, relative to the directory of the symlink itself
	 * @throws IOException If an error occurs while reading from the Git repository
	 * @throws FileNotFoundException If the given path cannot be found in the given commit-id
	 * @throws IllegalArgumentException If the given path does not denote a symlink
	 */
	public byte[] readSymlink(final String commit, final String path) throws IOException {
		RevCommit revCommit = buildRevCommit(commit);

		// and using commit's tree find the path
		RevTree tree = revCommit.getTree();

		// now read the file/directory attributes
		TreeWalk treeWalk = buildTreeWalk(tree, path);
		FileMode fileMode = treeWalk.getFileMode(0);
		if(!fileMode.equals(FileMode.SYMLINK)) {
			throw new IllegalArgumentException("Had request for symlink-target which is not a symlink, commit '" + commit + "' and path '" + path + "': " + fileMode.getBits());
		}

		return ByteStreams.toByteArray(new InputSupplier<InputStream>() {
			@Override public InputStream getInput() throws IOException {
				return openFile(commit, path);
			}
		});
	}
	
	/**
	 * Retrieve the contents of the given file as-of the given commit.
	 *
	 * @param commit The commit-id as-of which we read the data
	 * @param path The path to the file/directory
	 *
	 * @return An InputStream which can be used to read the contents of the file.
	 *
	 * @throws IllegalStateException If the path or the commit cannot be found or does not denote a file
	 * @throws IOException If access to the Git repository fails
	 * @throws FileNotFoundException If the given path cannotbe found in the given commit-id
	 */
	public InputStream openFile(String commit, String path) throws IOException {
		RevCommit revCommit = buildRevCommit(commit);

		// use the commit's tree find the path
		RevTree tree = revCommit.getTree();
		//System.out.println("Having tree: " + tree + " for commit " + commit);

		// now try to find a specific file
		TreeWalk treeWalk = buildTreeWalk(tree, path);
		if((treeWalk.getFileMode(0).getBits() & FileMode.TYPE_FILE) == 0) {
			throw new IllegalStateException("Tried to read the contents of a non-file for commit '" + commit + "' and path '" + path + "', had filemode " + treeWalk.getFileMode(0).getBits());
		}

		// then open the file for reading.
		ObjectId objectId = treeWalk.getObjectId(0);
		ObjectLoader loader = repository.open(objectId);

		// finally open an InputStream for the file contents
		return loader.openStream();
	}

	private TreeWalk buildTreeWalk(RevTree tree, final String path) throws IOException {
		TreeWalk treeWalk = TreeWalk.forPath(repository, path, tree);

		if(treeWalk == null) {
			throw new FileNotFoundException("Did not find expected file '" + path + "' in tree '" + tree.getName() + "'");
		}

		return treeWalk;
	}

	private RevCommit buildRevCommit(String commit) throws IOException {
		// a RevWalk allows to walk over commits based on some filtering that is defined
		RevWalk revWalk = new RevWalk(repository);
		return revWalk.parseCommit(ObjectId.fromString(commit));
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
	 * Retrieve directory-entries based on a commit-id and a given directory in that commit.
	 *
	 * @param commit The commit-id to show the path as-of
	 * @param path The path underneath the commit-id to list
	 *
	 * @return A list of file, directory and symlink elements underneath the given path
	 *
	 * @throws IllegalStateException If the path or the commit cannot be found or does not denote a directory
	 * @throws IOException If access to the Git repository fails
	 * @throws FileNotFoundException If the given path cannot be found as part of the commit-id
	 */
	public List<String> readElementsAt(String commit, String path) throws IOException {
		RevCommit revCommit = buildRevCommit(commit);

		// and using commit's tree find the path
		RevTree tree = revCommit.getTree();
		//System.out.println("Having tree: " + tree + " for commit " + commit);

		List<String> items = new ArrayList<String>();

		// shortcut for root-path
		if(path.isEmpty()) {
			TreeWalk treeWalk = new TreeWalk(repository);
			treeWalk.addTree(tree);
			treeWalk.setRecursive(false);
			treeWalk.setPostOrderTraversal(false);

			while(treeWalk.next()) {
				items.add(treeWalk.getPathString());
			}

			return items;
		}

		// now try to find a specific file
		TreeWalk treeWalk = buildTreeWalk(tree, path);
		if((treeWalk.getFileMode(0).getBits() & FileMode.TYPE_TREE) == 0) {
			throw new IllegalStateException("Tried to read the elements of a non-tree for commit '" + commit + "' and path '" + path + "', had filemode " + treeWalk.getFileMode(0).getBits());
		}

		TreeWalk dirWalk = new TreeWalk(repository);
		dirWalk.addTree(treeWalk.getObjectId(0));
		dirWalk.setRecursive(false);
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
