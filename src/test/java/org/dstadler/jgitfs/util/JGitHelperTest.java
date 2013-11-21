package org.dstadler.jgitfs.util;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import net.fusejna.StatWrapperFactory;
import net.fusejna.StructStat.StatWrapper;
import net.fusejna.types.TypeMode;
import net.fusejna.types.TypeMode.NodeType;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.RefUpdate;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.storage.file.FileRepositoryBuilder;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.base.Charsets;

public class JGitHelperTest {
	public final static String DEFAULT_COMMIT = "ede9797616a805d6cbeca376bfbbac9a8b7eb64f";

	private static Git git;

	private JGitHelper helper;

	@BeforeClass
	public static void setUpClass() throws GitAPIException, IOException {
		FileRepositoryBuilder builder = new FileRepositoryBuilder();
		Repository repository = builder.setGitDir(new File(".git"))
		  .readEnvironment() // scan environment GIT_* variables
		  .findGitDir() // scan up the file system tree
		  .build();
		git = new Git(repository);

		// a RevWalk allows to walk over commits based on some filtering that is defined
		RevWalk revWalk = new RevWalk(repository);
		RevCommit revCommit = revWalk.parseCommit(ObjectId.fromString(DEFAULT_COMMIT));

		git.tag().setName("__testtag").setForceUpdate(true).setObjectId(revCommit).call();
		git.tag().setName("__test/tag").setForceUpdate(true).setObjectId(revCommit).call();
		git.branchCreate().setName("__testbranch").setForce(true).setStartPoint(revCommit).call();
		git.branchCreate().setName("__test/branch").setForce(true).setStartPoint(revCommit).call();

		RefUpdate update = git.getRepository().updateRef("refs/remotes/__origin/testbranch");
		update.setNewObjectId(revCommit);
		update.forceUpdate();

		update = git.getRepository().updateRef("refs/remotes/__origin/test/branch");
		update.setNewObjectId(revCommit);
		update.forceUpdate();
	}

	@AfterClass
	public static void tearDownClass() throws GitAPIException, IOException {
		if (git == null) {
			return;
		}
		git.branchDelete().setBranchNames("__test/branch").setForce(true).call();
		git.branchDelete().setBranchNames("__testbranch").setForce(true).call();
		git.tagDelete().setTags("__test/tag").call();
		git.tagDelete().setTags("__testtag").call();

		RefUpdate update = git.getRepository().updateRef("refs/remotes/__origin/testbranch");
		update.setForceUpdate(true);
		update.delete();

		update = git.getRepository().updateRef("refs/remotes/__origin/test/branch");
		update.setForceUpdate(true);
		update.delete();
	}

	@Before
	public void setUp() throws IOException {
		helper = new JGitHelper(".");
	}

	@After
	public void tearDown() throws IOException {
		helper.close();
	}

	@Test
	public void test() throws Exception {
		assertNotNull(helper);
	}

	@Test
	public void testWithGitdir() throws Exception {
		JGitHelper lhelper = new JGitHelper("./.git");
		assertNotNull(lhelper);
		lhelper.close();
	}

	@Test
	public void testNotexistingDir() throws Exception {
		try {
			JGitHelper jGitHelper = new JGitHelper("notexisting");
			jGitHelper.close();
			fail("Should catch exception here");
		} catch (IllegalStateException e) {
			assertTrue(e.getMessage().contains("notexisting"));
		}
	}

	@Test
	public void testReadCommit() throws Exception {
		assertEquals("abcd", helper.readCommit("abcd"));
		assertEquals("abcd", helper.readCommit("/commit/abcd"));
		assertEquals("1234567890123456789012345678901234567890",
				helper.readCommit("/commit/12345678901234567890123456789012345678901234567890"));
		assertEquals("1234567890123456789012345678901234567890",
				helper.readCommit("/commit/1234567890123456789012345678901234567890/blabla"));
	}

	@Test
	public void testReadPath() throws Exception {
		assertEquals("", helper.readPath("abcd"));
		assertEquals("", helper.readPath("/commit/abcd"));
		assertEquals("blabla", helper.readPath("/commit/1234567890123456789012345678901234567890/blabla"));
	}

	@Test
	public void testReadType() throws Exception {
		final StatWrapper wrapper = getStatsWrapper();

		System.out.println("Had commit: " + DEFAULT_COMMIT);
		helper.readType(DEFAULT_COMMIT, "src", wrapper);
		assertEquals(NodeType.DIRECTORY, wrapper.type());

		helper.readType(DEFAULT_COMMIT, "src/main/java/org", wrapper);
		assertEquals(NodeType.DIRECTORY, wrapper.type());

		helper.readType(DEFAULT_COMMIT, "README.md", wrapper);
		assertEquals(NodeType.FILE, wrapper.type());
		assertTrue((wrapper.mode() & TypeMode.S_IXUSR) == 0);
		assertTrue((wrapper.mode() & TypeMode.S_IXGRP) == 0);
		assertTrue((wrapper.mode() & TypeMode.S_IXOTH) == 0);

		helper.readType(DEFAULT_COMMIT, "src/main/java/org/dstadler/jgitfs/JGitFS.java", wrapper);
		assertEquals(NodeType.FILE, wrapper.type());
		assertTrue((wrapper.mode() & TypeMode.S_IXUSR) == 0);
		assertTrue((wrapper.mode() & TypeMode.S_IXGRP) == 0);
		assertTrue((wrapper.mode() & TypeMode.S_IXOTH) == 0);
	}

	@Test
	public void testReadTypeFails() throws Exception {
		final StatWrapper wrapper = getStatsWrapper();
		assertFalse(helper.readType(DEFAULT_COMMIT, "notexisting", wrapper));
	}

	@Test
	public void testReadTypeExecutable() throws Exception {
		final StatWrapper wrapper = getStatsWrapper();
		// Look at a specific older commit to have an executable file
		helper.readType("355ea52f1e38b1c8e6537c093332180918808b68", "run.sh", wrapper);
		assertEquals(NodeType.FILE, wrapper.type());
		assertTrue((wrapper.mode() & TypeMode.S_IXUSR) != 0);
		assertTrue((wrapper.mode() & TypeMode.S_IXGRP) != 0);
		assertTrue((wrapper.mode() & TypeMode.S_IXOTH) != 0);
	}

	private StatWrapper getStatsWrapper() {
		final StatWrapper wrapper;
		try {
			wrapper = StatWrapperFactory.create();
		} catch (UnsatisfiedLinkError e) {
			System.out.println("This might fail on machines without fuse-binaries.");
			e.printStackTrace();
			Assume.assumeNoException(e);	// stop test silently
			return null;
		} catch(NoClassDefFoundError e) {
			System.out.println("This might fail on machines without fuse-binaries.");
			e.printStackTrace();
			Assume.assumeNoException(e);	// stop test silently
			return null;
		}
		return wrapper;
	}

	@Test
	public void testOpenFile() throws Exception {
		System.out.println("Had commit: " + DEFAULT_COMMIT);
		String runSh = IOUtils.toString(helper.openFile(DEFAULT_COMMIT, "README.md"));
		assertTrue("Had: " + runSh, StringUtils.isNotEmpty(runSh));
	}

	@Test
	public void testOpenFileFails() throws Exception {
		try {
			IOUtils.toString(helper.openFile(DEFAULT_COMMIT, "src"));
			fail("Should catch exception here");
		} catch (IllegalStateException e) {
			assertTrue(e.getMessage().contains("src"));
		}

		try {
			IOUtils.toString(helper.openFile(DEFAULT_COMMIT, "src/org"));
			fail("Should catch exception here");
		} catch (FileNotFoundException e) {
			assertTrue(e.getMessage().contains("src/org"));
		}

		try {
			IOUtils.toString(helper.openFile(DEFAULT_COMMIT, "notexisting"));
			fail("Should catch exception here");
		} catch (FileNotFoundException e) {
			assertTrue(e.getMessage().contains("notexisting"));
		}
	}

	@Test
	public void testReadElementsAt() throws Exception {
		System.out.println("Had commit: " + DEFAULT_COMMIT);
		assertEquals("[main, test]", helper.readElementsAt(DEFAULT_COMMIT, "src").toString());
		assertEquals("[dstadler]", helper.readElementsAt(DEFAULT_COMMIT, "src/main/java/org").toString());

		String list = helper.readElementsAt(DEFAULT_COMMIT, "").toString();
		assertTrue("Had: " + list, list.contains("src"));
		assertTrue("Had: " + list, list.contains("README.md"));
		assertTrue("Had: " + list, list.contains("build.gradle"));
	}

	@Test
	public void testReadElementsAtFails() throws Exception {
		try {
			helper.readElementsAt(DEFAULT_COMMIT, "run.sh");
			fail("Should catch exception here");
		} catch (FileNotFoundException e) {
			assertTrue(e.getMessage().contains("run.sh"));
		}

		try {
			helper.readElementsAt(DEFAULT_COMMIT, "README.md");
			fail("Should catch exception here");
		} catch (IllegalStateException e) {
			assertTrue(e.getMessage().contains("README.md"));
		}

		try {
			helper.readElementsAt(DEFAULT_COMMIT, "notexisting");
			fail("Should catch exception here");
		} catch (FileNotFoundException e) {
			assertTrue(e.getMessage().contains("notexisting"));
		}
	}

	@Test
	public void testGetBranchHeadCommit() throws IOException {
		assertNull(helper.getBranchHeadCommit("somebranch"));
		assertEquals(DEFAULT_COMMIT, helper.getBranchHeadCommit("__testbranch"));
		assertEquals(DEFAULT_COMMIT, helper.getBranchHeadCommit("__test/branch"));
	}

	@Test
	public void testGetRemoteHeadCommit() throws IOException {
		assertNull(helper.getRemoteHeadCommit("somebranch"));
		assertNotNull(helper.getRemoteHeadCommit("__origin/testbranch"));
		assertNotNull(helper.getRemoteHeadCommit("__origin/test/branch"));
	}

	@Test
	public void testGetBranches() throws IOException {
		List<String> branches = helper.getBranches();
		assertTrue(branches.size() > 0);
		assertTrue("Had: " + branches.toString(), branches.contains("__testbranch"));
		assertTrue("Had: " + branches.toString(), branches.contains("__test/branch"));
	}

	@Test
	public void testGetRemotes() throws IOException {
		List<String> remotes = helper.getRemotes();
		assertTrue(remotes.size() > 0);
		assertTrue("Had: " + remotes.toString(), remotes.contains("__origin/testbranch"));
		assertTrue("Had: " + remotes.toString(), remotes.contains("__origin/test/branch"));
	}

	@Test
	public void testGetTagHead() throws IOException {
		assertNull(helper.getTagHeadCommit("sometag"));
		assertNotNull(helper.getTagHeadCommit("__testtag"));
		assertNotNull(helper.getTagHeadCommit("__test/tag"));
	}

	@Test
	public void testGetTags() throws IOException {
		List<String> tags = helper.getTags();
		assertTrue(tags.size() > 0);
		assertTrue("Had: " + tags.toString(), tags.contains("__testtag"));
		assertTrue("Had: " + tags.toString(), tags.contains("__test/tag"));
	}

	@Ignore("local test")
	@Test
	public void testSubversionEmptyFile() throws Exception {
		JGitHelper jgitHelper = new JGitHelper("/opt/Subversion/git");
		List<String> items = jgitHelper.getBranches();
		assertNotNull(items);
		assertTrue(items.contains("ppa_1.7.11"));

		String commit = jgitHelper.getBranchHeadCommit("ppa_1.7.11");
		assertNotNull(commit);

		items = jgitHelper.readElementsAt(commit, "");
		assertNotNull(items);
		assertTrue(items.size() > 0);

		//subversion/branch/ppa_1.7.11/build/generator/__init__.py

		items = jgitHelper.readElementsAt(commit, "build");
		assertNotNull(items);
		assertTrue(items.size() > 0);

		items = jgitHelper.readElementsAt(commit, "build/generator");
		assertNotNull(items);
		assertTrue(items.size() > 0);
		assertTrue("Had: " + items, items.contains("__init__.py"));

		InputStream openFile = jgitHelper.openFile(commit, "build/generator/__init__.py");
		try {
			String string = IOUtils.toString(openFile);
			System.out.println("Having " + string.length() + " bytes: \n" + string);
		} finally {
			openFile.close();
		}


		openFile = jgitHelper.openFile(commit, "build/generator/__init__.py");

		try {
			// skip until we are at the offset
			openFile.skip(0);

			byte[] arr = new byte[4096];
			int read = openFile.read(arr, 0, 4096);
			System.out.println("Had: " + read);
		} finally {
			openFile.close();
		}


		jgitHelper.close();
	}

	@Test
	public void testWithTestdata() throws IOException {
		String commit = helper.getBranchHeadCommit("master");

		// check that the test-data is there
		List<String> elements = helper.readElementsAt(commit, "src/test/data");
		assertEquals("Had: " + elements, 4, elements.size());
		assertTrue(elements.contains("emptytestfile"));
		assertTrue(elements.contains("one"));
		assertTrue(elements.contains("symlink"));
		assertTrue(elements.contains("rellink"));

		// check type of files
		final StatWrapper wrapper = getStatsWrapper();
		helper.readType(commit, "src/test/data", wrapper);
		assertEquals(NodeType.DIRECTORY, wrapper.type());
		helper.readType(commit, "src/test/data/emptytestfile", wrapper);
		assertEquals(NodeType.FILE, wrapper.type());
		helper.readType(commit, "src/test/data/one", wrapper);
		assertEquals(NodeType.FILE, wrapper.type());
		helper.readType(commit, "src/test/data/symlink", wrapper);
		assertEquals(NodeType.SYMBOLIC_LINK, wrapper.type());
		helper.readType(commit, "src/test/data/rellink", wrapper);
		assertEquals(NodeType.SYMBOLIC_LINK, wrapper.type());

		// check that the empty file is actually empty
		InputStream stream = helper.openFile(commit, "src/test/data/emptytestfile");
		try {
			assertEquals("", IOUtils.toString(stream));
		} finally {
			stream.close();
		}

		// check that the file has the correct content
		stream = helper.openFile(commit, "src/test/data/one");
		try {
			assertEquals("1", IOUtils.toString(stream).trim());
		} finally {
			stream.close();
		}

		// check that we can read the symlink
		stream = helper.openFile(commit, "src/test/data/symlink");
		try {
			assertEquals("Should be 'one' as it contains the filename of the file pointed to!",
					"one", IOUtils.toString(stream).trim());
		} finally {
			stream.close();
		}
		stream = helper.openFile(commit, "src/test/data/rellink");
		try {
			assertEquals("Should be '../../../build.gradle' as it contains the filename of the file pointed to!",
					"../../../build.gradle", IOUtils.toString(stream).trim());
		} finally {
			stream.close();
		}

		// read the symlinks
		assertArrayEquals("one".getBytes(Charsets.US_ASCII),
				helper.readSymlink(commit, "src/test/data/symlink"));
		assertArrayEquals("../../../build.gradle".getBytes(Charsets.US_ASCII),
				helper.readSymlink(commit, "src/test/data/rellink"));
		try {
			helper.readSymlink(commit, "src/test/data/one");
			fail("Should not be able to read symlink for normal file");
		} catch (IllegalArgumentException e) {
			assertTrue(e.getMessage().contains("src/test/data/one"));
		}
		try {
			helper.readSymlink(commit, "src/test/data");
			fail("Should not be able to read symlink for directory");
		} catch (IllegalArgumentException e) {
			assertTrue(e.getMessage().contains("src/test/data"));
		}

	}

	@Test
    public void testToString() throws Exception {
        // toString should not return null
        assertNotNull("A derived toString() should not return null!", helper.toString());

        // toString should not return an empty string
        assertFalse("A derived toString() should not return an empty string!", helper.toString().equals(""));

        // check that calling it multiple times leads to the same value
        String value = helper.toString();
        for (int i = 0; i < 10; i++) {
            assertEquals("toString() is expected to result in the same result across repeated calls!", value,
                    helper.toString());
        }
	    
    }
}
