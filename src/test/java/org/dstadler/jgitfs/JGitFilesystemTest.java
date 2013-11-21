package org.dstadler.jgitfs;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import net.fusejna.DirectoryFiller;
import net.fusejna.ErrorCodes;
import net.fusejna.FuseException;
import net.fusejna.StatWrapperFactory;
import net.fusejna.StructStat.StatWrapper;
import net.fusejna.types.TypeMode.NodeType;

import org.dstadler.jgitfs.util.FuseUtils;
import org.dstadler.jgitfs.util.JGitHelperTest;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;


public class JGitFilesystemTest {
	private static final String DEFAULT_COMMIT_PATH = "/commit/" + JGitHelperTest.DEFAULT_COMMIT;
	private static final String DEFAULT_TREE_PATH = "/tree/" + JGitHelperTest.DEFAULT_TREE;

	private JGitFilesystem fs;

	@BeforeClass
	public static void setUpClass() throws GitAPIException, IOException {
		JGitHelperTest.setUpClass();
	}

	@AfterClass
	public static void tearDownClass() throws GitAPIException, IOException {
		JGitHelperTest.tearDownClass();
	}

	@Before
	public void setUp() throws IOException {
		fs = new JGitFilesystem(".", false);
	}

	@After
	public void tearDown() throws IOException {
		fs.close();
	}

	@Test
	public void testConstructClose() throws IOException {
		// do nothing here, just construct and close the fs in before/after...
	}

	@Test
	public void testConstructMountClose() throws IOException, FuseException {
		// ensure that we can actually load FUSE-binaries before we try to mount/unmount
		// an assumption will fail if the binaries are missing
		assertNotNull(getStatsWrapper());

		File mountPoint = mount();

		unmount(mountPoint);
	}

	@Test
	public void testGetAttr() throws IOException, FuseException {
		StatWrapper stat = getStatsWrapper();
		assertEquals(0, fs.getattr("/", stat));
		assertEquals(NodeType.DIRECTORY, stat.type());
		assertEquals(0, fs.getattr("/commit", stat));
		assertEquals(NodeType.DIRECTORY, stat.type());
		assertEquals(0, fs.getattr("/tree", stat));
		assertEquals(NodeType.DIRECTORY, stat.type());
		assertEquals(0, fs.getattr("/tag", stat));
		assertEquals(NodeType.DIRECTORY, stat.type());
		assertEquals(0, fs.getattr("/branch", stat));
		assertEquals(NodeType.DIRECTORY, stat.type());
		assertEquals(0, fs.getattr("/remote", stat));
		assertEquals(NodeType.DIRECTORY, stat.type());
		assertEquals(0, fs.getattr(DEFAULT_COMMIT_PATH, stat));
		assertEquals(NodeType.DIRECTORY, stat.type());
		assertEquals(0, fs.getattr(DEFAULT_COMMIT_PATH + "/README.md", stat));
		assertEquals(NodeType.FILE, stat.type());
		assertEquals(0, fs.getattr(DEFAULT_TREE_PATH, stat));
		assertEquals(NodeType.DIRECTORY, stat.type());
		assertEquals(0, fs.getattr(DEFAULT_TREE_PATH + "/README.md", stat));
		assertEquals(NodeType.FILE, stat.type());
		assertEquals(0, fs.getattr("/branch/__testbranch", stat));
		assertEquals(NodeType.SYMBOLIC_LINK, stat.type());
		assertEquals(0, fs.getattr("/branch/__test", stat));
		assertEquals(NodeType.DIRECTORY, stat.type());
		assertEquals(0, fs.getattr("/branch/__test/branch", stat));
		assertEquals(NodeType.SYMBOLIC_LINK, stat.type());
		assertEquals(0, fs.getattr("/tag/__testtag", stat));
		assertEquals(NodeType.SYMBOLIC_LINK, stat.type());
		assertEquals(0, fs.getattr("/tag/__test", stat));
		assertEquals(NodeType.DIRECTORY, stat.type());
		assertEquals(0, fs.getattr("/tag/__test/tag", stat));
		assertEquals(NodeType.SYMBOLIC_LINK, stat.type());
		assertEquals(0, fs.getattr("/remote/__origin", stat));
		assertEquals(NodeType.DIRECTORY, stat.type());
		assertEquals(0, fs.getattr("/remote/__origin/testbranch", stat));
		assertEquals(NodeType.SYMBOLIC_LINK, stat.type());
		assertEquals(0, fs.getattr("/remote/__origin/test", stat));
		assertEquals(NodeType.DIRECTORY, stat.type());
		assertEquals(0, fs.getattr("/remote/__origin/test/branch", stat));
		assertEquals(NodeType.SYMBOLIC_LINK, stat.type());

		// invalid file-name causes IllegalStateException
		String path = DEFAULT_COMMIT_PATH + "/notexist.txt";
		assertEquals(-ErrorCodes.ENOENT(), fs.getattr(path, stat));
		// invalid top-level-dir causes ENOENT
		assertEquals(-ErrorCodes.ENOENT(), fs.getattr("/notexistingmain", stat));
		
		// hidden dirs are not found and not printed to stdout
		assertEquals(-ErrorCodes.ENOENT(), fs.getattr("/.Trash", stat));
		assertEquals(-ErrorCodes.ENOENT(), fs.getattr("/tag/123/.hidden", stat));
		assertEquals(-ErrorCodes.ENOENT(), fs.getattr("/branch/123/.hidden", stat));
		assertEquals(-ErrorCodes.ENOENT(), fs.getattr("/remote/123/.hidden", stat));
		assertEquals(-ErrorCodes.ENOENT(), fs.getattr("/master/some/file/direct/.hidden", stat));
	}

	@Test
	public void testRead() {
		ByteBuffer buffer = ByteBuffer.allocate(100);
		assertEquals(100, fs.read(DEFAULT_COMMIT_PATH + "/README.md", buffer, 100, 0, null));
		buffer.rewind();
		assertEquals(100, fs.read(DEFAULT_TREE_PATH + "/README.md", buffer, 100, 0, null));
	}

	@Test
	public void testReadTooMuch() {
		ByteBuffer buffer = ByteBuffer.allocate(100000);
		int read = fs.read(DEFAULT_COMMIT_PATH + "/README.md", buffer, 100000, 0, null);
		assertEquals(4816, read);
		buffer.rewind();
		read = fs.read(DEFAULT_TREE_PATH + "/README.md", buffer, 100000, 0, null);
		assertEquals(4816, read);
	}

	@Test
	public void testReadFails() {
		assertEquals(-ErrorCodes.ENOENT(), fs.read("/somepath", null, 0, 0, null));
	}

	@Test
	public void testReadDir() {
		final List<String> filledFiles = new ArrayList<String>();
		DirectoryFiller filler = new DirectoryFillerImplementation(filledFiles);

		fs.readdir("/", filler);
		assertEquals("[/branch, /commit, /remote, /tag, /tree]", filledFiles.toString());

		filledFiles.clear();
		fs.readdir("/tag", filler);
		assertTrue("Had: " + filledFiles.toString(), filledFiles.contains("__testtag"));
		assertTrue("Had: " + filledFiles.toString(), filledFiles.contains("__test"));

		filledFiles.clear();
		fs.readdir("/tag/__test", filler);
		assertTrue("Had: " + filledFiles.toString(), filledFiles.contains("tag"));

		filledFiles.clear();
		fs.readdir("/branch", filler);
		assertTrue("Had: " + filledFiles.toString(), filledFiles.contains("__testbranch"));
		assertTrue("Had: " + filledFiles.toString(), filledFiles.contains("__test"));

		filledFiles.clear();
		fs.readdir("/branch/__test", filler);
		assertTrue("Had: " + filledFiles.toString(), filledFiles.contains("branch"));

		filledFiles.clear();
		fs.readdir("/remote", filler);
		assertTrue("Had: " + filledFiles.toString(), filledFiles.contains("__origin"));

		filledFiles.clear();
		fs.readdir("/remote/__origin", filler);
		assertTrue("Had: " + filledFiles.toString(), filledFiles.contains("testbranch"));
		assertTrue("Had: " + filledFiles.toString(), filledFiles.contains("test"));

		filledFiles.clear();
		fs.readdir("/remote/__origin/test", filler);
		assertTrue("Had: " + filledFiles.toString(), filledFiles.contains("branch"));

		filledFiles.clear();
		fs.readdir("/commit", filler);
		assertTrue(filledFiles.isEmpty());

		filledFiles.clear();
		fs.readdir(DEFAULT_COMMIT_PATH, filler);
		assertTrue("Had: " + filledFiles.toString(), filledFiles.contains("README.md"));

		filledFiles.clear();
		fs.readdir(DEFAULT_COMMIT_PATH + "/src", filler);
		assertEquals("Had: " + filledFiles.toString(), "[main, test]", filledFiles.toString());

		filledFiles.clear();
		fs.readdir(DEFAULT_TREE_PATH, filler);
		assertTrue("Had: " + filledFiles.toString(), filledFiles.contains("README.md"));

		filledFiles.clear();
		fs.readdir(DEFAULT_TREE_PATH + "/src", filler);
		assertEquals("Had: " + filledFiles.toString(), "[main, test]", filledFiles.toString());
	}

	@Test
	public void testReadDirPathFails() {
		final List<String> filledFiles = new ArrayList<String>();
		DirectoryFiller filler = new DirectoryFillerImplementation(filledFiles);
		assertEquals(-ErrorCodes.ENOENT(), fs.readdir(DEFAULT_COMMIT_PATH + "/notexisting", filler));
		assertEquals(-ErrorCodes.ENOENT(), fs.readdir(DEFAULT_TREE_PATH + "/notexisting", filler));
	}

	@Test
	public void testReadDirTag() {
		final List<String> filledFiles = new ArrayList<String>();
		DirectoryFiller filler = new DirectoryFillerImplementation(filledFiles);

		fs.readdir("/tag", filler);
		assertTrue("Had: " + filledFiles.toString(), filledFiles.contains("__testtag"));
		assertTrue("Had: " + filledFiles.toString(), filledFiles.contains("__test"));

		fs.readdir("/tag/__test", filler);
		assertTrue("Had: " + filledFiles.toString(), filledFiles.contains("tag"));
	}

	@Test
	public void testReadDirTagFails() throws IOException {
		final List<String> filledFiles = new ArrayList<String>();
		DirectoryFiller filler = new DirectoryFillerImplementation(filledFiles);

		fs.close();
		fs.close();

		// for some reason this does not fail, seems the Git repository still works even if closed
		fs.readdir("/tag", filler);
		assertTrue("Had: " + filledFiles.toString(), filledFiles.contains("__testtag"));
	}

	@Test
	public void testReadDirBranch() {
		final List<String> filledFiles = new ArrayList<String>();
		DirectoryFiller filler = new DirectoryFillerImplementation(filledFiles);

		fs.readdir("/branch", filler);
		assertTrue("Had: " + filledFiles.toString(), filledFiles.contains("__testbranch"));
		assertTrue("Had: " + filledFiles.toString(), filledFiles.contains("__test"));

		fs.readdir("/branch/__test", filler);
		assertTrue("Had: " + filledFiles.toString(), filledFiles.contains("branch"));
	}

	@Test
	public void testReadDirRemote() {
		final List<String> filledFiles = new ArrayList<String>();
		DirectoryFiller filler = new DirectoryFillerImplementation(filledFiles);

		fs.readdir("/remote", filler);
		assertTrue("Had: " + filledFiles.toString(), filledFiles.contains("__origin"));

		filledFiles.clear();
		fs.readdir("/remote/__origin", filler);
		assertTrue("Had: " + filledFiles.toString(), filledFiles.contains("testbranch"));
		assertTrue("Had: " + filledFiles.toString(), filledFiles.contains("test"));

		filledFiles.clear();
		fs.readdir("/remote/__origin/test", filler);
		assertTrue("Had: " + filledFiles.toString(), filledFiles.contains("branch"));
	}

	@Test
	public void testReadDirFails() {
		try {
			fs.readdir("/somepath", null);
			fail("Should throw exception as this should not occur");
		} catch (IllegalStateException e) {
			assertTrue(e.toString(), e.toString().contains("Error reading directories"));
			assertTrue(e.toString(), e.toString().contains("/somepath"));
		}
	}

	@Test
	public void testReadLinkTag() {
		ByteBuffer buffer = ByteBuffer.allocate(100);
		int readlink = fs.readlink("/tag/__testtag", buffer, 100);
		assertEquals("Had: " + readlink + ": " + new String(buffer.array()), 0, readlink);

		String target = new String(buffer.array(), 0, buffer.position());
		assertTrue("Had: " + target, target.startsWith("../commit"));

		buffer.rewind();
		readlink = fs.readlink("/tag/__test/tag", buffer, 100);
		assertEquals("Had: " + readlink + ": " + new String(buffer.array()), 0, readlink);

		target = new String(buffer.array(), 0, buffer.position());
		assertTrue("Had: " + target, target.startsWith("../../commit"));
	}

	@Test
	public void testReadLinkBranch() {
		ByteBuffer buffer = ByteBuffer.allocate(100);
		int readlink = fs.readlink("/branch/__testbranch", buffer, 100);
		assertEquals("Had: " + readlink + ": " + new String(buffer.array()), 0, readlink);

		String target = new String(buffer.array(), 0, buffer.position());
		assertTrue("Had: " + target, target.startsWith("../commit"));

		buffer.rewind();
		readlink = fs.readlink("/branch/__test/branch", buffer, 100);
		assertEquals("Had: " + readlink + ": " + new String(buffer.array()), 0, readlink);

		target = new String(buffer.array(), 0, buffer.position());
		assertTrue("Had: " + target, target.startsWith("../../commit"));
	}

	@Test
	public void testReadLinkRemote() {
		ByteBuffer buffer = ByteBuffer.allocate(100);
		int readlink = fs.readlink("/remote/__origin/testbranch", buffer, 100);
		assertEquals("Had: " + readlink + ": " + new String(buffer.array()), 0, readlink);

		String target = new String(buffer.array(), 0, buffer.position());
		assertTrue("Had: " + target, target.startsWith("../../commit"));

		buffer.rewind();
		readlink = fs.readlink("/remote/__origin/test/branch", buffer, 100);
		assertEquals("Had: " + readlink + ": " + new String(buffer.array()), 0, readlink);

		target = new String(buffer.array(), 0, buffer.position());
		assertTrue("Had: " + target, target.startsWith("../../../commit"));
	}

	@Test
	public void testReadLinkRemoteFails() {
		ByteBuffer buffer = ByteBuffer.allocate(100);
		int readlink = fs.readlink("/remote/nonexisting_master", buffer, 100);
		assertEquals("Had: " + readlink + ": " + new String(buffer.array()), -ErrorCodes.ENOENT(), readlink);

		assertEquals(0, buffer.position());
	}

	@Test
	public void testReadLinkExceedSize() {
		ByteBuffer buffer = ByteBuffer.allocate(21);
		try {
			fs.readlink("/tag/__testtag", buffer, 21);
			fail("Should catch exception here");
		} catch (BufferOverflowException e) {
			// expected...
		}
	}

	@Test
	public void testReadLinkDifferentSize() {
		ByteBuffer buffer = ByteBuffer.allocate(21);
		try {
			fs.readlink("/tag/__testtag", buffer, 30);
			fail("Should catch exception here");
		} catch (BufferOverflowException e) {
			// expected...
		}
	}

	@Test
	public void testReadLinkUnknown() {
		assertEquals(-ErrorCodes.ENOENT(), fs.readlink("/branch/notexisting", null, 0));
		assertEquals(-ErrorCodes.ENOENT(), fs.readlink("/somepath", null, 0));
	}

	private File mount() throws IOException, UnsatisfiedLinkError, FuseException {
		File mountPoint = File.createTempFile("git-fs-mount", ".test");
		assertTrue(mountPoint.delete());

		FuseUtils.prepareMountpoint(mountPoint);

		fs.mount(mountPoint, false);

		return mountPoint;
	}

	private void unmount(File mountPoint) throws IOException, FuseException {
		fs.unmount();

		mountPoint.delete();
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

	private static final int NUMBER_OF_THREADS = 7;
	private static final int NUMBER_OF_TESTS = 500;

	@Test
	@Ignore("takes too long currently, need to revisit later")
    public void testMultipleThreads() throws Throwable {
        ThreadTestHelper helper =
            new ThreadTestHelper(NUMBER_OF_THREADS, NUMBER_OF_TESTS);

        helper.executeTest(new ThreadTestHelper.TestRunnable() {
            @Override
            public void doEnd(int threadnum) throws Exception {
                // do stuff at the end ...
            }

            @Override
            public void run(int threadnum, int iter) throws Exception {
            	switch (threadnum) {
					case 0:
						testGetAttr();
						break;
					case 1:
						testRead();
						break;
					case 2:
						testReadDir();
						break;
					case 3:
						testReadLinkBranch();
						break;
					case 4:
						testReadLinkBranch();
						break;
					case 5:
						testReadLinkTag();
						break;
					case 6:
						testWalkRecursively();
						break;
					default:
						throw new IllegalStateException("No work for thread " + threadnum);
				}
            }

        });
    }

	@Test
	public void testWalkRecursively() {
		StatWrapper stat = getStatsWrapper();
		assertEquals(0, fs.getattr("/", stat));

		final List<String> filledFiles = new ArrayList<String>();
		DirectoryFiller filler = new DirectoryFillerImplementation(filledFiles);

		assertEquals(0, fs.readdir("/", filler));
		assertEquals("[/branch, /commit, /remote, /tag, /tree]", filledFiles.toString());

		for(String file : new ArrayList<String>(filledFiles)) {
			assertEquals(0, fs.getattr(file, stat));
			assertEquals(0, fs.readdir(file, filler));
		}

		filledFiles.clear();
		assertEquals(0, fs.readdir("/commit", filler));
		assertTrue(filledFiles.isEmpty());

		filledFiles.clear();
		assertEquals(0, fs.readdir("/tree", filler));
		assertTrue(filledFiles.isEmpty());

		filledFiles.clear();
		assertEquals(0, fs.readdir("/branch", filler));
		for(String file : new ArrayList<String>(filledFiles)) {
			assertEquals(0, fs.getattr("/branch/" + file, stat));
			if (stat.type() != NodeType.SYMBOLIC_LINK) {
				assertEquals(NodeType.DIRECTORY, stat.type());
				filledFiles.clear();
				for(String subfile : new ArrayList<String>(filledFiles)) {
					assertEquals(0, fs.getattr("/branch/" + file + "/" + subfile, stat));
				}
			}
		}

		filledFiles.clear();
		assertEquals(0, fs.readdir("/remote", filler));
		for(String file : new ArrayList<String>(filledFiles)) {
			assertEquals(0, fs.getattr("/remote/" + file, stat));
			assertEquals(NodeType.DIRECTORY, stat.type());
			//fs.readlink("/branch/" + file, ByteBuffer.allocate(capacity), size)
		}
	}

	@Test
	public void testWithTestData() {
		ByteBuffer buffer = ByteBuffer.allocate(1000);
		assertEquals(0, fs.readlink("/branch/master", buffer, 1000));
		assertEquals("A commit-ish link should be written to the buffer, but had: " + new String(buffer.array(), 0, buffer.position()), 
				1000-50, buffer.remaining());
		// e.g. ../commit/4327273e69afcd040ba1b4d3766ea1f43e0024f3
		String commit = new String(buffer.array(), 0, buffer.position()).substring(2);
		
		// check that the test-data is there
		final List<String> filledFiles = new ArrayList<String>();
		DirectoryFiller filler = new DirectoryFillerImplementation(filledFiles);
		assertEquals(0, fs.readdir(commit + "/src/test/data", filler));
		assertEquals("Had: " + filledFiles, 4, filledFiles.size());
		assertTrue(filledFiles.contains("emptytestfile"));
		assertTrue(filledFiles.contains("one"));
		assertTrue(filledFiles.contains("symlink"));
		assertTrue(filledFiles.contains("rellink"));
		
		
		// check type of files
		final StatWrapper wrapper = getStatsWrapper();
		assertEquals(0, fs.getattr(commit + "/src/test/data", wrapper));
		assertEquals(NodeType.DIRECTORY, wrapper.type());
		assertEquals(0, fs.getattr(commit + "/src/test/data/emptytestfile", wrapper));
		assertEquals(NodeType.FILE, wrapper.type());
		assertEquals(0, fs.getattr(commit + "/src/test/data/one", wrapper));
		assertEquals(NodeType.FILE, wrapper.type());		
		assertEquals(0, fs.getattr(commit + "/src/test/data/symlink", wrapper));
		assertEquals(NodeType.SYMBOLIC_LINK, wrapper.type());		
		assertEquals(0, fs.getattr(commit + "/src/test/data/rellink", wrapper));
		assertEquals(NodeType.SYMBOLIC_LINK, wrapper.type());		
		
		// check that the empty file is actually empty
		buffer = ByteBuffer.allocate(1000);
		assertEquals(0, fs.read(commit + "/src/test/data/emptytestfile", buffer, 1000, 0, null));
		assertEquals("No data should be written to the buffer", 1000, buffer.remaining());
		
		// check that the file has the correct content
		buffer = ByteBuffer.allocate(1000);
		assertEquals(2, fs.read(commit + "/src/test/data/one", buffer, 1000, 0, null));
		assertEquals("Only two bytes should be written to the buffer", 998, buffer.remaining());
		assertEquals("1", new String(buffer.array(), 0, 1));
		
		// check that we can read the symlink
		buffer = ByteBuffer.allocate(1000);
		assertEquals(3, fs.read(commit + "/src/test/data/symlink", buffer, 1000, 0, null));
		assertEquals("Three bytes should be written to the buffer", 997, buffer.remaining());
		assertEquals("one", new String(buffer.array(), 0, buffer.position()));
		
		buffer = ByteBuffer.allocate(1000);
		assertEquals(21, fs.read(commit + "/src/test/data/rellink", buffer, 1000, 0, null));
		assertEquals("21 bytes should be written to the buffer", 979, buffer.remaining());
		assertEquals("../../../build.gradle", new String(buffer.array(), 0, buffer.position()));
		
		// reading the link-target of symlinks should return the correct link
		buffer = ByteBuffer.allocate(1000);
		assertEquals(0, fs.readlink(commit + "/src/test/data/symlink", buffer, 1000));
		assertEquals("Three bytes should be written to the buffer", 997, buffer.remaining());
		assertEquals("one", new String(buffer.array(), 0, buffer.position()));

		buffer = ByteBuffer.allocate(1000);
		assertEquals(0, fs.readlink(commit + "/src/test/data/rellink", buffer, 1000));
		assertEquals("21 bytes should be written to the buffer", 979, buffer.remaining());
		assertEquals("../../../build.gradle", new String(buffer.array(), 0, buffer.position()));
	}

	@Test
	public void testWithTestDataRemote() {
		ByteBuffer buffer = ByteBuffer.allocate(1000);
		assertEquals(0, fs.readlink("/remote/origin/master", buffer, 1000));
		assertEquals("A commit-ish link should be written to the buffer, but had: " + new String(buffer.array(), 0, buffer.position()), 
				1000-53, buffer.remaining());
		// e.g. ../../commit/4327273e69afcd040ba1b4d3766ea1f43e0024f3
		String commit = new String(buffer.array(), 0, buffer.position()).substring(5);
		
		// check that the test-data is there
		final List<String> filledFiles = new ArrayList<String>();
		DirectoryFiller filler = new DirectoryFillerImplementation(filledFiles);
		assertEquals(0, fs.readdir(commit + "/src/test/data", filler));
		assertEquals("Had: " + filledFiles, 4, filledFiles.size());
		assertTrue(filledFiles.contains("emptytestfile"));
		assertTrue(filledFiles.contains("one"));
		assertTrue(filledFiles.contains("symlink"));
		assertTrue(filledFiles.contains("rellink"));
		
		
		// check type of files
		final StatWrapper wrapper = getStatsWrapper();
		assertEquals(0, fs.getattr(commit + "/src/test/data", wrapper));
		assertEquals(NodeType.DIRECTORY, wrapper.type());
		assertEquals(0, fs.getattr(commit + "/src/test/data/emptytestfile", wrapper));
		assertEquals(NodeType.FILE, wrapper.type());
		assertEquals(0, fs.getattr(commit + "/src/test/data/one", wrapper));
		assertEquals(NodeType.FILE, wrapper.type());		
		assertEquals(0, fs.getattr(commit + "/src/test/data/symlink", wrapper));
		assertEquals(NodeType.SYMBOLIC_LINK, wrapper.type());		
		assertEquals(0, fs.getattr(commit + "/src/test/data/rellink", wrapper));
		assertEquals(NodeType.SYMBOLIC_LINK, wrapper.type());		
		
		// check that the empty file is actually empty
		buffer = ByteBuffer.allocate(1000);
		assertEquals(0, fs.read(commit + "/src/test/data/emptytestfile", buffer, 1000, 0, null));
		assertEquals("No data should be written to the buffer", 1000, buffer.remaining());
		
		// check that the file has the correct content
		buffer = ByteBuffer.allocate(1000);
		assertEquals(2, fs.read(commit + "/src/test/data/one", buffer, 1000, 0, null));
		assertEquals("Only two bytes should be written to the buffer", 998, buffer.remaining());
		assertEquals("1", new String(buffer.array(), 0, 1));
		
		// check that we can read the symlink
		buffer = ByteBuffer.allocate(1000);
		assertEquals(3, fs.read(commit + "/src/test/data/symlink", buffer, 1000, 0, null));
		assertEquals("Three bytes should be written to the buffer", 997, buffer.remaining());
		assertEquals("one", new String(buffer.array(), 0, buffer.position()));
		
		buffer = ByteBuffer.allocate(1000);
		assertEquals(21, fs.read(commit + "/src/test/data/rellink", buffer, 1000, 0, null));
		assertEquals("21 bytes should be written to the buffer", 979, buffer.remaining());
		assertEquals("../../../build.gradle", new String(buffer.array(), 0, buffer.position()));
		
		// reading the link-target of symlinks should return the correct link
		buffer = ByteBuffer.allocate(1000);
		assertEquals(0, fs.readlink(commit + "/src/test/data/symlink", buffer, 1000));
		assertEquals("Three bytes should be written to the buffer", 997, buffer.remaining());
		assertEquals("one", new String(buffer.array(), 0, buffer.position()));

		buffer = ByteBuffer.allocate(1000);
		assertEquals(0, fs.readlink(commit + "/src/test/data/rellink", buffer, 1000));
		assertEquals("21 bytes should be written to the buffer", 979, buffer.remaining());
		assertEquals("../../../build.gradle", new String(buffer.array(), 0, buffer.position()));
	}

	private final class DirectoryFillerImplementation implements DirectoryFiller {
		private final List<String> filledFiles;

		private DirectoryFillerImplementation(List<String> filledFiles) {
			this.filledFiles = filledFiles;
		}

		@Override
		public boolean add(String... files) {
			for(String file : files) {
				filledFiles.add(file);
			}
			return true;
		}

		@Override
		public boolean add(Iterable<String> files) {
			for(String file : files) {
				filledFiles.add(file);
			}
			return true;
		}
	}
}
