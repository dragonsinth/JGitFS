package org.dstadler.jgitfs.util;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Test;



public class GitUtilsTest {

	@Test
	public void testIsCommitDir() {
		assertFalse(GitUtils.isCommitDir(""));
		assertFalse(GitUtils.isCommitDir("/"));
		assertFalse(GitUtils.isCommitDir("/something"));
		assertFalse(GitUtils.isCommitDir("/branch"));
		assertFalse(GitUtils.isCommitDir("/tag"));
		assertFalse(GitUtils.isCommitDir("/commit"));
		assertFalse(GitUtils.isCommitDir(GitUtils.COMMIT_SLASH + "00/"));
		assertFalse(GitUtils.isCommitDir(GitUtils.COMMIT_SLASH + "0g"));
		assertFalse(GitUtils.isCommitDir(GitUtils.COMMIT_SLASH + "fg"));
		assertFalse(GitUtils.isCommitDir(GitUtils.COMMIT_SLASH + "zz"));
		assertFalse(GitUtils.isCommitDir(GitUtils.COMMIT_SLASH + "00"));
		assertFalse(GitUtils.isCommitDir(GitUtils.COMMIT_SLASH + "ab"));
		assertFalse(GitUtils.isCommitDir(GitUtils.COMMIT_SLASH + "12345678901234567890123456789012345678901"));
		assertFalse(GitUtils.isCommitDir(GitUtils.COMMIT_SLASH + "1234567890123456789012345678901234567890/"));

		assertTrue(GitUtils.isCommitDir(GitUtils.COMMIT_SLASH + "1234567890123456789012345678901234567890"));
		assertTrue(GitUtils.isCommitDir(GitUtils.COMMIT_SLASH + "1234567890123456789012345678901234567890/foo"));
	}

	@Test
	public void testIsTreeDir() {
		assertFalse(GitUtils.isTreeDir(""));
		assertFalse(GitUtils.isTreeDir("/"));
		assertFalse(GitUtils.isTreeDir("/something"));
		assertFalse(GitUtils.isTreeDir("/branch"));
		assertFalse(GitUtils.isTreeDir("/tag"));
		assertFalse(GitUtils.isTreeDir("/tree"));
		assertFalse(GitUtils.isTreeDir(GitUtils.TREE_SLASH + "00/"));
		assertFalse(GitUtils.isTreeDir(GitUtils.TREE_SLASH + "0g"));
		assertFalse(GitUtils.isTreeDir(GitUtils.TREE_SLASH + "fg"));
		assertFalse(GitUtils.isTreeDir(GitUtils.TREE_SLASH + "zz"));
		assertFalse(GitUtils.isTreeDir(GitUtils.TREE_SLASH + "00"));
		assertFalse(GitUtils.isTreeDir(GitUtils.TREE_SLASH + "ab"));
		assertFalse(GitUtils.isTreeDir(GitUtils.TREE_SLASH + "12345678901234567890123456789012345678901"));
		assertFalse(GitUtils.isTreeDir(GitUtils.TREE_SLASH + "1234567890123456789012345678901234567890/"));

		assertTrue(GitUtils.isTreeDir(GitUtils.TREE_SLASH + "1234567890123456789012345678901234567890"));
		assertTrue(GitUtils.isTreeDir(GitUtils.TREE_SLASH + "1234567890123456789012345678901234567890/foo"));
	}

	@Test
	public void testIsBranchDir() {
		assertFalse(GitUtils.isBranchDir(""));
		assertFalse(GitUtils.isBranchDir("/"));
		assertFalse(GitUtils.isBranchDir("/something"));
		assertFalse(GitUtils.isBranchDir("/tag"));
		assertFalse(GitUtils.isBranchDir("/commit"));
		assertFalse(GitUtils.isBranchDir("/branch"));
		assertFalse(GitUtils.isBranchDir(GitUtils.BRANCH_SLASH + "ae/.hidden"));

		assertTrue(GitUtils.isBranchDir(GitUtils.BRANCH_SLASH + "00/"));
		assertTrue(GitUtils.isBranchDir(GitUtils.BRANCH_SLASH + "asdfasd/sjwekw"));
		assertTrue(GitUtils.isBranchDir(GitUtils.BRANCH_SLASH + "asdfasd_aldsjfasd asdlkjasdj.,.;_:;:öÖLP\"=)==\"§\"§%/\"!§)$§\""));

		assertTrue(GitUtils.isBranchDir(GitUtils.BRANCH_SLASH + "asdfasd"));
		assertTrue(GitUtils.isBranchDir(GitUtils.BRANCH_SLASH + "fg"));
		assertTrue(GitUtils.isBranchDir(GitUtils.BRANCH_SLASH + "zz"));
		assertTrue(GitUtils.isBranchDir(GitUtils.BRANCH_SLASH + "00"));
		assertTrue(GitUtils.isBranchDir(GitUtils.BRANCH_SLASH + "ff"));
		assertTrue(GitUtils.isBranchDir(GitUtils.BRANCH_SLASH + "ae"));
		assertTrue(GitUtils.isBranchDir(GitUtils.BRANCH_SLASH + "asdfasd_aldsjfasd asdlkjasdj.,.;_:;:öÖLP\"=)==\"§\"§%\"!§)$§\""));
	}

	@Test
	public void testIsTagDir() {
		assertFalse(GitUtils.isTagDir(""));
		assertFalse(GitUtils.isTagDir("/"));
		assertFalse(GitUtils.isTagDir("/something"));
		assertFalse(GitUtils.isTagDir("/branch"));
		assertFalse(GitUtils.isTagDir("/commit"));
		assertFalse(GitUtils.isTagDir("/tag"));
		assertFalse(GitUtils.isTagDir(GitUtils.BRANCH_SLASH + "ae/.hidden"));

		assertTrue(GitUtils.isTagDir(GitUtils.TAG_SLASH + "00/"));
		assertTrue(GitUtils.isTagDir(GitUtils.TAG_SLASH + "asdfasd/sjwekw"));
		assertTrue(GitUtils.isTagDir(GitUtils.TAG_SLASH + "asdfasd_aldsjfasd asdlkjasdj.,.;_:;:öÖLP\"=)==\"§\"§%/\"!§)$§\""));

		assertTrue(GitUtils.isTagDir(GitUtils.TAG_SLASH + "asdfasd"));
		assertTrue(GitUtils.isTagDir(GitUtils.TAG_SLASH + "fg"));
		assertTrue(GitUtils.isTagDir(GitUtils.TAG_SLASH + "zz"));
		assertTrue(GitUtils.isTagDir(GitUtils.TAG_SLASH + "00"));
		assertTrue(GitUtils.isTagDir(GitUtils.TAG_SLASH + "ff"));
		assertTrue(GitUtils.isTagDir(GitUtils.TAG_SLASH + "ae"));
		assertTrue(GitUtils.isTagDir(GitUtils.TAG_SLASH + "asdfasd_aldsjfasd asdlkjasdj.,.;_:;:öÖLP\"=)==\"§\"§%\"!§)$§\""));
	}

	@Test
	public void testIsRemoteDir() {
		assertFalse(GitUtils.isRemoteDir(""));
		assertFalse(GitUtils.isRemoteDir("/"));
		assertFalse(GitUtils.isRemoteDir("/something"));
		assertFalse(GitUtils.isRemoteDir("/branch"));
		assertFalse(GitUtils.isRemoteDir("/commit"));
		assertFalse(GitUtils.isRemoteDir("/tag"));
		assertFalse(GitUtils.isRemoteDir(GitUtils.BRANCH_SLASH + "ae/.hidden"));

		assertTrue(GitUtils.isRemoteDir(GitUtils.REMOTE_SLASH + "00/"));
		assertTrue(GitUtils.isRemoteDir(GitUtils.REMOTE_SLASH + "asdfasd/sjwekw"));
		assertTrue(GitUtils.isRemoteDir(GitUtils.REMOTE_SLASH + "asdfasd_aldsjfasd asdlkjasdj.,.;_:;:öÖLP\"=)==\"§\"§%/\"!§)$§\""));

		assertTrue(GitUtils.isRemoteDir(GitUtils.REMOTE_SLASH + "asdfasd"));
		assertTrue(GitUtils.isRemoteDir(GitUtils.REMOTE_SLASH + "fg"));
		assertTrue(GitUtils.isRemoteDir(GitUtils.REMOTE_SLASH + "zz"));
		assertTrue(GitUtils.isRemoteDir(GitUtils.REMOTE_SLASH + "00"));
		assertTrue(GitUtils.isRemoteDir(GitUtils.REMOTE_SLASH + "ff"));
		assertTrue(GitUtils.isRemoteDir(GitUtils.REMOTE_SLASH + "ae"));
		assertTrue(GitUtils.isRemoteDir(GitUtils.REMOTE_SLASH + "asdfasd_aldsjfasd asdlkjasdj.,.;_:;:öÖLP\"=)==\"§\"§%\"!§)$§\""));
	}

	@Test
	public void testGetUID() throws IOException {
		assertTrue(GitUtils.getUID() >= 0);
	}

	@Test
	public void testGetGID() throws IOException {
		assertTrue(GitUtils.getGID() >= 0);
	}
}
