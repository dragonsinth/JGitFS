Hi!  Welcome to JGitFS, a virtual read-only filesystem that reflects a git
repository. In fact, it reflects _every_ revision in the entire repository.

This JGitFS mount reflects the git repository at:

```
%s
```

A bit about each of the subfolders you see:

- `/branch` contains symlinks local branches of the repo, e.g. `git branch`
- `/tag` contains symlinks for each tag in the repo, e.g. `git tag`
- `/remote` contains symlinks for all remote branches, e.g. `git branch -r`

- `/commit` is where all those symlinks point to.  Although it looks empty,
it's really not, it contains EVERY commit in the entire git repo! But you
have to know the commit SHA for the commit you want to access, e.g.
`/commit/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/`.

- `/tree` is like `/commit` in that you can't list it's contents. But it
contains every tree and subtree in the git repo, you just have to know the
tree SHA for the tree you want to access, e.g.
`/tree/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb/`

How can you find out the SHA for a tree? There's a hidden file in every
mirrored directory named `.gittree` which contains the tree SHA of the
directory it's in.  So from anywhere, you can obtain a permanent path to the
tree like this:

```
$ cat .gittree
bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb

$ cd <path>/tree/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb/
```
