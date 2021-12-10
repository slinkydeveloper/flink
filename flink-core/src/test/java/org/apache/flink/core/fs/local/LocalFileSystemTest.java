/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.core.fs.local;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.FileSystemKind;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.TestLogger;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * This class tests the functionality of the {@link LocalFileSystem} class in its components. In
 * particular, file/directory access, creation, deletion, read, write is tested.
 */
public class LocalFileSystemTest extends TestLogger {

    @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    /** This test checks the functionality of the {@link LocalFileSystem} class. */
    @Test
    public void testLocalFilesystem() throws Exception {
        final File tempdir = new File(temporaryFolder.getRoot(), UUID.randomUUID().toString());

        final File testfile1 = new File(tempdir, UUID.randomUUID().toString());
        final File testfile2 = new File(tempdir, UUID.randomUUID().toString());

        final Path pathtotestfile1 = new Path(testfile1.toURI().getPath());
        final Path pathtotestfile2 = new Path(testfile2.toURI().getPath());

        final LocalFileSystem lfs = new LocalFileSystem();

        final Path pathtotmpdir = new Path(tempdir.toURI().getPath());

        /*
         * check that lfs can see/create/delete/read directories
         */

        // check that dir is not existent yet
        assertThat(lfs.exists(pathtotmpdir)).isFalse();
        assertThat(tempdir.mkdirs()).isTrue();

        // check that local file system recognizes file..
        assertThat(lfs.exists(pathtotmpdir)).isTrue();
        final FileStatus localstatus1 = lfs.getFileStatus(pathtotmpdir);

        // check that lfs recognizes directory..
        assertThat(localstatus1.isDir()).isTrue();

        // get status for files in this (empty) directory..
        final FileStatus[] statusforfiles = lfs.listStatus(pathtotmpdir);

        // no files in there.. hence, must be zero
        assertThat(statusforfiles.length == 0).isTrue();

        // check that lfs can delete directory..
        lfs.delete(pathtotmpdir, true);

        // double check that directory is not existent anymore..
        assertThat(lfs.exists(pathtotmpdir)).isFalse();
        assertThat(tempdir.exists()).isFalse();

        // re-create directory..
        lfs.mkdirs(pathtotmpdir);

        // creation successful?
        assertThat(tempdir.exists()).isTrue();

        /*
         * check that lfs can create/read/write from/to files properly and read meta information..
         */

        // create files.. one ""natively"", one using lfs
        final FSDataOutputStream lfsoutput1 = lfs.create(pathtotestfile1, WriteMode.NO_OVERWRITE);
        assertThat(testfile2.createNewFile()).isTrue();

        // does lfs create files? does lfs recognize created files?
        assertThat(testfile1.exists()).isTrue();
        assertThat(lfs.exists(pathtotestfile2)).isTrue();

        // test that lfs can write to files properly
        final byte[] testbytes = {1, 2, 3, 4, 5};
        lfsoutput1.write(testbytes);
        lfsoutput1.close();

        assertThat(5L).isEqualTo(testfile1.length());

        byte[] testbytestest = new byte[5];
        try (FileInputStream fisfile1 = new FileInputStream(testfile1)) {
            assertThat(fisfile1.read(testbytestest)).isEqualTo(testbytestest.length);
        }

        assertThat(testbytestest).isEqualTo(testbytes);

        // does lfs see the correct file length?
        assertThat(testfile1.length()).isEqualTo(lfs.getFileStatus(pathtotestfile1).getLen());

        // as well, when we call the listStatus (that is intended for directories?)
        assertThat(testfile1.length()).isEqualTo(lfs.listStatus(pathtotestfile1)[0].getLen());

        // test that lfs can read files properly
        final FileOutputStream fosfile2 = new FileOutputStream(testfile2);
        fosfile2.write(testbytes);
        fosfile2.close();

        testbytestest = new byte[5];
        final FSDataInputStream lfsinput2 = lfs.open(pathtotestfile2);
        assertThat(5).isEqualTo(lfsinput2.read(testbytestest));
        lfsinput2.close();
        assertThat(Arrays.equals(testbytes, testbytestest)).isTrue();

        // does lfs see two files?
        assertThat(2).isEqualTo(lfs.listStatus(pathtotmpdir).length);

        // do we get exactly one blocklocation per file? no matter what start and len we provide
        assertThat(1)
                .isEqualTo(
                        lfs.getFileBlockLocations(lfs.getFileStatus(pathtotestfile1), 0, 0).length);

        /*
         * can lfs delete files / directories?
         */
        assertThat(lfs.delete(pathtotestfile1, false)).isTrue();

        // and can lfs also delete directories recursively?
        assertThat(lfs.delete(pathtotmpdir, true)).isTrue();

        assertThat(!tempdir.exists()).isTrue();
    }

    /**
     * Test that {@link FileUtils#deletePathIfEmpty(FileSystem, Path)} deletes the path if it is
     * empty. A path can only be empty if it is a directory which does not contain any
     * files/directories.
     */
    @Test
    public void testDeletePathIfEmpty() throws IOException {
        File file = temporaryFolder.newFile();
        File directory = temporaryFolder.newFolder();
        File directoryFile = new File(directory, UUID.randomUUID().toString());

        assertThat(directoryFile.createNewFile()).isTrue();

        Path filePath = new Path(file.toURI());
        Path directoryPath = new Path(directory.toURI());
        Path directoryFilePath = new Path(directoryFile.toURI());

        FileSystem fs = FileSystem.getLocalFileSystem();

        // verify that the files have been created
        assertThat(fs.exists(filePath)).isTrue();
        assertThat(fs.exists(directoryFilePath)).isTrue();

        // delete the single file
        assertThat(FileUtils.deletePathIfEmpty(fs, filePath)).isFalse();
        assertThat(fs.exists(filePath)).isTrue();

        // try to delete the non-empty directory
        assertThat(FileUtils.deletePathIfEmpty(fs, directoryPath)).isFalse();
        assertThat(fs.exists(directoryPath)).isTrue();

        // delete the file contained in the directory
        assertThat(fs.delete(directoryFilePath, false)).isTrue();

        // now the deletion should work
        assertThat(FileUtils.deletePathIfEmpty(fs, directoryPath)).isTrue();
        assertThat(fs.exists(directoryPath)).isFalse();
    }

    @Test
    public void testRenamePath() throws IOException {
        final File rootDirectory = temporaryFolder.newFolder();

        // create a file /root/src/B/test.csv
        final File srcDirectory = new File(new File(rootDirectory, "src"), "B");
        assertThat(srcDirectory.mkdirs()).isTrue();

        final File srcFile = new File(srcDirectory, "test.csv");
        assertThat(srcFile.createNewFile()).isTrue();

        // Move/rename B and its content to /root/dst/A
        final File destDirectory = new File(new File(rootDirectory, "dst"), "B");
        final File destFile = new File(destDirectory, "test.csv");

        final Path srcDirPath = new Path(srcDirectory.toURI());
        final Path srcFilePath = new Path(srcFile.toURI());
        final Path destDirPath = new Path(destDirectory.toURI());
        final Path destFilePath = new Path(destFile.toURI());

        FileSystem fs = FileSystem.getLocalFileSystem();

        // pre-conditions: /root/src/B exists but /root/dst/B does not
        assertThat(fs.exists(srcDirPath)).isTrue();
        assertThat(fs.exists(destDirPath)).isFalse();

        // do the move/rename: /root/src/B -> /root/dst/
        assertThat(fs.rename(srcDirPath, destDirPath)).isTrue();

        // post-conditions: /root/src/B doesn't exists, /root/dst/B/test.csv has been created
        assertThat(fs.exists(destFilePath)).isTrue();
        assertThat(fs.exists(srcDirPath)).isFalse();

        // re-create source file and test overwrite
        assertThat(srcDirectory.mkdirs()).isTrue();
        assertThat(srcFile.createNewFile()).isTrue();

        // overwrite the destination file
        assertThat(fs.rename(srcFilePath, destFilePath)).isTrue();

        // post-conditions: now only the src file has been moved
        assertThat(fs.exists(srcFilePath)).isFalse();
        assertThat(fs.exists(srcDirPath)).isTrue();
        assertThat(fs.exists(destFilePath)).isTrue();
    }

    @Test
    public void testRenameNonExistingFile() throws IOException {
        final FileSystem fs = FileSystem.getLocalFileSystem();

        final File srcFile = new File(temporaryFolder.newFolder(), "someFile.txt");
        final File destFile = new File(temporaryFolder.newFolder(), "target");

        final Path srcFilePath = new Path(srcFile.toURI());
        final Path destFilePath = new Path(destFile.toURI());

        // this cannot succeed because the source file does not exist
        assertThat(fs.rename(srcFilePath, destFilePath)).isFalse();
    }

    @Test
    public void testRenameFileWithNoAccess() throws IOException {
        final FileSystem fs = FileSystem.getLocalFileSystem();

        final File srcFile = temporaryFolder.newFile("someFile.txt");
        final File destFile = new File(temporaryFolder.newFolder(), "target");

        // we need to make the file non-modifiable so that the rename fails
        Assume.assumeTrue(srcFile.getParentFile().setWritable(false, false));
        Assume.assumeTrue(srcFile.setWritable(false, false));

        try {
            final Path srcFilePath = new Path(srcFile.toURI());
            final Path destFilePath = new Path(destFile.toURI());

            // this cannot succeed because the source folder has no permission to remove the file
            assertThat(fs.rename(srcFilePath, destFilePath)).isFalse();
        } finally {
            // make sure we give permission back to ensure proper cleanup

            //noinspection ResultOfMethodCallIgnored
            srcFile.getParentFile().setWritable(true, false);
            //noinspection ResultOfMethodCallIgnored
            srcFile.setWritable(true, false);
        }
    }

    @Test
    public void testRenameToNonEmptyTargetDir() throws IOException {
        final FileSystem fs = FileSystem.getLocalFileSystem();

        // a source folder with a file
        final File srcFolder = temporaryFolder.newFolder();
        final File srcFile = new File(srcFolder, "someFile.txt");
        assertThat(srcFile.createNewFile()).isTrue();

        // a non-empty destination folder
        final File dstFolder = temporaryFolder.newFolder();
        final File dstFile = new File(dstFolder, "target");
        assertThat(dstFile.createNewFile()).isTrue();

        // this cannot succeed because the destination folder is not empty
        assertThat(fs.rename(new Path(srcFolder.toURI()), new Path(dstFolder.toURI()))).isFalse();

        // retry after deleting the occupying target file
        assertThat(dstFile.delete()).isTrue();
        assertThat(fs.rename(new Path(srcFolder.toURI()), new Path(dstFolder.toURI()))).isTrue();
        assertThat(new File(dstFolder, srcFile.getName()).exists()).isTrue();
    }

    @Test
    public void testKind() {
        final FileSystem fs = FileSystem.getLocalFileSystem();
        assertThat(fs.getKind()).isEqualTo(FileSystemKind.FILE_SYSTEM);
    }

    @Test
    public void testConcurrentMkdirs() throws Exception {
        final FileSystem fs = FileSystem.getLocalFileSystem();
        final File root = temporaryFolder.getRoot();
        final int directoryDepth = 10;
        final int concurrentOperations = 10;

        final Collection<File> targetDirectories =
                createTargetDirectories(root, directoryDepth, concurrentOperations);

        final ExecutorService executor = Executors.newFixedThreadPool(concurrentOperations);
        final CyclicBarrier cyclicBarrier = new CyclicBarrier(concurrentOperations);

        try {
            final Collection<CompletableFuture<Void>> mkdirsFutures =
                    new ArrayList<>(concurrentOperations);
            for (File targetDirectory : targetDirectories) {
                final CompletableFuture<Void> mkdirsFuture =
                        CompletableFuture.runAsync(
                                () -> {
                                    try {
                                        cyclicBarrier.await();
                                        assertThat(fs.mkdirs(Path.fromLocalFile(targetDirectory)))
                                                .isEqualTo(true);
                                    } catch (Exception e) {
                                        throw new CompletionException(e);
                                    }
                                },
                                executor);

                mkdirsFutures.add(mkdirsFuture);
            }

            final CompletableFuture<Void> allFutures =
                    CompletableFuture.allOf(
                            mkdirsFutures.toArray(new CompletableFuture[concurrentOperations]));

            allFutures.get();
        } finally {
            final long timeout = 10000L;
            ExecutorUtils.gracefulShutdown(timeout, TimeUnit.MILLISECONDS, executor);
        }
    }

    /** This test verifies the issue https://issues.apache.org/jira/browse/FLINK-18612. */
    @Test
    public void testCreatingFileInCurrentDirectoryWithRelativePath() throws IOException {
        FileSystem fs = FileSystem.getLocalFileSystem();

        Path filePath = new Path("local_fs_test_" + RandomStringUtils.randomAlphanumeric(16));
        try (FSDataOutputStream outputStream = fs.create(filePath, WriteMode.OVERWRITE)) {
            // Do nothing.
        } finally {
            for (int i = 0; i < 10 && fs.exists(filePath); ++i) {
                fs.delete(filePath, true);
            }
        }
    }

    private Collection<File> createTargetDirectories(
            File root, int directoryDepth, int numberDirectories) {
        final StringBuilder stringBuilder = new StringBuilder();

        for (int i = 0; i < directoryDepth; i++) {
            stringBuilder.append('/').append(i);
        }

        final Collection<File> targetDirectories = new ArrayList<>(numberDirectories);

        for (int i = 0; i < numberDirectories; i++) {
            targetDirectories.add(new File(root, stringBuilder.toString() + '/' + i));
        }

        return targetDirectories;
    }
}
