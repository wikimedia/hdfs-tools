package org.wikimedia.analytics.hdfstools

import java.io.{PrintWriter, File}
import java.net.URI
import java.nio.file.attribute._
// Non necessary imports kept for commented tests at the bottom
import java.nio.file.{LinkOption, FileSystems, Files, Paths}

import org.apache.commons.io.FileUtils
import org.apache.log4j.Level


class TestHdfsRsyncExec extends TestHdfsRsyncHelper {

    // Reused values
    val tmpDstBaseFile = new File(tmpDstBase)

    private def writeStringInFile(fileUri: URI, data: String): Unit = {
        val file1writer = new PrintWriter(new File(fileUri))
        file1writer.write(data)
        file1writer.close()
    }

    private def checkTmpDstContainsTmpSrc(): Unit = {
        val tmpContent = tmpDstBaseFile.list()
        tmpContent.size should equal(1)
        tmpContent.head should equal("test_folder")
        val innerList1 = new File(tmpDst).list()
        innerList1.size should equal(2)
        innerList1 should contain("file_1")
        innerList1 should contain("folder_1")
        val innerList2 = new File(tmpDstFolder1).list()
        innerList2.size should equal(1)
        innerList2 should contain("file_2")
    }

    private def checkTmpDstContainsTmpSrcAndSrc2(): Unit = {
        val tmpContent = tmpDstBaseFile.list()
        tmpContent.size should equal(1)
        tmpContent.head should equal("test_folder")
        val innerList1 = new File(tmpDst).list()
        innerList1.size should equal(4)
        innerList1 should contain("file_1")
        innerList1 should contain("folder_1")
        innerList1 should contain("file_3")
        innerList1 should contain("folder_2")
        val innerList2 = new File(tmpDstFolder1).list()
        innerList2.size should equal(1)
        innerList2 should contain("file_2")
        val innerList3 = new File(tmpDstFolder2).list()
        innerList3.size should equal(1)
        innerList3 should contain("file_4")
    }

    "HdfsRsyncExec" should "log files to be copied without dst recursively" in {
        val config = baseConfig.copy(
            allURIs = Seq(tmpSrc),
            recurse = true
        ).initialize
        new HdfsRsyncExec(config).apply()

        val logEvents = testLogAppender.logEvents
        logEvents.size should equal(4)
        logEvents.forall(e => e.getLevel == Level.INFO) should equal(true)
        val messages = logEvents.map(_.getMessage)
        // Using  version in messages
        messages should contain(s"CREATE_DIR [no-dst] - $tmpSrc")
        messages should contain(s"COPY_FILE [no-dst] - $tmpSrcFile1")
        messages should contain(s"CREATE_DIR [no-dst] - $tmpSrcFolder1")
        messages should contain(s"COPY_FILE [no-dst] - $tmpSrcFolder1File2")
    }

    it should "log files to be copied without dst recursively with trailing slash" in {
        val config = baseConfig.copy(
            allURIs = Seq(new URI(s"$tmpSrc/")),
            recurse = true,
            dryRun = true
        ).initialize
        new HdfsRsyncExec(config).apply()

        val logEvents = testLogAppender.logEvents
        logEvents.size should equal(3)
        logEvents.forall(e => e.getLevel == Level.INFO) should equal(true)
        val messages = logEvents.map(_.getMessage)
        // Using  version in messages
        messages should contain(s"COPY_FILE [no-dst] - $tmpSrcFile1")
        messages should contain(s"CREATE_DIR [no-dst] - $tmpSrcFolder1")
        messages should contain(s"COPY_FILE [no-dst] - $tmpSrcFolder1File2")

    }

    it should "log actions in dryrun mode recursively" in {
        val config = baseConfig.copy(
            allURIs = Seq(tmpSrc, tmpDstBase),
            recurse = true,
            dryRun = true
        ).initialize
        new HdfsRsyncExec(config).apply()

        // Check no copy has been made
        new File(tmpDstBase).list().size should equal(0)

        // Check logs
        val logEvents = testLogAppender.logEvents
        logEvents.size should equal(4)
        logEvents.forall(e => e.getLevel == Level.INFO) should equal(true)
        val messages = logEvents.map(_.getMessage)
        // Using  version in messages
        messages should contain(s"CREATE_DIR [dryrun] - $tmpDst")
        messages should contain(s"COPY_FILE [dryrun] - $tmpSrcFile1 --> $tmpDstFile1")
        messages should contain(s"CREATE_DIR [dryrun] - $tmpDstFolder1")
        messages should contain(s"COPY_FILE [dryrun] - $tmpSrcFolder1File2 --> $tmpDstFolder1File2")
    }

    it should "copy src to dst with trailing slash (files only)" in {
        val config = baseConfig.copy(
            allURIs = Seq(new URI(s"$tmpSrc/"), tmpDstBase)
        ).initialize
        new HdfsRsyncExec(config).apply()

        val tmpContent = tmpDstBaseFile.list()
        tmpContent.size should equal(1)
        tmpContent should contain("file_1")
    }

    it should "copy src to dst copying directories with size-only and not update second copy" in {
        val config = baseConfig.copy(
            allURIs = Seq(tmpSrc, tmpDstBase),
            copyDirs = true,
            sizeOnly = true
        ).initialize
        new HdfsRsyncExec(config).apply()

        checkTmpDstContainsTmpSrc()

        // Drop a file from dst
        new File(tmpDstFolder1File2).delete()
        new File(tmpDstFolder1File2).exists() should equal(false)

        // Copy Again - File should still be missing (cause folder not copied again)
        new HdfsRsyncExec(config).apply()
        new File(tmpDstFolder1File2).exists() should equal(false)

    }

    it should "copy src to dst recursively with size-only and not copy existing" in {
        val config = baseConfig.copy(
            allURIs = Seq(tmpSrc, tmpDstBase),
            recurse = true,
            sizeOnly = true,
            applicationLogLevel = "DEBUG" // Skipping messages are logged in debug mode
        ).initialize
        new HdfsRsyncExec(config).apply()

        checkTmpDstContainsTmpSrc()

        // Now remove file in folder_1, execute rsync again - file should be back
        // and skipping log messages should be there for files not deleted (matching size only)
        new File(tmpDstFolder1File2).delete()
        new File(tmpDstFolder1File2).exists() should equal(false)

        new HdfsRsyncExec(config).apply()
        val innerList3 = new File(tmpDstFolder1).list()
        innerList3.size should equal(1)
        innerList3 should contain("file_2")

        val messages = testLogAppender.logEvents.map(_.getMessage.toString)
        messages.count(_.startsWith("SAME_FILE")) should equal(1)
    }

    it should "copy src to dst recursively and prune empty dirs in dry-run mode" in {
        // Delete file in folder_1 for it to be pruned
        new File(tmpSrcFolder1File2).delete()
        new File(tmpSrcFolder1File2).exists() should equal(false)

        val config = baseConfig.copy(
            allURIs = Seq(tmpSrc, tmpDstBase),
            dryRun = true,
            recurse = true,
            pruneEmptyDirs = true,
            applicationLogLevel = "DEBUG" // Skipping messages are logged in debug mode
        ).initialize
        new HdfsRsyncExec(config).apply()

        val messages = testLogAppender.logEvents.map(_.getMessage.toString)
        messages.count(_.startsWith("PRUNE_DIR")) should equal(2)
    }

    it should "copy src to dst recursively with chmod and prune empty dirs" in {
        // Delete file in folder_1 for it to be pruned
        new File(tmpSrcFolder1File2).delete()
        new File(tmpSrcFolder1File2).exists() should equal(false)

        val config = baseConfig.copy(
            allURIs = Seq(tmpSrc, tmpDstBase),
            recurse = true,
            chmodCommands = Seq("D700"),
            pruneEmptyDirs = true,
            applicationLogLevel = "DEBUG" // Skipping messages are logged in debug mode
        ).initialize
        new HdfsRsyncExec(config).apply()

        val innerList = new File(tmpDst).list()
        innerList.size should equal(1)
        innerList should contain("file_1")

        val messages = testLogAppender.logEvents.map(_.getMessage.toString)
        messages.count(_.startsWith("PRUNE_DIR")) should equal(1)
    }

    it should "copy src to dst updating modification timestamp" in {
        val config = baseConfig.copy(
            allURIs = Seq(tmpSrc, tmpDstBase),
            preserveTimes = true,
            recurse = true
        ).initialize
        new HdfsRsyncExec(config).apply()

        val tmpContent = tmpDstBaseFile.list()
        tmpContent.size should equal(1)
        tmpContent.head should equal("test_folder")
        val f = new File(tmpDst)
        // Since we test on a single local filesystem, no precision problem
        f.lastModified() should equal(new File(tmpSrc).lastModified())
    }

    it should "copy src to dst recursively with times and update existing only on second copy" in {
        val config = baseConfig.copy(
            allURIs = Seq(tmpSrc, tmpDstBase),
            recurse = true,
            preserveTimes = true
        ).initialize
        new HdfsRsyncExec(config).apply()

        checkTmpDstContainsTmpSrc()

        // Force full-deletion and creation of sources for dates updates
        FileUtils.deleteDirectory(new File(tmpSrcBase))
        createTestFiles()
        // Delete existing dst to check not updated
        new File(tmpDstFile1).delete()

        // Check dates don't match
        new File(tmpDstFolder1).lastModified should be <= new File(tmpSrcFolder1).lastModified

        new HdfsRsyncExec(config.copy(existing = true).initialize).apply()

        // Not existing file not copied
        new File(tmpDstFile1).exists() should equal(false)
        // Existing file (folder) updated
        new File(tmpDstFolder1).lastModified should equal(new File(tmpSrcFolder1).lastModified)
        new File(tmpDstFolder1File2).lastModified should equal(new File(tmpSrcFolder1File2).lastModified)
    }

    it should "copy src to dst recursively 2 times with ignore-times and preserve-times" in {
        val config = baseConfig.copy(
            allURIs = Seq(tmpSrc,tmpDstBase),
            recurse = true,
            preserveTimes = true,
            ignoreTimes = true,
            applicationLogLevel = "DEBUG" // Skipping messages are logged in debug mode
        ).initialize
        new HdfsRsyncExec(config).apply()

        // withot ignore-times flag, this copy should lead to SKIPPING all
        // since size and time would be equal thanks to preserve-time
        new HdfsRsyncExec(config).apply()

        val messages = testLogAppender.logEvents.map(_.getMessage.toString)
        val skippingMessages = messages.filter(m => m.startsWith("SKIP_FILE"))
        skippingMessages.size should equal(0)
    }


    it should "copy src to dst recursively with times and ignore existing on second copy" in {
        val config = baseConfig.copy(
            allURIs = Seq(tmpSrc, tmpDstBase),
            recurse = true,
            preserveTimes = true
        ).initialize
        new HdfsRsyncExec(config).apply()

        checkTmpDstContainsTmpSrc()

        // Force full-deletion and creation of sources for dates updates
        FileUtils.deleteDirectory(new File(tmpSrcBase))
        createTestFiles()
        // Delete existing dst to check updated
        new File(tmpDstFile1).delete()

        // Check dates don't match
        new File(tmpDstFolder1).lastModified should be <= new File(tmpSrcFolder1).lastModified

        new HdfsRsyncExec(config.copy(ignoreExisting = true).initialize).apply()

        // Not existing file copied
        new File(tmpDstFile1).exists() should equal(true)
        new File(tmpDstFile1).lastModified should equal(new File(tmpSrcFile1).lastModified)
        // Existing file (folder) should not be updated
        new File(tmpDstFolder1).lastModified should be <= new File(tmpSrcFolder1).lastModified
        new File(tmpDstFolder1File2).lastModified should be <= new File(tmpSrcFolder1File2).lastModified
    }

    it should "copy src to dst recursively with times and update only (ignore newer-dst) on second copy" in {
        val config = baseConfig.copy(
            allURIs = Seq(tmpSrc, tmpDstBase),
            recurse = true,
            preserveTimes = true
        ).initialize
        new HdfsRsyncExec(config).apply()

        checkTmpDstContainsTmpSrc()

        // Force full-deletion and creation of sources for dates updates
        FileUtils.deleteDirectory(new File(tmpSrcBase))
        createTestFiles()
        // Update existing dst to be newer than src with delta bigger than accepted approx
        new File(tmpDstFile1).setLastModified(new File(tmpSrcFile1).lastModified() + config.acceptedTimesDiffMs + 1L)

        // Check dates don't match
        new File(tmpDstFolder1).lastModified should be <= new File(tmpSrcFolder1).lastModified

        new HdfsRsyncExec(config.copy(update = true).initialize).apply()

        // Newer on dst file not touched
        new File(tmpDstFile1).exists() should equal(true)
        new File(tmpDstFile1).lastModified should be >= new File(tmpSrcFile1).lastModified
        // Older existing file should be updated
        new File(tmpDstFolder1).lastModified should equal(new File(tmpSrcFolder1).lastModified)
        new File(tmpDstFolder1File2).lastModified should equal(new File(tmpSrcFolder1File2).lastModified)
    }

    it should "copy src to dst recursively with size-only and overwrite existing (directories and files)" in {
        val config = baseConfig.copy(
            allURIs = Seq(tmpSrc, tmpDstBase),
            recurse = true,
            sizeOnly = true
        ).initialize
        new HdfsRsyncExec(config).apply()

        checkTmpDstContainsTmpSrc()

        // Update dst folder1 to be a file and execute rsync again - folder should be back
        new File(tmpDstFolder1).delete()
        new File(tmpDstFolder1).createNewFile()

        new HdfsRsyncExec(config).apply()
        new File(tmpDstFolder1).exists() should equal(true)
        new File(tmpDstFolder1).isDirectory should equal(true)

        // Update dst file1 (adding data) and execute rsync again - empty file should be back
        writeStringInFile(tmpDstFile1, "test data")
        new File(tmpDstFile1).length() should be > 0L

        new HdfsRsyncExec(config).apply()
        new File(tmpDstFile1).exists() should equal(true)
        new File(tmpDstFile1).length() should equal(0)
    }

    it should "copy src to dst recursively and delete extraneous dst files" in {
        // Create extraneous file in dst
        val testFolder = new File(new URI(s"$tmpDstBase/folder_to_delete"))
        testFolder.mkdirs()
        testFolder.exists() should equal(true)
        val testFile = new File(new URI(s"$tmpDstBase/folder_to_delete/file_to_delete"))
        testFile.createNewFile()
        testFile.exists() should equal(true)

        val config = baseConfig.copy(
            allURIs = Seq(tmpSrc, tmpDstBase),
            recurse = true,
            deleteExtraneous = true
        ).initialize
        new HdfsRsyncExec(config).apply()

        // Verify extraneous files are gone
        testFolder.exists() should equal(false)
        testFile.exists() should equal(false)
    }

    it should "copy src to dst recursively preserving time and perms" in {

        // change src/file_1 permissions
        val testPerms1 = "rwxr-----"
        val testFilePath = Paths.get(new URI(s"$tmpSrc/file_1"))
        Files.setPosixFilePermissions(testFilePath, PosixFilePermissions.fromString(testPerms1))

        val config = baseConfig.copy(
            allURIs = Seq(tmpSrc, tmpDstBase),
            recurse = true,
            preservePerms = true,
            preserveTimes = true,
            applicationLogLevel = "DEBUG" // Skipping messages are logged in debug mode
        ).initialize
        new HdfsRsyncExec(config).apply()

        // Checking permissions have been preserved
        val resPath1 = Paths.get(tmpDstFile1)
        val resPerms1 = Files.readAttributes(resPath1, classOf[PosixFileAttributes]).permissions()
        PosixFilePermissions.toString(resPerms1) should equal(testPerms1)

        // Changing again permission of src/file_1
        val testPerms2 = "rw-rw-r--"
        Files.setPosixFilePermissions(testFilePath, PosixFilePermissions.fromString(testPerms2))

        // Second run, file already exists in dst, file should be skipped and permissions updated
        new HdfsRsyncExec(config).apply()

        val messages = testLogAppender.logEvents.map(_.getMessage.toString)
        val skippingMessages = messages.filter(m => m.startsWith("SAME_FILE") && m.contains("test_folder/file_1"))
        skippingMessages.size should equal(1)

        val resPath2 = Paths.get(tmpDstFile1)
        val resPerms2 = Files.readAttributes(resPath2, classOf[PosixFileAttributes]).permissions()
        PosixFilePermissions.toString(resPerms2) should equal(testPerms2)
    }

    it should "copy src to dst recursively with time updating perms with chmod (to new files only)" in {

        val config = baseConfig.copy(
            allURIs = Seq(tmpSrc, tmpDstBase),
            recurse = true,
            preserveTimes = true,
            chmodCommands = Seq("F600", "D750")
        ).initialize
        new HdfsRsyncExec(config).apply()

        // Checking chmod has been applied to dst
        val resPathF1 = Paths.get(tmpDstFolder1File2)
        val resPermsF1 = Files.readAttributes(resPathF1, classOf[PosixFileAttributes]).permissions()
        PosixFilePermissions.toString(resPermsF1) should equal("rw-------")
        val resPathD1 = Paths.get(tmpDstFolder1)
        val resPermsD1 = Files.readAttributes(resPathD1, classOf[PosixFileAttributes]).permissions()
        PosixFilePermissions.toString(resPermsD1) should equal("rwxr-x---")

        // Drop copied file to copy it again with new chmod perms
        new File(tmpDstFolder1File2).delete()

        val newConfig = config.copy(allURIs = Seq(tmpSrc, tmpDstBase), chmodCommands = Seq("F640")).initialize
        new HdfsRsyncExec(newConfig).apply()

        // Checking chmod has been applied to new file only
        val resPathF2 = Paths.get(tmpDstFolder1File2)
        val resPermsF2 = Files.readAttributes(resPathF2, classOf[PosixFileAttributes]).permissions()
        PosixFilePermissions.toString(resPermsF2) should equal("rw-r-----")
        val resPathD2 = Paths.get(tmpDstFolder1)
        val resPermsD2 = Files.readAttributes(resPathD2, classOf[PosixFileAttributes]).permissions()
        PosixFilePermissions.toString(resPermsD2) should equal("rwxr-x---")
        // Not copied file should have kept its perms
        val resPathFF2 = Paths.get(tmpDstFile1)
        val resPermsFF2 = Files.readAttributes(resPathFF2, classOf[PosixFileAttributes]).permissions()
        PosixFilePermissions.toString(resPermsFF2) should equal("rw-------")
    }

    it should "copy src to dst recursively with time updating perms with chmod (to old files as well)" in {

        val config = baseConfig.copy(
            allURIs = Seq(tmpSrc, tmpDstBase),
            recurse = true,
            preserveTimes = true,
            preservePerms = true,
            chmodCommands = Seq("F600", "D750")
        ).initialize
        new HdfsRsyncExec(config).apply()

        // Checking chmod has been applied to dst
        val resPathF1 = Paths.get(tmpDstFolder1File2)
        val resPermsF1 = Files.readAttributes(resPathF1, classOf[PosixFileAttributes]).permissions()
        PosixFilePermissions.toString(resPermsF1) should equal("rw-------")
        val resPathD1 = Paths.get(tmpDstFolder1)
        val resPermsD1 = Files.readAttributes(resPathD1, classOf[PosixFileAttributes]).permissions()
        PosixFilePermissions.toString(resPermsD1) should equal("rwxr-x---")

        // Drop copied file to copy it again
        new File(tmpDstFolder1File2).delete()

        // Checking chmod has been applied to all files including existing ones
        val newConfig = config.copy(allURIs = Seq(tmpSrc, tmpDstBase), chmodCommands = Seq("F640", "D700")).initialize
        new HdfsRsyncExec(newConfig).apply()

        // Not checking simple copy, going aight for file permissions
        val resPathF2 = Paths.get(tmpDstFolder1File2)
        val resPermsF2 = Files.readAttributes(resPathF2, classOf[PosixFileAttributes]).permissions()
        PosixFilePermissions.toString(resPermsF2) should equal("rw-r-----")
        val resPathD2 = Paths.get(tmpDstFolder1)
        val resPermsD2 = Files.readAttributes(resPathD2, classOf[PosixFileAttributes]).permissions()
        PosixFilePermissions.toString(resPermsD2) should equal("rwx------")
        // Not copied file should have kept its perms
        val resPathFF2 = Paths.get(tmpDstFile1)
        val resPermsFF2 = Files.readAttributes(resPathFF2, classOf[PosixFileAttributes]).permissions()
        PosixFilePermissions.toString(resPermsFF2) should equal("rw-r-----")
    }

    it should "copy src to dst recursively and not delete extraneous dst file excluded" in {
        // Create extraneous file in dst
        val testFolder = new File(new URI(s"$tmpDstBase/folder_to_delete"))
        testFolder.mkdirs()
        testFolder.exists() should equal(true)
        val testFile = new File(new URI(s"$tmpDstBase/folder_to_delete/file_to_delete"))
        testFile.createNewFile()
        testFile.exists() should equal(true)

        val config = baseConfig.copy(
            allURIs = Seq(tmpSrc, tmpDstBase),
            recurse = true,
            deleteExtraneous = true,
            filterRules = Seq("- folder_to_delete"),
            applicationLogLevel = "DEBUG"
        ).initialize
        new HdfsRsyncExec(config).apply()

        // Verify extraneous files are gone
        testFolder.exists() should equal(true)
        testFile.exists() should equal(true)

        val messages = testLogAppender.logEvents.map(_.getMessage.toString)
        messages.count(_.startsWith("EXCLUDE_")) should equal(1)
    }

    it should "copy src to dst recursively except excluded" in {
        val config = baseConfig.copy(
            allURIs = Seq(tmpSrc, tmpDstBase),
            recurse = true,
            filterRules = Seq("- file*"),
            applicationLogLevel = "DEBUG" // Skipping messages are logged in debug mode
        ).initialize
        new HdfsRsyncExec(config).apply()

        val innerList1 = new File(tmpDst).list()
        innerList1.size should equal(1)
        innerList1 should contain("folder_1")

        val innerList2 = new File(tmpDstFolder1).list()
        innerList2.size should equal(0)

        val messages1 = testLogAppender.logEvents.map(_.getMessage.toString)
        messages1.count(_.startsWith("EXCLUDE_")) should equal(2)

        val config2 = config.copy(allURIs = Seq(tmpSrc, tmpDstBase), filterRules = Seq("- file_1")).initialize
        testLogAppender.reset()
        new HdfsRsyncExec(config2).apply()

        val innerList3 = new File(tmpDst).list()
        innerList3.size should equal(1)
        innerList3 should contain("folder_1")

        val innerList4 = new File(tmpDstFolder1).list()
        innerList4.size should equal(1)
        innerList4 should contain("file_2")

        val messages = testLogAppender.logEvents.map(_.getMessage.toString)
        messages.count(_.startsWith("EXCLUDE_")) should equal(1)
    }

    it should "copy src and src2 to dst recursively with size-only and not copy existing" in {
        val config = baseConfig.copy(
            allURIs = Seq(tmpSrc, tmpSrc2, tmpDstBase),
            recurse = true,
            sizeOnly = true,
            applicationLogLevel = "DEBUG" // Skipping messages are logged in debug mode
        ).initialize
        new HdfsRsyncExec(config).apply()

        checkTmpDstContainsTmpSrcAndSrc2()

        // Now remove file in folder_1 and folder_2, execute rsync again - file should be back
        // and skipping log messages should be there for files not deleted (matching size only)
        new File(tmpDstFolder1File2).delete()
        new File(tmpDstFolder2File4).delete()

        new File(tmpDstFolder1File2).exists() should equal(false)
        new File(tmpDstFolder2File4).exists() should equal(false)

        new HdfsRsyncExec(config).apply()
        val innerList1 = new File(tmpDstFolder1).list()
        innerList1.size should equal(1)
        innerList1 should contain("file_2")
        val innerList2 = new File(tmpDstFolder2).list()
        innerList2.size should equal(1)
        innerList2 should contain("file_4")

        val messages = testLogAppender.logEvents.map(_.getMessage.toString)
        messages.count(_.startsWith("SAME_FILE")) should equal(2)
    }

    it should "fail to copy src and src2 in case of directory-conflict in dir mode" in {
        val config = baseConfig.copy(
            allURIs = Seq(tmpSrc, tmpSrc2, tmpDstBase),
            copyDirs = true
        ).initialize

        the [IllegalStateException] thrownBy new HdfsRsyncExec(config).apply() should
            have message "SRC_CONFLICT - Trying to copy multiple objects with the same filename at the same destination"
    }


    it should "fail to copy src and src2 in recursive mode in case of files-conflict" in {
        // Create conflicting file in src2
        val conflictingFileURI = new URI(s"$tmpSrc2/file_1")
        new File(conflictingFileURI).createNewFile()
        new File(conflictingFileURI).exists should equal(true)
        new File(conflictingFileURI).isFile should equal(true)

        val config = baseConfig.copy(
            allURIs = Seq(tmpSrc, tmpSrc2, tmpDstBase),
            recurse = true
        ).initialize

        the [IllegalStateException] thrownBy new HdfsRsyncExec(config).apply() should
            have message "SRC_CONFLICT - Trying to copy multiple objects with the same filename at the same destination"
    }

    it should "fail to copy src and src2 in recursive mode in case of file-folder-conflict" in {
        // Create conflicting file in src2
        val conflictingFileURI = new URI(s"$tmpSrc2/file_1")
        new File(conflictingFileURI).mkdirs()
        new File(conflictingFileURI).exists should equal(true)
        new File(conflictingFileURI).isDirectory should equal(true)

        val config = baseConfig.copy(
            allURIs = Seq(tmpSrc, tmpSrc2, tmpDstBase),
            recurse = true
        ).initialize

        the [IllegalStateException] thrownBy new HdfsRsyncExec(config).apply() should
            have message "SRC_CONFLICT - Trying to copy multiple objects with the same filename at the same destination"
    }

    it should "copy src and src2 to dst recursively with times and use first-src time by default" in {
        // Force modif-time difference
        new File(tmpSrc2).setLastModified(new File(tmpSrc).lastModified() + 1000L)
        new File(tmpSrc).lastModified() should be < new File(tmpSrc2).lastModified()

        val config = baseConfig.copy(
            allURIs = Seq(tmpSrc, tmpSrc2, tmpDstBase),
            recurse = true,
            preserveTimes = true
        ).initialize
        new HdfsRsyncExec(config).apply()

        checkTmpDstContainsTmpSrcAndSrc2()

        new File(tmpDst).lastModified() should equal(new File(tmpSrc).lastModified())
    }

    it should "copy src to dst in dirs mode resolving conflict" in {
        // Force modif-time difference
        new File(tmpSrc2).setLastModified(new File(tmpSrc).lastModified() + 1000L)
        new File(tmpSrc).lastModified() should be < new File(tmpSrc2).lastModified()

        val config = baseConfig.copy(
            allURIs = Seq(tmpSrc, tmpSrc2, tmpDstBase),
            copyDirs = true,
            preserveTimes = true,
            resolveConflicts = true
        ).initialize
        new HdfsRsyncExec(config).apply()

        checkTmpDstContainsTmpSrc()

        new File(tmpDst).lastModified() should equal(new File(tmpSrc).lastModified())
    }

    it should "copy src2 to dst in dirs mode resolving conflict with most-recent-modif-time set" in {
        // Force modif-time difference
        new File(tmpSrc2).setLastModified(new File(tmpSrc).lastModified() + 1000L)
        new File(tmpSrc).lastModified() should be < new File(tmpSrc2).lastModified()

        val config = baseConfig.copy(
            allURIs = Seq(tmpSrc, tmpSrc2, tmpDstBase),
            copyDirs = true,
            preserveTimes = true,
            resolveConflicts = true,
            useMostRecentModifTimes = true
        ).initialize
        new HdfsRsyncExec(config).apply()

        val tmpContent = tmpDstBaseFile.list()
        tmpContent.size should equal(1)
        tmpContent.head should equal("test_folder")
        val innerList1 = new File(tmpDst).list()
        innerList1.size should equal(2)
        innerList1 should contain("file_3")
        innerList1 should contain("folder_2")
        val innerList = new File(tmpDstFolder2).list()
        innerList.size should equal(1)
        innerList should contain("file_4")

        new File(tmpDst).lastModified() should equal(new File(tmpSrc2).lastModified())
    }

    it should "copy src and src2 to dst recursively with times and use most-recent-modif-time when set" in {
        // Force modif-time difference
        new File(tmpSrc2).setLastModified(new File(tmpSrc).lastModified() + 1000L)
        new File(tmpSrc).lastModified() should be < new File(tmpSrc2).lastModified()

        val config = baseConfig.copy(
            allURIs = Seq(tmpSrc, tmpSrc2, tmpDstBase),
            recurse = true,
            preserveTimes = true,
            useMostRecentModifTimes = true
        ).initialize
        new HdfsRsyncExec(config).apply()

        checkTmpDstContainsTmpSrcAndSrc2()

        new File(tmpDst).lastModified() should equal(new File(tmpSrc2).lastModified())
    }

    it should "copy src and src2 recursively with times resolving conflict using first-listed src as default" in {
        // Create conflicting file in src2 with forced modif-time difference
        val conflictingFileURI = new URI(s"$tmpSrc2/file_1")
        new File(conflictingFileURI).createNewFile()
        new File(conflictingFileURI).setLastModified(new File(tmpSrcFile1).lastModified() + 1000L)
        new File(conflictingFileURI).exists should equal(true)
        new File(conflictingFileURI).isFile should equal(true)
        new File(conflictingFileURI).lastModified() should be > new File(tmpSrcFile1).lastModified()

        val config = baseConfig.copy(
            allURIs = Seq(tmpSrc, tmpSrc2, tmpDstBase),
            recurse = true,
            preserveTimes = true,
            resolveConflicts = true
        ).initialize
        new HdfsRsyncExec(config).apply()

        checkTmpDstContainsTmpSrcAndSrc2()

        new File(tmpDstFile1).lastModified() should equal(new File(tmpSrcFile1).lastModified())
    }

    it should "copy src and src2 recursively with times resolving conflict using most-recent-modif-time when set" in {
        // Create conflicting file in src2 with forced modif-time difference
        val conflictingFileURI = new URI(s"$tmpSrc2/file_1")
        new File(conflictingFileURI).createNewFile()
        new File(conflictingFileURI).setLastModified(new File(tmpSrcFile1).lastModified() + 1000L)
        new File(conflictingFileURI).exists should equal(true)
        new File(conflictingFileURI).isFile should equal(true)
        new File(conflictingFileURI).lastModified() should be > new File(tmpSrcFile1).lastModified()


        val config = baseConfig.copy(
            allURIs = Seq(tmpSrc, tmpSrc2, tmpDstBase),
            recurse = true,
            preserveTimes = true,
            resolveConflicts = true,
            useMostRecentModifTimes = true
        ).initialize
        new HdfsRsyncExec(config).apply()

        checkTmpDstContainsTmpSrcAndSrc2()

        new File(tmpDstFile1).lastModified() should equal(new File(conflictingFileURI).lastModified())
    }

    // Tests for usermap/groupmap and chown - Those are commented since they require special system
    // configuration and root access to work:
    //  * create user test_rsync with its own group:
    //     'sudo useradd test_rsync'
    //  * uncomment tests below
    //  * run maven from root using local repo to prevent downloading dependency jars anew:
    //      'sudo mvn -Dmaven.repo.local=/home/USERNAME/.m2/repository test'
    //  * After the tests, don't forget to delete the target folder as it will be unaccessible
    //    except from root
    //      'sudo rm -r target'

    /*
    it should "copy src to dst recursively changing applying src ownership (user)" in {
        val p = Paths.get(tmpSrcFile1)
        val lookupService = FileSystems.getDefault.getUserPrincipalLookupService
        val user = lookupService.lookupPrincipalByName("test_rsync")
        Files.setOwner(p, user)

        val config = baseConfig.copy(
            allURIs = Seq(tmpSrc, tmpDstBase),
            recurse = true,
            preserveOwner = true
        ).initialize
        new HdfsRsyncExec(config).apply()
        checkTmpDstContainsTmpSrc()

        Seq(tmpDst, tmpDstFolder1, tmpDstFolder1File2).foreach(f => {
            val attr = Files.readAttributes(Paths.get(f), classOf[PosixFileAttributes])
            attr.owner().getName should equal("root")
            attr.group().getName should equal("root")
        })

        val attr = Files.readAttributes(Paths.get(tmpDstFile1), classOf[PosixFileAttributes])
        attr.owner().getName should equal("test_rsync")
        attr.group().getName should equal("root")
    }

    it should "copy src to dst recursively changing applying src ownership (group)" in {
        val p = Paths.get(tmpSrcFile1)
        val lookupService = FileSystems.getDefault.getUserPrincipalLookupService
        val group = lookupService.lookupPrincipalByGroupName("test_rsync")
        Files.getFileAttributeView(p, classOf[PosixFileAttributeView], LinkOption.NOFOLLOW_LINKS).setGroup(group)

        val config = baseConfig.copy(
            allURIs = Seq(tmpSrc, tmpDstBase),
            recurse = true,
            preserveGroup = true
        ).initialize
        new HdfsRsyncExec(config).apply()
        checkTmpDstContainsTmpSrc()

        Seq(tmpDst, tmpDstFolder1, tmpDstFolder1File2).foreach(f => {
            val attr = Files.readAttributes(Paths.get(f), classOf[PosixFileAttributes])
            attr.owner().getName should equal("root")
            attr.group().getName should equal("root")
        })

        val attr = Files.readAttributes(Paths.get(tmpDstFile1), classOf[PosixFileAttributes])
        attr.owner().getName should equal("root")
        attr.group().getName should equal("test_rsync")
    }

    it should "copy src to dst recursively changing applying src ownership (both)" in {
        val p = Paths.get(tmpSrcFile1)
        val lookupService = FileSystems.getDefault.getUserPrincipalLookupService
        val user = lookupService.lookupPrincipalByName("test_rsync")
        val group = lookupService.lookupPrincipalByGroupName("test_rsync")
        Files.setOwner(p, user)
        Files.getFileAttributeView(p, classOf[PosixFileAttributeView], LinkOption.NOFOLLOW_LINKS).setGroup(group)

        val config = baseConfig.copy(
            allURIs = Seq(tmpSrc, tmpDstBase),
            recurse = true,
            preserveOwner = true,
            preserveGroup = true
        ).initialize
        new HdfsRsyncExec(config).apply()
        checkTmpDstContainsTmpSrc()

        Seq(tmpDst, tmpDstFolder1, tmpDstFolder1File2).foreach(f => {
            val attr = Files.readAttributes(Paths.get(f), classOf[PosixFileAttributes])
            attr.owner().getName should equal("root")
            attr.group().getName should equal("root")
        })

        val attr = Files.readAttributes(Paths.get(tmpDstFile1), classOf[PosixFileAttributes])
        attr.owner().getName should equal("test_rsync")
        attr.group().getName should equal("test_rsync")
    }

    it should "copy src to dst recursively changing applying src chowned ownership (user)" in {

        val config = baseConfig.copy(
            allURIs = Seq(tmpSrc, tmpDstBase),
            recurse = true,
            preserveOwner = true,
            chown = Some("test_rsync:test_rsync")
        ).initialize
        new HdfsRsyncExec(config).apply()
        checkTmpDstContainsTmpSrc()

        Seq(tmpDst, tmpDstFile1, tmpDstFolder1, tmpDstFolder1File2).foreach(f => {
            val attr = Files.readAttributes(Paths.get(f), classOf[PosixFileAttributes])
            attr.owner().getName should equal("test_rsync")
            attr.group().getName should equal("root")
        })
    }

    it should "copy src to dst recursively changing applying src chowned ownership (user 2)" in {

        val config = baseConfig.copy(
            allURIs = Seq(tmpSrc, tmpDstBase),
            recurse = true,
            preserveOwner = true,
            preserveGroup = true,
            chown = Some("test_rsync")
        ).initialize
        new HdfsRsyncExec(config).apply()
        checkTmpDstContainsTmpSrc()

        Seq(tmpDst, tmpDstFile1, tmpDstFolder1, tmpDstFolder1File2).foreach(f => {
            val attr = Files.readAttributes(Paths.get(f), classOf[PosixFileAttributes])
            attr.owner().getName should equal("test_rsync")
            attr.group().getName should equal("root")
        })
    }

    it should "copy src to dst recursively changing applying src chowned ownership (group)" in {

        val config = baseConfig.copy(
            allURIs = Seq(tmpSrc, tmpDstBase),
            recurse = true,
            preserveGroup = true,
            chown = Some("test_rsync:test_rsync")
        ).initialize
        new HdfsRsyncExec(config).apply()
        checkTmpDstContainsTmpSrc()

        Seq(tmpDst, tmpDstFile1, tmpDstFolder1, tmpDstFolder1File2).foreach(f => {
            val attr = Files.readAttributes(Paths.get(f), classOf[PosixFileAttributes])
            attr.owner().getName should equal("root")
            attr.group().getName should equal("test_rsync")
        })
    }

    it should "copy src to dst recursively changing applying src chowned ownership (group 2)" in {
        val config = baseConfig.copy(
            allURIs = Seq(tmpSrc, tmpDstBase),
            recurse = true,
            preserveOwner = true,
            preserveGroup = true,
            chown = Some(":test_rsync")
        ).initialize
        new HdfsRsyncExec(config).apply()
        checkTmpDstContainsTmpSrc()

        Seq(tmpDst, tmpDstFile1, tmpDstFolder1, tmpDstFolder1File2).foreach(f => {
            val attr = Files.readAttributes(Paths.get(f), classOf[PosixFileAttributes])
            attr.owner().getName should equal("root")
            attr.group().getName should equal("test_rsync")
        })
    }

    it should "copy src to dst recursively changing applying src chowned ownership (both)" in {

        val config = baseConfig.copy(
            allURIs = Seq(tmpSrc, tmpDstBase),
            recurse = true,
            preserveOwner = true,
            preserveGroup = true,
            chown = Some("test_rsync:test_rsync")
        ).initialize
        new HdfsRsyncExec(config).apply()
        checkTmpDstContainsTmpSrc()

        Seq(tmpDst, tmpDstFile1, tmpDstFolder1, tmpDstFolder1File2).foreach(f => {
            val attr = Files.readAttributes(Paths.get(f), classOf[PosixFileAttributes])
            attr.owner().getName should equal("test_rsync")
            attr.group().getName should equal("test_rsync")
        })
    }
    */

}

