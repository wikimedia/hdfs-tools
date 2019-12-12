package org.wikimedia.analytics.hdfstools

import java.io.File
import java.net.URI
import java.nio.file.attribute.{PosixFileAttributes, PosixFilePermissions}
import java.nio.file.{Files, Paths}

import org.apache.log4j.Level
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class TestHdfsRsyncExec extends AnyFlatSpec with Matchers with BeforeAndAfterEach with TestHdfsRsyncHelper {

    override def beforeEach(): Unit = helperBefore()

    override def afterEach(): Unit = helperAfter()

    // Reused values
    val tmpDstBaseFile = new File(tmpDstBase)

    val originalConfig = new HdfsRsyncConfig()
    // To turn on console logging in tests, comment next line and uncomment the one after
    val baseConfig = originalConfig.copy(logAppender = Seq(testLogAppender))
    //val baseConfig = originalConfig.copy(logAppender = originalConfig.logAppender :+ testLogAppender)

    private def checkTmpDstEqualsTmpSrc(): Unit = {
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

    "HdfsRsyncExec" should "log files to be copied without dst" in {

        val config = baseConfig.copy(src = tmpSrc).initialize
        new HdfsRsyncExec(config).apply()

        val logEvents = testLogAppender.logEvents
        logEvents.size should equal(1)
        logEvents.forall(e => e.getLevel == Level.INFO) should equal(true)
        val messages = logEvents.map(_.getMessage)
        // Using  version in messages
        messages should contain(s"COPY_FILE [no-dst] - $tmpSrc")
    }

    it should "log files to be copied without dst recursively" in {
        val config = baseConfig.copy(
            src = tmpSrc,
            recurse = true,
            dryRun = true
        ).initialize
        new HdfsRsyncExec(config).apply()

        val logEvents = testLogAppender.logEvents
        logEvents.size should equal(4)
        logEvents.forall(e => e.getLevel == Level.INFO) should equal(true)
        val messages = logEvents.map(_.getMessage)
        // Using  version in messages
        messages should contain(s"CREATE_FOLDER [no-dst] - $tmpSrc")
        messages should contain(s"COPY_FILE [no-dst] - $tmpSrcFile1")
        messages should contain(s"CREATE_FOLDER [no-dst] - $tmpSrcFolder1")
        messages should contain(s"COPY_FILE [no-dst] - $tmpSrcFolder1File2")
    }

    it should "log files to be copied without dst recursively with trailing slash" in {
        val config = baseConfig.copy(
            src = new URI(s"$tmpSrc/"),
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
        messages should contain(s"CREATE_FOLDER [no-dst] - $tmpSrcFolder1")
        messages should contain(s"COPY_FILE [no-dst] - $tmpSrcFolder1File2")

    }

    it should "log actions in dryrun mode recursively" in {
        val config = baseConfig.copy(
            src = tmpSrc,
            dst = Some(tmpDstBase),
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
        messages should contain(s"CREATE_FOLDER [dryrun] - $tmpSrc --> $tmpDst")
        messages should contain(s"COPY_FILE [dryrun] - $tmpSrcFile1 --> $tmpDstFile1")
        messages should contain(s"CREATE_FOLDER [dryrun] - $tmpSrcFolder1 --> $tmpDstFolder1")
        messages should contain(s"COPY_FILE [dryrun] - $tmpSrcFolder1File2 --> $tmpDstFolder1File2")
    }

    it should "copy src to dst NOT recursively and not copy if file/folder match" in {
        val config = baseConfig.copy(
            src = tmpSrc,
            dst = Some(tmpDstBase)
        ).initialize
        new HdfsRsyncExec(config).apply()

        checkTmpDstEqualsTmpSrc()

        // Now remove file in folder_1, execute rsync again
        // File should still be gone because of no recursion
        new File(tmpDstFolder1File2).delete()
        new HdfsRsyncExec(config).apply()
        val innerList3 = new File(tmpDstFolder1).list()
        innerList3.size should equal(0)
    }

    it should "copy src to dst with trailing slash " in {
        val config = baseConfig.copy(
            src = new URI(s"$tmpSrc/"),
            dst = Some(tmpDstBase)
        ).initialize
        new HdfsRsyncExec(config).apply()

        val tmpContent = tmpDstBaseFile.list()
        tmpContent.size should equal(2)
        tmpContent should contain("file_1")
        tmpContent should contain("folder_1")
        val innerList1 = new File(new URI(s"$tmpDstBase/folder_1")).list()
        innerList1.size should equal(1)
        innerList1 should contain("file_2")
    }

    it should "copy src to dst recursively with size-only and not copy existing" in {
        val config = baseConfig.copy(
            src = tmpSrc,
            dst = Some(tmpDstBase),
            recurse = true,
            sizeOnly = true,
            logLevel = Level.DEBUG // Skipping messages are logged in debug mode
        ).initialize
        new HdfsRsyncExec(config).apply()

        checkTmpDstEqualsTmpSrc()

        // Now remove file in folder_1, execute rsync again - file should be back
        // and skipping log messages should be there for files not deleted (matching size only)
        new File(tmpDstFolder1File2).delete()

        new HdfsRsyncExec(config).apply()
        val innerList3 = new File(tmpDstFolder1).list()
        innerList3.size should equal(1)
        innerList3 should contain("file_2")

        val messages = testLogAppender.logEvents.map(_.getMessage.toString)
        val skippingMessages = messages.filter(_.startsWith("SKIP_"))
        skippingMessages.size should equal(3)
    }

    it should "copy src to dst updating modification timestamp" in {
        val config = baseConfig.copy(
            src = tmpSrc,
            dst = Some(tmpDstBase),
            preserveTimes = true
        ).initialize
        new HdfsRsyncExec(config).apply()

        val tmpContent = tmpDstBaseFile.list()
        tmpContent.size should equal(1)
        tmpContent.head should equal("test_folder")
        val f = new File(tmpDst)
        // Since we test on a single local filesystem, no precision problem
        f.lastModified() should equal(new File(tmpSrc).lastModified())
    }

    it should "copy src to dst recursively 2 times with ignore-times and preserve-times" in {
        val config = baseConfig.copy(
            src = tmpSrc,
            dst = Some(tmpDstBase),
            recurse = true,
            preserveTimes = true,
            ignoreTimes = true,
            logLevel = Level.DEBUG // Skipping messages are logged in debug mode
        ).initialize
        new HdfsRsyncExec(config).apply()

        // without ignore-times flag, this copy should lead to SKIPPING all
        // since size and time would be equal thanks to preserve-time
        new HdfsRsyncExec(config).apply()

        val messages = testLogAppender.logEvents.map(_.getMessage.toString)
        val skippingMessages = messages.filter(m => m.startsWith("SKIP_"))
        skippingMessages.size should equal(0)
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
            src = tmpSrc,
            dst = Some(tmpDstBase),
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
            src = tmpSrc,
            dst = Some(tmpDstBase),
            recurse = true,
            preservePerms = true,
            preserveTimes = true,
            logLevel = Level.DEBUG // Skipping messages are logged in debug mode
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
        val skippingMessages = messages.filter(m => m.startsWith("SKIP_") && m.contains("test_folder/file_1"))
        skippingMessages.size should equal(1)

        val resPath2 = Paths.get(tmpDstFile1)
        val resPerms2 = Files.readAttributes(resPath2, classOf[PosixFileAttributes]).permissions()
        PosixFilePermissions.toString(resPerms2) should equal(testPerms2)
    }

    it should "copy src to dst recursively with time updating perms with chmod (to new files only)" in {

        val config = baseConfig.copy(
            src = tmpSrc,
            dst = Some(tmpDstBase),
            recurse = true,
            preserveTimes = true,
            chmod = Seq("F600", "D750")
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

        val newConfig = config.copy(chmod = Seq("F640")).initialize
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
            src = tmpSrc,
            dst = Some(tmpDstBase),
            recurse = true,
            preserveTimes = true,
            preservePerms = true,
            chmod = Seq("F600", "D750")
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
        val newConfig = config.copy(chmod = Seq("F640", "D700")).initialize
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
}

