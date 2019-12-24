package org.wikimedia.analytics.hdfstools

import java.io.File
import java.net.URI

import org.apache.commons.io.FileUtils
import org.apache.log4j.spi.LoggingEvent
import org.apache.log4j.{AppenderSkeleton, ConsoleAppender}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


trait TestHdfsRsyncHelper extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

    /**
     * Custom log appender allowing tests to get access to messages logged during execution
     */
    class TestLogAppender extends AppenderSkeleton {
        val logEvents: scala.collection.mutable.Buffer[LoggingEvent] = scala.collection.mutable.Buffer.empty[LoggingEvent]

        override def append(loggingEvent: LoggingEvent): Unit = logEvents += loggingEvent

        override def requiresLayout(): Boolean = false

        override def close(): Unit = {}

        def reset(): Unit = logEvents.clear()
    }

    val originalConfig = new HdfsRsyncConfig()
    val testLogAppender: TestLogAppender = new TestLogAppender()
    val baseConfig: HdfsRsyncConfig = originalConfig.copy(logAppender = Seq(testLogAppender))

    val userTmp = "/tmp/" + System.getProperty("user.name")
    val testBaseURI = new URI(s"file:$userTmp/test_hdfs_rsync")

    // src paths
    val tmpSrcBase = new URI(s"$testBaseURI/src")
    val tmpSrc = new URI(s"$tmpSrcBase/test_folder")
    val tmpSrcFile1 = new URI(s"$tmpSrc/file_1")
    val tmpSrcFolder1 = new URI(s"$tmpSrc/folder_1")
    val tmpSrcFolder1File2 = new URI(s"$tmpSrcFolder1/file_2")

    // src2 paths
    val tmpSrc2Base = new URI(s"$testBaseURI/src2")
    val tmpSrc2 = new URI(s"$tmpSrc2Base/test_folder")
    val tmpSrc2File3 = new URI(s"$tmpSrc2/file_3")
    val tmpSrc2Folder2 = new URI(s"$tmpSrc2/folder_2")
    val tmpSrc2Folder2File4 = new URI(s"$tmpSrc2Folder2/file_4")

    //  dst paths
    val tmpDstBase = new URI(s"$testBaseURI/dst")
    val tmpDst = new URI(s"$tmpDstBase/test_folder")
    val tmpDstFile1 = new URI(s"$tmpDst/file_1")
    val tmpDstFolder1 = new URI(s"$tmpDst/folder_1")
    val tmpDstFolder1File2 = new URI(s"$tmpDstFolder1/file_2")

    //  dst paths with src2
    val tmpDstFile3 = new URI(s"$tmpDst/file_3")
    val tmpDstFolder2 = new URI(s"$tmpDst/folder_2")
    val tmpDstFolder2File4 = new URI(s"$tmpDstFolder2/file_4")

    /**
     *
     * Creates source and destination test folders:
     * - /tmp/${user.name}/test_hdfsrsync/src
     * - /tmp/${user.name}/test_hdfsrsync/src2
     * - /tmp/${user.name}/test_hdfsrsync/dst
     *
     * Fill in the src test folder with the following hierarchy:
     * src
     * | file_1
     * | folder_1
     *   | file_2
     *
     * src2
     * | file_3
     * | folder_2
     *   | file_4
     */
    def createTestFiles(): Unit = {
        new File(tmpDstBase).mkdirs()
        new File(tmpSrcBase).mkdirs()
        new File(tmpSrc2Base).mkdirs()

        new File(tmpSrc).mkdir()
        new File(tmpSrcFile1).createNewFile()
        new File(tmpSrcFolder1).mkdir()
        new File(tmpSrcFolder1File2).createNewFile()

        new File(tmpSrc2).mkdir()
        new File(tmpSrc2File3).createNewFile()
        new File(tmpSrc2Folder2).mkdir()
        new File(tmpSrc2Folder2File4).createNewFile()
    }

    def deleteTestFiles(): Unit = {
        FileUtils.deleteDirectory(new File(testBaseURI))
    }

    override def beforeEach(): Unit = {
        // In case there are leftovers
        deleteTestFiles()
        testLogAppender.reset()

        createTestFiles()

    }
     override def afterEach(): Unit = {
         deleteTestFiles()
         testLogAppender.reset()
     }

}
