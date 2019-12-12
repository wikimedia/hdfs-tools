package org.wikimedia.analytics.hdfstools

import java.io.File
import java.net.URI

import org.apache.commons.io.FileUtils
import org.apache.log4j.AppenderSkeleton
import org.apache.log4j.spi.LoggingEvent
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

    val testLogAppender = new TestLogAppender()

    val originalConfig = new HdfsRsyncConfig()
    // To turn on console logging in tests, comment next line and uncomment the one after
    val baseConfig = originalConfig.copy(logAppender = Seq(testLogAppender))
    //val baseConfig = originalConfig.copy(logAppender = originalConfig.logAppender :+ testLogAppender)

    val userTmp = "/tmp/" + System.getProperty("user.name")

    // src paths
    val tmpSrcBase = new URI(s"file:$userTmp/test_hdfsrsync/src")
    val tmpSrc = new URI(s"$tmpSrcBase/test_folder")
    val tmpSrcFile1 = new URI(s"$tmpSrc/file_1")
    val tmpSrcFolder1 = new URI(s"$tmpSrc/folder_1")
    val tmpSrcFolder1File2 = new URI(s"$tmpSrcFolder1/file_2")

    //  dst paths
    val tmpDstBase = new URI(s"file:$userTmp/test_hdfsrsync/dst")
    val tmpDst = new URI(s"$tmpDstBase/test_folder")
    val tmpDstFile1 = new URI(s"$tmpDst/file_1")
    val tmpDstFolder1 = new URI(s"$tmpDst/folder_1")
    val tmpDstFolder1File2 = new URI(s"$tmpDstFolder1/file_2")

    /**
     *
     * Creates source and destination test folders:
     * - /tmp/${user.name}/test_hdfsrsync/src
     * - /tmp/${user.name}/test_hdfsrsync/dst
     *
     * Fill in the src test folder with the following hierarchy:
     * src
     * | file_1
     * | folder_1
     *   | file_2
     */
    def createTestFiles(): Unit = {
        new File(tmpDstBase).mkdirs()
        new File(tmpSrcBase).mkdirs()

        new File(tmpSrc).mkdir()
        new File(tmpSrcFile1).createNewFile()
        new File(tmpSrcFolder1).mkdir()
        new File(tmpSrcFolder1File2).createNewFile()
    }

    def deleteTestFiles(): Unit = {
        FileUtils.deleteDirectory(new File(tmpDstBase))
        FileUtils.deleteDirectory(new File(tmpSrcBase))
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
