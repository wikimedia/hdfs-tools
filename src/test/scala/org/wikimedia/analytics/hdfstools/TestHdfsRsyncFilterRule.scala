package org.wikimedia.analytics.hdfstools

import org.apache.hadoop.fs.{FileStatus, Path}


class TestHdfsRsyncFilterRule extends TestHdfsRsyncHelper {

    val config = baseConfig.copy(src = tmpSrc).initialize

    var statusFile1, statusFolder1, statusFolder1File2 = null.asInstanceOf[FileStatus]

    override def beforeEach(): Unit = {
        super.beforeEach()
        statusFile1 = config.srcFs.getFileStatus(new Path(tmpSrcFile1))
        statusFolder1 = config.srcFs.getFileStatus(new Path(tmpSrcFolder1))
        statusFolder1File2 = config.srcFs.getFileStatus(new Path(tmpSrcFolder1File2))
    }

    "HdfsRsyncFilterRule" should "match simple pattern" in {
        val filterRule = config.getParsedFilterRule("+ file_2")

        filterRule.matches(statusFile1, new Path(tmpSrc)) should equal(false)
        filterRule.matches(statusFolder1, new Path(tmpSrc)) should equal(false)
        filterRule.matches(statusFolder1File2, new Path(tmpSrc)) should equal(true)
    }

    "HdfsRsyncFilterRule" should "match simple pattern with NOT modifier" in {
        val filterRule = config.getParsedFilterRule("+! file_2")

        filterRule.matches(statusFile1, new Path(tmpSrc)) should equal(true)
        filterRule.matches(statusFolder1, new Path(tmpSrc)) should equal(true)
        filterRule.matches(statusFolder1File2, new Path(tmpSrc)) should equal(false)
    }

    "HdfsRsyncFilterRule" should "match * pattern" in {
        val filterRule = config.getParsedFilterRule("+ file*")

        filterRule.matches(statusFile1, new Path(tmpSrc)) should equal(true)
        filterRule.matches(statusFolder1, new Path(tmpSrc)) should equal(false)
        filterRule.matches(statusFolder1File2, new Path(tmpSrc)) should equal(true)
    }

    "HdfsRsyncFilterRule" should "match anchored star pattern" in {
        val filterRule = config.getParsedFilterRule("+ /file*")

        filterRule.matches(statusFile1, new Path(tmpSrc)) should equal(true)
        filterRule.matches(statusFolder1, new Path(tmpSrc)) should equal(false)
        filterRule.matches(statusFolder1File2, new Path(tmpSrc)) should equal(false)
    }

    "HdfsRsyncFilterRule" should "match anchored star pattern with full-path modifier" in {
        // this actually matches /file* at filesystem root :)
        val filterRule = config.getParsedFilterRule("+/ /file*")

        filterRule.matches(statusFile1, new Path(tmpSrc)) should equal(false)
        filterRule.matches(statusFolder1, new Path(tmpSrc)) should equal(false)
        filterRule.matches(statusFolder1File2, new Path(tmpSrc)) should equal(false)
    }

    "HdfsRsyncFilterRule" should "match double star pattern" in {
        val filterRule = config.getParsedFilterRule("+ **")

        filterRule.matches(statusFile1, new Path(tmpSrc)) should equal(true)
        filterRule.matches(statusFolder1, new Path(tmpSrc)) should equal(true)
        filterRule.matches(statusFolder1File2, new Path(tmpSrc)) should equal(true)
    }

    "HdfsRsyncFilterRule" should "match double star pattern in folder" in {
        val filterRule = config.getParsedFilterRule("+ folder_1/*")

        filterRule.matches(statusFile1, new Path(tmpSrc)) should equal(false)
        filterRule.matches(statusFolder1, new Path(tmpSrc)) should equal(false)
        filterRule.matches(statusFolder1File2, new Path(tmpSrc)) should equal(true)
    }

    "HdfsRsyncFilterRule" should "match question-mark pattern" in {
        val filterRule = config.getParsedFilterRule("+ file_?")

        filterRule.matches(statusFile1, new Path(tmpSrc)) should equal(true)
        filterRule.matches(statusFolder1, new Path(tmpSrc)) should equal(false)
        filterRule.matches(statusFolder1File2, new Path(tmpSrc)) should equal(true)
    }

    "HdfsRsyncFilterRule" should "match anchored question-mark pattern" in {
        val filterRule = config.getParsedFilterRule("+ /file_?")

        filterRule.matches(statusFile1, new Path(tmpSrc)) should equal(true)
        filterRule.matches(statusFolder1, new Path(tmpSrc)) should equal(false)
        filterRule.matches(statusFolder1File2, new Path(tmpSrc)) should equal(false)
    }

    "HdfsRsyncFilterRule" should "match complex pattern" in {
        val filterRule = config.getParsedFilterRule("+ f[io]l[de]*[!2]")

        filterRule.matches(statusFile1, new Path(tmpSrc)) should equal(true)
        filterRule.matches(statusFolder1, new Path(tmpSrc)) should equal(true)
        filterRule.matches(statusFolder1File2, new Path(tmpSrc)) should equal(false)
    }
}

