package org.wikimedia.analytics.hdfstools


class TestHdfsRsyncCLI extends TestHdfsRsyncHelper {

    "HdfsRsyncCLI" should "correctly parse URIs" in {
        val args = Array(tmpSrc.toString, tmpSrcFile1.toString, tmpDstFolder1.toString)
        HdfsRsyncCLI.argsParser.parse(args, HdfsRsyncConfig()) match {
            case Some(configNotInitialized) =>
                configNotInitialized.allURIs.size should equal(3)
                configNotInitialized.srcsList.size should equal(0)
                configNotInitialized.dst.isEmpty should equal(true)

                val config = configNotInitialized.initialize
                config.srcsList.size should equal(2)
                config.dst.isEmpty should equal(false)
                config.dst.get.toString should equal(tmpDstFolder1.toString)

            case _ => fail()
        }
    }

    it should "correctly load rules from file" in {
        val rulesFilePath = this.getClass.getResource("/rules.txt").getPath
        val args = Array(
            "--filter-from-file", rulesFilePath,
            "--exclude-from-file", rulesFilePath,
            "--include-from-file", rulesFilePath,
            tmpSrc.toString
        )
        HdfsRsyncCLI.argsParser.parse(args, HdfsRsyncConfig()) match {
            case Some(configNotInitialized) =>
                val filterRules = configNotInitialized.filterRules
                filterRules.size should equal (3 * 5)
                filterRules.count(r => r.startsWith("+ ")) should equal(5 + 3)
                filterRules.count(r => r.startsWith("- ")) should equal(5 + 2)
                filterRules.head should equal("+ this is")
                filterRules.last should equal("+ + spaced empty lines")

            case _ => fail()
        }
    }
}

