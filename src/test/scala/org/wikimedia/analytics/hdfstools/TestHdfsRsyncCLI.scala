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
}

