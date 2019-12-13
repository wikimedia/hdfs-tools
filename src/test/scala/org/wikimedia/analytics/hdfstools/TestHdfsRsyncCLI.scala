package org.wikimedia.analytics.hdfstools


class TestHdfsRsyncCLI extends TestHdfsRsyncHelper {

    "HdfsRsyncCLI" should "correctly parse URIs" in {
        val args = Array("file:/home", "file:/root", "file:/")
        HdfsRsyncCLI.argsParser.parse(args, HdfsRsyncConfig()) match {
            case Some(configNotInitialized) =>
                configNotInitialized.allURIs.size should equal(3)
                configNotInitialized.srcList.size should equal(0)
                configNotInitialized.dst.isEmpty should equal(true)

                val config = configNotInitialized.initialize
                config.srcList.size should equal(2)
                config.dst.isEmpty should equal(false)
                config.dst.get.toString should equal("file:/")

            case _ => fail()
        }
    }
}

