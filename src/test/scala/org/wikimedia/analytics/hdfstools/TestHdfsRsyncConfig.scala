package org.wikimedia.analytics.hdfstools

import java.io.File
import java.net.URI


class TestHdfsRsyncConfig extends TestHdfsRsyncHelper {

    "HdfsRsyncConfig" should "validate src URI" in {
        // Invalid cases
        baseConfig.copy(allURIs = Seq(new URI("/no/scheme/uri"))).validateSrcsList should equal(Some("Error validating src list:\n\t/no/scheme/uri does not specify scheme"))
        baseConfig.copy(allURIs = Seq(new URI("file:."))).validateSrcsList should equal(Some("Error validating src list:\n\tfile:. is not absolute"))
        baseConfig.copy(allURIs = Seq(new URI("wrongscheme:///"))).validateSrcsList should equal(Some("Error validating src list:\n\tscheme must be either 'file' or 'hdfs'"))
        baseConfig.copy(allURIs = Seq(new URI("file:/this/is/a/non/existent/path"))).validateSrcsList should equal(Some("Error validating src list:\n\tfile:/this/is/a/non/existent/path does not exist"))

        // Valid cases
        baseConfig.copy(allURIs = Seq(tmpSrcFile1)).validateSrcsList should equal(None)
        baseConfig.copy(allURIs = Seq(tmpSrc)).validateSrcsList should equal(None)
        baseConfig.copy(allURIs = Seq(new URI(s"$tmpSrc/"))).validateSrcsList should equal(None)
        baseConfig.copy(allURIs = Seq(new URI(s"$tmpSrc/*"))).validateSrcsList should equal(None)
    }

    it should "validate dst URI" in {
        // Invalid case (folder doesn't exist)
        baseConfig.copy(allURIs = Seq(tmpSrcFile1, tmpSrcFile1)).validateDst should equal(Some(s"Error validating dst:\n\t$tmpSrcFile1 dst dir cannot be created"))
        // Valid case
        baseConfig.copy(allURIs = Seq(tmpSrcFile1, tmpSrc)).validateDst should equal(None)
        // Valid case with folder creation
        val uri = new URI(s"$tmpSrc/test_folder_creation")
        baseConfig.copy(allURIs = Seq(tmpSrcFile1, uri)).validateDst should equal(None)
        new File(uri).exists() should equal(true)
    }

    it should "validate chmod commands" in {
        // Some valid cases
        baseConfig.copy(chmodCommands = Seq("660")).validateChmods should equal(None)
        baseConfig.copy(chmodCommands = Seq("D1766", "F0600")).validateChmods should equal(None)
        baseConfig.copy(chmodCommands = Seq("D755", "Fu=rw")).validateChmods should equal(None)
        baseConfig.copy(chmodCommands = Seq("ug+x", "u+rwxt", "a+rX")).validateChmods should equal(None)

        // Some invalid patterns
        baseConfig.copy(chmodCommands = Seq("789")).validateChmods should equal(Some("Error validating chmod commands:\n\tInvalid chmod patterns: 789"))
        baseConfig.copy(chmodCommands = Seq("2000", "-x")).validateChmods should equal(Some("Error validating chmod commands:\n\tInvalid chmod patterns: 2000"))
        baseConfig.copy(chmodCommands = Seq("ug-rwxtx", "--w")).validateChmods should equal(Some("Error validating chmod commands:\n\tInvalid chmod patterns: ug-rwxtx, --w"))
        baseConfig.copy(chmodCommands = Seq("ugoa+r", "Fug=rw", "G+r", "Do-x")).validateChmods should equal(Some("Error validating chmod commands:\n\tInvalid chmod patterns: ugoa+r, G+r"))

        // Some invalid patterns-conjunctions
        baseConfig.copy(chmodCommands = Seq("D755", "Du=rw")).validateChmods should equal(Some("Error validating chmod commands:\n\tCan't have both octal and symbolic chmod commands for dirs"))
        baseConfig.copy(chmodCommands = Seq("F755", "Fu+w")).validateChmods should equal(Some("Error validating chmod commands:\n\tCan't have both octal and symbolic chmod commands for files"))
        baseConfig.copy(chmodCommands = Seq("F755", "F660")).validateChmods should equal(Some("Error validating chmod commands:\n\tOnly one octal chmod command is allowed for files"))
        baseConfig.copy(chmodCommands = Seq("755", "660")).validateChmods should equal(Some("Error validating chmod commands:\n\tOnly one octal chmod command is allowed for files\n\tOnly one octal chmod command is allowed for dirs"))
    }

    it should "validate filter rules" in {
        // Some valid cases
        baseConfig.copy(filterRules = Seq("+ x")).validateFilterRules should equal(None)
        baseConfig.copy(filterRules = Seq("+ x", "- z")).validateFilterRules should equal(None)
        baseConfig.copy(filterRules = Seq("+/ x", "-! z", "+!/ t", "-/! q")).validateFilterRules should equal(None)
        baseConfig.copy(filterRules = Seq("+ /x", "- z/")).validateFilterRules should equal(None)

        // Some invalid case
        baseConfig.copy(filterRules = Seq("invalid")).validateFilterRules should equal(Some("Error validating filter rules:\n\tInvalid filter rule: invalid"))
        baseConfig.copy(filterRules = Seq("+- invalid")).validateFilterRules should equal(Some("Error validating filter rules:\n\tInvalid filter rule: +- invalid"))
        baseConfig.copy(filterRules = Seq("! invalid", "+  invalid")).validateFilterRules should equal(Some("Error validating filter rules:\n\tInvalid filter rule: ! invalid\n\tInvalid filter rule: +  invalid"))
        baseConfig.copy(filterRules = Seq("+ valid", "! invalid")).validateFilterRules should equal(Some("Error validating filter rules:\n\tInvalid filter rule: ! invalid"))
    }

    it should "validate boolean flags limitations" in {
        baseConfig.copy(sizeOnly = true, ignoreTimes = true).validateFlags should equal(Some("Error validating flags:\n\tskip-times and use size-only can't be used simultaneously"))
        baseConfig.copy(deleteExcluded = true, deleteExtraneous = false).validateFlags should equal(Some("Error validating flags:\n\tdelete-excluded must be used in conjunction with delete"))
    }
}

