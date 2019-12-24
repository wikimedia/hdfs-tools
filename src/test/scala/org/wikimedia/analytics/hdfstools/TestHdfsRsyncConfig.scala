package org.wikimedia.analytics.hdfstools

import java.io.File
import java.net.URI


class TestHdfsRsyncConfig extends TestHdfsRsyncHelper {

    "HdfsRsyncConfig" should "validate src URI" in {
        // Invalid cases
        baseConfig.copy(allURIs = Seq(new URI("/no/scheme/uri"))).validateSrcsList should
            equal(Some("Error validating src list:\n\t\t/no/scheme/uri does not specify scheme"))
        baseConfig.copy(allURIs = Seq(new URI("file:."))).validateSrcsList should
            equal(Some("Error validating src list:\n\t\tfile:. is not absolute"))
        baseConfig.copy(allURIs = Seq(new URI("wrongscheme:///"))).validateSrcsList should
            equal(Some("Error validating src list:\n\t\tscheme must be either 'file' or 'hdfs'"))
        baseConfig.copy(allURIs = Seq(new URI("file:/this/is/a/non/existent/path"))).validateSrcsList should
            equal(Some("Error validating src list:\n\t\tfile:/this/is/a/non/existent/path does not exist"))

        // Valid cases
        baseConfig.copy(allURIs = Seq(tmpSrcFile1)).validateSrcsList should equal(None)
        baseConfig.copy(allURIs = Seq(tmpSrc)).validateSrcsList should equal(None)
        baseConfig.copy(allURIs = Seq(new URI(s"$tmpSrc/"))).validateSrcsList should equal(None)
        baseConfig.copy(allURIs = Seq(new URI(s"$tmpSrc/*"))).validateSrcsList should equal(None)
    }

    it should "validate dst URI" in {
        // Invalid case (folder doesn't exist)
        baseConfig.copy(allURIs = Seq(tmpSrcFile1, tmpSrcFile1)).validateDst should
            equal(Some(s"Error validating dst:\n\t\t$tmpSrcFile1 dst dir cannot be created"))

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
        baseConfig.copy(chmodCommands = Seq("789")).validateChmods should
            equal(Some("Error validating chmod commands:\n\t\tInvalid chmod patterns: 789"))
        baseConfig.copy(chmodCommands = Seq("2000", "-x")).validateChmods should
            equal(Some("Error validating chmod commands:\n\t\tInvalid chmod patterns: 2000"))
        baseConfig.copy(chmodCommands = Seq("ug-rwxtx", "--w")).validateChmods should
            equal(Some("Error validating chmod commands:\n\t\tInvalid chmod patterns: ug-rwxtx, --w"))
        baseConfig.copy(chmodCommands = Seq("ugoa+r", "Fug=rw", "G+r", "Do-x")).validateChmods should
            equal(Some("Error validating chmod commands:\n\t\tInvalid chmod patterns: ugoa+r, G+r"))

        // Some invalid patterns-conjunctions
        baseConfig.copy(chmodCommands = Seq("D755", "Du=rw")).validateChmods should
            equal(Some("Error validating chmod commands:\n\t\tCan't have both octal and symbolic chmod commands for dirs"))
        baseConfig.copy(chmodCommands = Seq("F755", "Fu+w")).validateChmods should
            equal(Some("Error validating chmod commands:\n\t\tCan't have both octal and symbolic chmod commands for files"))
        baseConfig.copy(chmodCommands = Seq("F755", "F660")).validateChmods should
            equal(Some("Error validating chmod commands:\n\t\tOnly one octal chmod command is allowed for files"))
        baseConfig.copy(chmodCommands = Seq("755", "660")).validateChmods should
            equal(Some("Error validating chmod commands:\n\t\tOnly one octal chmod command is allowed for files\n" +
                "\t\tOnly one octal chmod command is allowed for dirs"))
    }

    it should "validate chown commands" in {
        // Some valid cases
        baseConfig.copy(chown = Some("test_user:test_group")).validateChown should equal(None)
        baseConfig.copy(chown = Some("test_user_only")).validateChown should equal(None)
        baseConfig.copy(chown = Some(":test_group_only")).validateChown should equal(None)

        // Some invalid formats
        baseConfig.copy(chown = Some(":")).validateChown should
            equal(Some("Error validating chown:\n\t\tInvalid chown format: :"))
        baseConfig.copy(chown = Some("user:group:more")).validateChown should
            equal(Some("Error validating chown:\n\t\tInvalid chown format: user:group:more"))
        baseConfig.copy(chown = Some("wrong_*_user")).validateChown should
            equal(Some("Error validating chown:\n\t\tInvalid username: wrong_*_user"))
        baseConfig.copy(chown = Some(":wrong_*_group")).validateChown should
            equal(Some("Error validating chown:\n\t\tInvalid groupname: wrong_*_group"))
        baseConfig.copy(chown = Some("wrong_*_user:group_ok")).validateChown should
            equal(Some("Error validating chown:\n\t\tInvalid username: wrong_*_user"))
        baseConfig.copy(chown = Some("user_ok:wrong_*_group")).validateChown should
            equal(Some("Error validating chown:\n\t\tInvalid groupname: wrong_*_group"))
        baseConfig.copy(chown = Some("too_long_for_a_valid_linux_username")).validateChown should
            equal(Some("Error validating chown:\n\t\tInvalid username size (max 32): too_long_for_a_valid_linux_username"))
        baseConfig.copy(chown = Some("too_long_for_a_valid_linux_username:group")).validateChown should
            equal(Some("Error validating chown:\n\t\tInvalid username size (max 32): too_long_for_a_valid_linux_username"))
        baseConfig.copy(chown = Some(":too_long_for_a_valid_linux_groupname")).validateChown should
            equal(Some("Error validating chown:\n\t\tInvalid groupname size (max 32): too_long_for_a_valid_linux_groupname"))
        baseConfig.copy(chown = Some("user:too_long_for_a_valid_linux_groupname")).validateChown should
            equal(Some("Error validating chown:\n\t\tInvalid groupname size (max 32): too_long_for_a_valid_linux_groupname"))
        baseConfig.copy(chown = Some("too_long_for_a_valid_linux_username:too_long_for_a_valid_linux_groupname")).validateChown should
            equal(Some("Error validating chown:\n\t\tInvalid username size (max 32): too_long_for_a_valid_linux_username\n" +
                "\t\tInvalid groupname size (max 32): too_long_for_a_valid_linux_groupname"))
    }

    it should "validate mappings" in {
        // Some valid cases
        baseConfig.validateMapping(Seq("*:test"), "test") should equal(None)
        baseConfig.validateMapping(Seq("*_test:test"), "test") should equal(None)
        baseConfig.validateMapping(Seq("test:test"), "test") should equal(None)

        // Some invalid formats
        baseConfig.validateMapping(Seq(":test"), "test") should
            equal(Some("Error validating test:\n\t\tInvalid mapping format: :test"))
        baseConfig.validateMapping(Seq("test:"), "test") should
            equal(Some("Error validating test:\n\t\tInvalid mapping format: test:"))
        baseConfig.validateMapping(Seq("test?:test"), "test") should
            equal(Some("Error validating test:\n\t\tInvalid mapping pattern: test?"))
        baseConfig.validateMapping(Seq("test:test*"), "test") should
            equal(Some("Error validating test:\n\t\tInvalid mapping value: test*"))
        baseConfig.validateMapping(Seq("too_long_for_a_valid_linux_username:test"), "test") should
            equal(Some("Error validating test:\n\t\tInvalid mapping pattern size (max 32): too_long_for_a_valid_linux_username"))
        baseConfig.validateMapping(Seq("pattern:too_long_for_a_valid_linux_username"), "test") should
            equal(Some("Error validating test:\n\t\tInvalid mapping value size (max 32): too_long_for_a_valid_linux_username"))
    }

    it should "validate filter rules" in {
        // Some valid cases
        baseConfig.copy(filterRules = Seq("+ x")).validateFilterRules should equal(None)
        baseConfig.copy(filterRules = Seq("+ x", "- z")).validateFilterRules should equal(None)
        baseConfig.copy(filterRules = Seq("+/ x", "-! z", "+!/ t", "-/! q")).validateFilterRules should equal(None)
        baseConfig.copy(filterRules = Seq("+ /x", "- z/")).validateFilterRules should equal(None)

        // Some invalid case
        baseConfig.copy(filterRules = Seq("invalid")).validateFilterRules should
            equal(Some("Error validating filter rules:\n\t\tInvalid filter rule: invalid"))
        baseConfig.copy(filterRules = Seq("+- invalid")).validateFilterRules should
            equal(Some("Error validating filter rules:\n\t\tInvalid filter rule: +- invalid"))
        baseConfig.copy(filterRules = Seq("! invalid", "+  invalid")).validateFilterRules should
            equal(Some("Error validating filter rules:\n\t\tInvalid filter rule: ! invalid\n\t\tInvalid filter rule: +  invalid"))
        baseConfig.copy(filterRules = Seq("+ valid", "! invalid")).validateFilterRules should
            equal(Some("Error validating filter rules:\n\t\tInvalid filter rule: ! invalid"))
    }

    it should "validate boolean flags limitations" in {
        baseConfig.copy(sizeOnly = true, ignoreTimes = true).validateFlags should
            equal(Some("Error validating flags:\n\t\tskip-times and use size-only cannot be used simultaneously"))
        baseConfig.copy(deleteExcluded = true, deleteExtraneous = false).validateFlags should
            equal(Some("Error validating flags:\n\t\tdelete-excluded must be used in conjunction with delete"))
        baseConfig.copy(recurse = true, copyDirs = true).validateFlags should
            equal(Some("Error validating flags:\n\t\trecurse and dirs cannot be used simultaneously"))
    }
}

