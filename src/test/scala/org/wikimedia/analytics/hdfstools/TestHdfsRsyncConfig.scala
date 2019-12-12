package org.wikimedia.analytics.hdfstools

import java.net.URI

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class TestHdfsRsyncConfig extends AnyFlatSpec with Matchers with BeforeAndAfterEach with TestHdfsRsyncHelper {

    override def beforeEach(): Unit = helperBefore()

    override def afterEach(): Unit = helperAfter()

    "HdfsRsyncConfig" should "validate src" in {
        val c = new HdfsRsyncConfig()

        // Invalid cases
        c.copy(src = new URI("/no/scheme/uri")).validateSrc should equal(Left("Error validating src: /no/scheme/uri does not specify scheme"))
        c.copy(src = new URI("file:.")).validateSrc should equal(Left("Error validating src: file:. is not absolute"))
        c.copy(src = new URI("wrongscheme:///")).validateSrc should equal(Left("Error validating src: scheme must be either 'file' or 'hdfs'"))
        c.copy(src = new URI("file:/this/is/a/non/existent/path")).validateSrc should equal(Left("Error validating src: file:/this/is/a/non/existent/path does not exist"))

        // Valid cases
        c.copy(src = tmpSrcFile1).validateSrc should equal(Right(()))
        c.copy(src = tmpSrc).validateSrc should equal(Right(()))
        c.copy(src = new URI(s"$tmpSrc/")).validateSrc should equal(Right(()))
        c.copy(src = new URI(s"$tmpSrc/*")).validateSrc should equal(Right(()))
    }

    it should "validate dst" in {
        val c = new HdfsRsyncConfig()

        // Invalid case (folder doesn't exist)
        c.copy(dst = Some(tmpSrcFile1)).validateDst should equal(Left(s"Error validating dst: $tmpSrcFile1 is not a directory"))
        // Valid case
        c.copy(dst = Some(tmpSrc)).validateDst should equal(Right(()))
    }

    it should "validate chmod" in {
        val c = new HdfsRsyncConfig()

        // Some valid cases
        c.copy(chmod = Seq("660")).validateChmods should equal(Right(()))
        c.copy(chmod = Seq("D1766", "F0600")).validateChmods should equal(Right(()))
        c.copy(chmod = Seq("D755", "Fu=rw")).validateChmods should equal(Right(()))
        c.copy(chmod = Seq("ug+x", "u+rwxt", "a+rX")).validateChmods should equal(Right(()))

        // Some invalid patterns
        c.copy(chmod = Seq("789")).validateChmods should equal(Left("Incorrect chmod parameters:\n\tInvalid chmod patterns: 789"))
        c.copy(chmod = Seq("2000", "-x")).validateChmods should equal(Left("Incorrect chmod parameters:\n\tInvalid chmod patterns: 2000"))
        c.copy(chmod = Seq("ug-rwxtx", "--w")).validateChmods should equal(Left("Incorrect chmod parameters:\n\tInvalid chmod patterns: ug-rwxtx, --w"))
        c.copy(chmod = Seq("ugoa+r", "Fug=rw", "G+r", "Do-x")).validateChmods should equal(Left("Incorrect chmod parameters:\n\tInvalid chmod patterns: ugoa+r, G+r"))

        // Some invalid patterns-conjunctions
        c.copy(chmod = Seq("D755", "Du=rw")).validateChmods should equal(Left("Incorrect chmod parameters:\n\tCan't have both octal and symbolic chmod commands for dirs"))
        c.copy(chmod = Seq("F755", "Fu+w")).validateChmods should equal(Left("Incorrect chmod parameters:\n\tCan't have both octal and symbolic chmod commands for files"))
        c.copy(chmod = Seq("F755", "F660")).validateChmods should equal(Left("Incorrect chmod parameters:\n\tOnly one octal chmod command is allowed for files"))
        c.copy(chmod = Seq("755", "660")).validateChmods should equal(Left("Incorrect chmod parameters:\n\tOnly one octal chmod command is allowed for files\n\tOnly one octal chmod command is allowed for dirs"))
    }

}

