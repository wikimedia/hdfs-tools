// Copyright 2019 Wikimedia Foundation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wikimedia.analytics.hdfstools

import java.io.IOException
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.ChmodParser
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Appender, Level}


/**
 * Configuration class for Hdfs Rsync
 * Some fields are provided through argument-parsing (see [[HdfsRsyncCLI]]),
 * some others are initialized from the former in the initialize method.
 *
 * Validation functions use scopt compliant Either[String, Unit] return types.
 * This type means and error in case Left("Error message"), and a success if Right(()).
 */
case class HdfsRsyncConfig(
    // Arguments
    src: URI = new URI(""),
    dst: Option[URI] = None,
    // Options
    dryRun: Boolean = false,
    recurse: Boolean = false,
    preservePerms: Boolean = false,
    preserveTimes: Boolean = false,
    deleteExtraneous: Boolean = false,
    acceptedTimesDiffMs: Long = 1000,
    ignoreTimes: Boolean = false,
    sizeOnly: Boolean = false,
    filterRules: Seq[String] = Seq.empty[String],
    chmod: Seq[String] = Seq.empty[String],
    logLevel: Level = Level.INFO,
    // Extracted config
    srcFs: FileSystem = null,
    srcPath: Path = null,
    dstFs: FileSystem = null,
    dstPath: Path = null,
    chmodFiles: Option[ChmodParser] = None,
    chmodDirs: Option[ChmodParser] = None,
    // Set hadoop-conf
    hadoopConf: Configuration = new Configuration(),
    // Logging appender - usefull to manage logging in tests
    logAppender: Seq[Appender] = Seq.empty
) {

    // These patterns are modified copies of the ones defined in hadoop chmod parser
    // https://github.com/apache/hadoop/blob/trunk/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/permission/ChmodParser.java
    private val chmodOctalPattern = "^([FD]?)([01]?[0-7]{3})$".r
    private val chmodSymbolicPattern = "^([FD]?)([ugoa]{0,3}[+=-]{1}[rwxXt]{1,4})$".r

    // Group 1: Include/exclude
    // Group 2: Modifiers (! is NOT MATCHING, / means checked against absolute pathname)
    // Group 3: actual pattern
    private val filterRulesPattern = "^([+-])([!/]?) (.*)$"

    /**
     * Get either local or hadoop filesystem for the given URI
     * @param uri the uri to get the filesystem for
     * @return the local or hadoop filesystem
     */
    private def getFS(uri: URI): FileSystem = {
        uri.getScheme match {
            case "file" => FileSystem.getLocal(hadoopConf)
            case "hdfs" => FileSystem.get(uri, hadoopConf)
            case other => throw new IOException(s"scheme must be either 'file' or 'hdfs'")
        }
    }

    /**
     * Generic function to validate src and dst URIs.
     * Valid URIs define scheme, are absolute and are:
     *  - Glob patterns returning non-null value (src)
     *  - existing folders (dst)
     * @param uri the uri to validate
     * @param isSrc whether to check for src (when true) or dst (when false)
     * @return Left("Error messages") if validation fails, Right(()) otherwise.
     */
    private def validateURI(uri: URI, isSrc: Boolean): Either[String, Unit] = {
        val paramName = if (isSrc) "src" else "dst"
        try {
            if (uri.getScheme == null || uri.getScheme.isEmpty) Left(s"Error validating $paramName: $uri does not specify scheme")
            else if (uri.getPath == null || !uri.getPath.startsWith("/")) Left(s"Error validating $paramName: $uri is not absolute")
            else {
                val fs = getFS(uri)
                val path = new Path(uri)
                // Using globStatus to check for path existence/files to copy as src can be a glob
                val globResult = fs.globStatus(path)
                if (globResult != null)
                    if (isSrc || fs.isDirectory(path)) Right(())
                    else Left(s"Error validating $paramName: $uri is not a directory")
                else Left(s"Error validating $paramName: $uri does not exist")
            }
        } catch {
            case e: IOException => Left(s"Error validating $paramName: ${e.getMessage}")
        }
    }

    /**
     * Validate src - should be a valid URI for a glob pattern returning non-null result
     * Note: Globs returning null are patterns without special characters not matching any file.
     * Patterns with special characters not matching any file return an empty list.
     * @return Left("Error messages") if src validation fails, Right(()) otherwise.
     */
    def validateSrc: Either[String, Unit] = validateURI(src, isSrc = true)

    /**
     * Validate dst - should be a valid URI for an existing directory
     * @return Left("Error message") if dst validation fails, Right(()) otherwise.
     */
    def validateDst: Either[String, Unit] = {
        // As dst is an option we apply validation only if it is defined and succeed otherwise
        dst.map(dstVal => validateURI(dstVal, isSrc = false)).getOrElse(Right(()))
    }

    /**
     * Validate the list of chmod commands.
     *  - Each command should be a valid pattern: a regular hadoop-chmod octal or symbolic action,
     *    possibly prefixed with 'F' or 'D' (to be applied to files only, or directory only).
     *  - The command list should either contain a single octal command or multiple symbolic ones,
     *    for both files and directories (we validate so by counting them as we validate the commands).
     *
     * @return Left("Error messages") if chmods validation fails, Right(()) otherwise.
     */
    def validateChmods: Either[String, Unit] = {

        // this case class is used as an accumulator to count
        // octal and symbolic chmod commands for both files and dirs
        // as we validate the commands themselves
        case class ChmodValidationAcc(invalids: Seq[String] = Seq.empty,
            octalFileNum: Int = 0, octalDirNum: Int = 0,
            symbolicFileNum: Int = 0, symbolicDirNum: Int = 0
        )

        val validationAcc: ChmodValidationAcc = chmod.foldLeft(new ChmodValidationAcc())((acc, mod) => {
            mod match {
                case chmodOctalPattern(t, _*) =>
                    t match {
                        case "F" => acc.copy(octalFileNum = acc.octalFileNum + 1)
                        case "D" => acc.copy(octalDirNum = acc.octalDirNum + 1)
                        case _ => acc.copy(octalFileNum = acc.octalFileNum + 1,
                            octalDirNum = acc.octalDirNum + 1)
                    }
                case chmodSymbolicPattern(t, _*) =>
                    t match {
                        case "F" => acc.copy(symbolicFileNum = acc.symbolicFileNum + 1)
                        case "D" => acc.copy(symbolicDirNum = acc.symbolicDirNum + 1)
                        case _ => acc.copy(symbolicFileNum = acc.symbolicFileNum + 1,
                            symbolicDirNum = acc.symbolicDirNum + 1)
                    }
                case invalid => acc.copy(invalids = acc.invalids :+ mod)
            }
        })

        // Error message generation from numbers checks
        val wrongNumbersErrorMessages = {
            // Using list concatenation chain checks for conciseness
            (if (validationAcc.octalFileNum > 1) Seq("Only one octal chmod command is allowed for files") else Nil) ++
                (if (validationAcc.octalDirNum > 1) Seq("Only one octal chmod command is allowed for dirs") else Nil) ++
                (if (validationAcc.octalFileNum == 1 && validationAcc.symbolicFileNum > 0) Seq("Can't have both octal and symbolic chmod commands for files") else Nil) ++
                (if (validationAcc.octalDirNum == 1 && validationAcc.symbolicDirNum > 0) Seq("Can't have both octal and symbolic chmod commands for dirs") else Nil)
        }
        val invalids = validationAcc.invalids

        if (invalids.isEmpty && wrongNumbersErrorMessages.isEmpty) Right(())
        else {
            val errorMessage = {
                "Incorrect chmod parameters:" +
                    (if (invalids.nonEmpty) s"\n\tInvalid chmod patterns: ${invalids.mkString(", ")}" else "") +
                    (if (wrongNumbersErrorMessages.nonEmpty) "\n" + wrongNumbersErrorMessages.map(s => s"\t$s").mkString("\n") else "")
            }
            Left(errorMessage)
        }
    }

    /**
     * Validate the list of filter rules.
     */
    def validateFilterRules: Either[String, Unit] = {
        // TODO
        Right(())
    }

    /**
     * Validate the whole config at once.
     * This allows for the scopt parser to be provided with all
     * error messages at once instead of failing at the first one.
     *
     * The error messages concatenation is done by folding validation functions
     * results, accumulating errors. The fold is initialized with Right(()) (success),
     * and at each stage checks for the function result. It returns an updated (error) status
     * if the function result is an error.
     *
     * @return Left("Error messages") if validation fails, Right(()) otherwise.
     */
    def validate: Either[String, Unit] = {
        Seq(
            validateSrc,
            validateDst,
            validateChmods,
            validateFilterRules
        ).fold(Right(()))((currentStatus, newResult) => {
            // newResult is a success, return unchanged currentStatus
            if (newResult.isRight) {
                currentStatus
            } else {
                if (currentStatus.isRight) {
                    // First error we encounter (currentStatus would be Left otherwise)
                    // Add error message header to newResult message
                    Left(s"Argument parsing error:\n\t${newResult.left.get}")
                } else {
                    // there is already existing errors and a new one - Concatenate them
                    Left(s"${currentStatus.left.get}\n\t${newResult.left.get}")
                }
            }
        })
    }

    // Rebuild a chmod string for either files (F) or dirs (D) from validated chmods.
    // This chmod will then be passed to hadoop-FS ChmodParser
    private def makeChmodString(chmods: Seq[String], files: Boolean): Option[String] = {
        val (sw, dr) = if (files) ("D", 'F') else ("F", 'D')
        chmods.foldLeft(None.asInstanceOf[Option[String]])((acc, mod) => {
            if (!mod.startsWith(sw))
                if (acc.isEmpty) Some(mod.dropWhile(c => c == dr))
                else Some(s"${acc.get},$mod")
            else acc
        })
    }

    /**
     * Function initializing inner-values of the config after parameters validation has been done
     * @return a new config with initialized values
     */
    def initialize: HdfsRsyncConfig = {
        this.copy(
            srcFs = getFS(src),
            // Add * to trailing slash to mimic rsync not copying src last folder but its content
            srcPath = if (src.toString.endsWith("/")) new Path(s"$src/*") else new Path(src),

            // Only initialize dstFS if dst is defined
            dstFs = dst.map(d => {
                val fs = getFS(d)
                fs.setWriteChecksum(false)
                fs
            }).orNull,
            dstPath = dst.map(d => new Path(d)).orNull,

            chmodFiles = makeChmodString(chmod, files = true).map(new ChmodParser(_)),
            chmodDirs = makeChmodString(chmod, files = false).map(new ChmodParser(_))
        )
    }
}
