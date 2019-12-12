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

import scala.util.matching.Regex


/**
 * Configuration class for Hdfs Rsync
 * Some fields are provided through argument-parsing (see [[HdfsRsyncCLI]]),
 * some others are initialized from the former in the initialize method.
 *
 * Validation functions use Option[String] return type. An empty returned option
 * means a successful validation, a non-empty one contains the error message.
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
    deleteExcluded: Boolean = false,
    acceptedTimesDiffMs: Long = 1000,
    ignoreTimes: Boolean = false,
    sizeOnly: Boolean = false,
    filterRules: Seq[String] = Seq.empty[String],
    chmodCommands: Seq[String] = Seq.empty[String],
    logLevel: Level = Level.INFO,

    // Internal (need to be initialized)
    srcFs: FileSystem = null,
    srcPath: Path = null,
    dstFs: FileSystem = null,
    dstPath: Path = null,
    chmodFiles: Option[ChmodParser] = None,
    chmodDirs: Option[ChmodParser] = None,
    parsedFilterRules: Seq[HdfsRsyncFilterRule] = Seq.empty[HdfsRsyncFilterRule],
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
    private val filterRulePattern = "^([+-])(!?/?|/!) ([^ ].*)$".r

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
     * function concatenating multiple error messages into a single one.
     * @param errorMessages the errorMessages to concatenate
     * @param errorMessageHeader the header to use for the errorMessages group
     * @return some concatenated error message if errorMessages is not empty, None otherwise
     */
    private def prepareErrorMessage(errorMessages: Seq[String], errorMessageHeader: String): Option[String] = {
        if (errorMessages.isEmpty) {
            None
        } else {
            Some(s"$errorMessageHeader\n${errorMessages.map(s => s"\t$s").mkString("\n")}")
        }
    }

    /********************************************************************************
     * Parameters validation functions
     */

    /**
     * Generic function to validate src and dst URIs.
     * Valid URIs define scheme, are absolute and are:
     *  - Glob patterns returning non-null value (src)
     *  - existing folders (dst)
     * @param uri the uri to validate
     * @param isSrc whether to check for src (when true) or dst (when false)
     * @return None if validation succeeds, Some(error-message) otherwise.
     */
    private def validateURI(uri: URI, isSrc: Boolean): Option[String] = {
        val paramName = if (isSrc) "src" else "dst"
        try {
            if (uri.getScheme == null || uri.getScheme.isEmpty) Some(s"Error validating $paramName: $uri does not specify scheme")
            else if (uri.getPath == null || !uri.getPath.startsWith("/")) Some(s"Error validating $paramName: $uri is not absolute")
            else {
                val fs = getFS(uri)
                val path = new Path(uri)
                // Using globStatus to check for path existence/files to copy as src can be a glob
                val globResult = fs.globStatus(path)
                if (globResult != null)
                    if (isSrc || fs.isDirectory(path)) None
                    else Some(s"Error validating $paramName: $uri is not a directory")
                else Some(s"Error validating $paramName: $uri does not exist")
            }
        } catch {
            case e: IOException => Some(s"Error validating $paramName: ${e.getMessage}")
        }
    }

    /**
     * Validate src - should be a valid URI for a glob pattern returning non-null result
     * Note: Globs returning null are patterns without special characters not matching any file.
     * Patterns with special characters not matching any file return an empty list.
     * @return None if validation succeeds, Some(error-message) otherwise.
     */
    def validateSrc: Option[String] = validateURI(src, isSrc = true)

    /**
     * Validate dst - should be a valid URI for an existing directory
     * @return None if validation succeeds, Some(error-message) otherwise.
     */
    def validateDst: Option[String] = {
        // As dst is an option we apply validation only if it is defined and succeed otherwise
        dst.map(dstVal => validateURI(dstVal, isSrc = false)).getOrElse(None)
    }

    /**
     * Validate the list of chmod commands.
     *  - Each command should be a valid pattern: a regular hadoop-chmod octal or symbolic action,
     *    possibly prefixed with 'F' or 'D' (to be applied to files only, or directory only).
     *  - The command list should either contain a single octal command or multiple symbolic ones,
     *    for both files and directories (we validate so by counting them as we validate the commands).
     *
     * Validation is made by folding chmod commands accumulating invalid patterns and numbers used
     * for global validation in the dedicated ChmodValidationAcc class.
     * After accumulation, number-check-errors and invalid patterns error are gathered and prepared
     * as a single message using [[prepareErrorMessage()]].
     *
     * @return None if validation succeeds, Some(error-message) otherwise.
     */
    def validateChmods: Option[String] = {

        // this case class is used as an accumulator to count
        // octal and symbolic chmod commands for both files and dirs
        // as we validate the commands themselves
        case class ChmodValidationAcc(invalids: Seq[String] = Seq.empty,
            octalFileNum: Int = 0, octalDirNum: Int = 0,
            symbolicFileNum: Int = 0, symbolicDirNum: Int = 0
        )

        val validationAcc: ChmodValidationAcc = chmodCommands.foldLeft(new ChmodValidationAcc())((acc, mod) => {
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

        // Error message generation
        val errorMessages = {
            // Using list concatenation chain checks for conciseness
            (if (validationAcc.invalids.nonEmpty) Seq(s"Invalid chmod patterns: ${validationAcc.invalids.mkString(", ")}") else Nil) ++
                (if (validationAcc.octalFileNum > 1) Seq("Only one octal chmod command is allowed for files") else Nil) ++
                (if (validationAcc.octalDirNum > 1) Seq("Only one octal chmod command is allowed for dirs") else Nil) ++
                (if (validationAcc.octalFileNum == 1 && validationAcc.symbolicFileNum > 0) Seq("Can't have both octal and symbolic chmod commands for files") else Nil) ++
                (if (validationAcc.octalDirNum == 1 && validationAcc.symbolicDirNum > 0) Seq("Can't have both octal and symbolic chmod commands for dirs") else Nil)
        }

        prepareErrorMessage(errorMessages, "Error validating chmod commands:")
    }

    /**
     * Validate the list of filter rules.
     *
     * Validation is made by folding filter rules accumulating invalid rules in a Seq.
     * After accumulation, single optional error message is prepared using [[prepareErrorMessage()]].
     *
     * @return None if validation succeeds, Some(error-message) otherwise.
     */
    def validateFilterRules: Option[String] = {
        val errorMessages = filterRules.foldLeft(Seq.empty[String])((accumulatedErrorMessages, filterRule) => {
            filterRule match {
                case filterRulePattern(_*) => accumulatedErrorMessages
                case _ => accumulatedErrorMessages :+ s"Invalid filter rule: $filterRule"
            }
        })

        prepareErrorMessage(errorMessages, "Error validating filter rules:")
    }

    /**
     * This function validates that some boolean options are (or not) called simultaneously:
     *  - sizeOnly and ignoreTimes shouldn't be set simultaneously
     *  - delete-excluded should only be set in conjunction with delete
     * @return None if validation succeeds, Some(error-message) otherwise.
     */
    def validateFlags: Option[String] = {
        val errorMessages = {
            (if (ignoreTimes && sizeOnly) Seq("skip-times and use size-only can't be used simultaneously") else Nil) ++
                (if (deleteExcluded && !deleteExtraneous) Seq("delete-excluded must be used in conjunction with delete") else Nil)
        }

        prepareErrorMessage(errorMessages, "Error validating flags:")
    }

    /**
     * Validate the whole config at once.
     * This allows for the scopt parser to be provided with all error messages at once
     * instead of failing at the first one.
     *
     * The error messages concatenation is done by folding validation functions results,
     * accumulating errors as Option[String]. The fold is initialized with None(success),
     * and at each stage checks for the function result. It returns an updated (error)
     * status if the function result is an error.
     *
     * @return None if validation succeeds, Some(error-message) otherwise.
     */
    def validate: Option[String] = {
        Seq(
            validateSrc,
            validateDst,
            validateChmods,
            validateFilterRules,
            validateFlags
        ).fold(None)((accumulatedValidationResults, newValidationResult) => {
            // newValidationResult is a success, return unchanged accumulatedValidationResults
            if (newValidationResult.isEmpty) {
                accumulatedValidationResults
            } else {
                if (accumulatedValidationResults.isEmpty) {
                    // First error we encounter (accumulatedValidationResults would not be empty otherwise)
                    // Add error message header to newValidationResult message
                    Some(s"Argument parsing error:\n\t${newValidationResult.get}")
                } else {
                    // there is already existing error(s) and a new one - Concatenate them
                    Some(s"${accumulatedValidationResults.get}\n\t${newValidationResult.get}")
                }
            }
        })
    }

    /********************************************************************************
     * Functions and classes needed for config initialization of internals
     */

    /**
     * Create an optional HDFS [[ChmodParser]] from a rebuilt chmod string (comma-separated
     * chmod commands) for either files or dirs. The rebuilt command string is extracted from
     * the fully validated list of chmod commands as follow: the full command list is fold,
     * concatenating in an option the files or directory commands with the 'F' or 'D' prefix
     * removed if any.
     * The ChmodParser is then created if the fold has generated some command.
     *
     * @param chmods the validated sequence of commands containing both files and dirs commands
     * @param files whether the generated command string should be for files or for dirs
     * @return the generated comma-separated chmod command for either files or dirs, if any
     */
    private def getChmodParser(chmods: Seq[String], files: Boolean): Option[ChmodParser] = {
        val (inversePrefix, prefixToRemove) = if (files) ("D", 'F') else ("F", 'D')
        val rebuiltString = chmods.foldLeft(None.asInstanceOf[Option[String]])((acc, mod) => {
            // It's important to use a negative match of inversePrefix here as we want to keep both
            // prefixed commands and not-prefixed commands applying to both.
            if (! mod.startsWith(inversePrefix))
                if (acc.isEmpty) Some(mod.dropWhile(c => c == prefixToRemove))
                else Some(s"${acc.get},$mod")
            else acc
        })
        rebuiltString.map(new ChmodParser(_))
    }

    /**
     *
     * @param filterRule
     * @return
     */
    def getParsedFilterRule(filterRule: String): HdfsRsyncFilterRule = {
        filterRule match {
            case filterRulePattern(rawType, rawModifiers, rawPattern) =>
                val pattern =  java.nio.file.FileSystems.getDefault.getPathMatcher(rawPattern)
                new HdfsRsyncFilterRule(
                    ruleType = if (rawType == "+") Include() else Exclude(),
                    pattern = pattern,
                    oppositeMatch = rawModifiers.contains('!'),
                    fullPathCheck = rawModifiers.contains('/') ||
                        rawPattern.dropRight(1).contains('/') ||
                        rawPattern.contains("**"),
                    anchoredToRoot = rawPattern.startsWith("/"),
                    directoryOnly = rawPattern.endsWith("/")
                )
        }
    }

    /**
     * Function initializing inner-values of the config after parameters validation has been done.
     *
     * It initializes filesystem-api reused fields (srcFs, dstFs, srcPath, dstPath), and prebuilt
     * HDFS ChmodParser for both files and dirs (if any)
     *
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

            chmodFiles = getChmodParser(chmodCommands, files = true),
            chmodDirs = getChmodParser(chmodCommands, files = false),

            parsedFilterRules =
        )
    }
}
