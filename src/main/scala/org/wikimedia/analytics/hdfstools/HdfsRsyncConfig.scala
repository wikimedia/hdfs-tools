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
import java.nio.file.FileSystems

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.ChmodParser
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Logger,Level,Appender,ConsoleAppender,RollingFileAppender,PatternLayout}

import scala.util.Try
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
    // This value is a hack to overcome scopt limitation
    // of having unbounded args as the last option.
    allURIs: Seq[URI] = Seq.empty[URI],
    srcsList: Seq[URI] = Seq.empty[URI],
    dst: Option[URI] = None,

    // Options
    dryRun: Boolean = false,
    rootLogLevel: String = "ERROR",
    applicationLogLevel: String = "INFO",
    logFile: Option[String] = None,

    recurse: Boolean = false,
    copyDirs: Boolean = false,
    pruneEmptyDirs: Boolean = false,

    resolveConflicts: Boolean = false,
    useMostRecentModifTimes: Boolean = false,

    existing: Boolean = false,
    ignoreExisting: Boolean = false,
    update: Boolean = false,

    ignoreTimes: Boolean = false,
    sizeOnly: Boolean = false,
    acceptedTimesDiffMs: Long = 1000,

    preserveTimes: Boolean = false,

    preservePerms: Boolean = false,
    chmodCommands: Seq[String] = Seq.empty[String],

    preserveOwner: Boolean = false,
    usermap: Seq[String] = Seq.empty,
    preserveGroup: Boolean = false,
    groupmap: Seq[String] = Seq.empty,
    chown: Option[String] = None,

    deleteExtraneous: Boolean = false,
    deleteExcluded: Boolean = false,

    filterRules: Seq[String] = Seq.empty[String],


    // Internal (need to be initialized)
    srcFs: FileSystem = null,
    srcPathsList: Seq[Path] = Seq.empty[Path],
    dstFs: FileSystem = null,
    dstPath: Path = null,
    chmodFiles: Option[ChmodParser] = None,
    chmodDirs: Option[ChmodParser] = None,
    parsedUsermap: Seq[(Regex, String)] = Seq.empty,
    parsedGroupmap: Seq[(Regex, String)] = Seq.empty,
    parsedFilterRules: Seq[HdfsRsyncFilterRule] = Seq.empty[HdfsRsyncFilterRule],
    // Set hadoop-conf
    hadoopConf: Configuration = new Configuration(),
    // Logging appender - usefull to manage logging in tests
    logAppender: Seq[Appender] = Seq.empty
) {

    // Accepted log levels
    private val validLogLevels = Set("ERROR", "WARN", "INFO", "DEBUG")

    // These patterns are modified copies of the ones defined in hadoop chmod parser
    // https://github.com/apache/hadoop/blob/trunk/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/permission/ChmodParser.java
    private val chmodOctalPattern = "^([FD]?)([01]?[0-7]{3})$".r
    private val chmodSymbolicPattern = "^([FD]?)([ugoa]{0,3}[+=-]{1}[rwxXt]{1,4})$".r

    // Pattern extracted and modified from https://github.com/shadow-maint/shadow/blob/master/libmisc/chkname.c#L58
    private val mappingPatternPattern = "^([a-z_]|\\*)([a-z0-9_-]|\\*)*(\\$)?$".r
    private val mappingNamePattern = "^[a-z_][a-z0-9_-]*\\$?$".r
    private val maxNameSize = 32

    // Group 1: Include/exclude
    // Group 2: Modifiers (! is NOT MATCHING, / means checked against absolute pathname)
    // Group 3: actual pattern
    private val filterRulePattern = "^([+-])(!?/?|/!) ([^ ].*)$".r

    //val loggerName = "org.wikimedia.analytics.hdfstools.hdfs"

    // Pattern used for logging
    private val loggingPattern = new PatternLayout("%d{yyyy-MM-dd'T'HH:mm:ss.SSS} %p %c{1} %m%n")

    /**
     * This function configures logging on root and package logger.
     *  - Set logging level for root logger
     *  - Configure appenders:
     *    - If no logAppender nor logFile, use consoleAppender
     *    - If no logAppender and logFile, use RollingFileAppender
     *    - If logAppender, use them
     */
    private def configureLogging(): Unit = {
        val rootLogger = Logger.getRootLogger
        rootLogger.setLevel(Level.toLevel(rootLogLevel))

        // Configure appenders on root logger
        rootLogger.removeAllAppenders ()
        val consoleLogAppender = if (logAppender.isEmpty) Some(new ConsoleAppender(loggingPattern, ConsoleAppender.SYSTEM_OUT)) else None
        val logFileAppender = logFile.map(new RollingFileAppender(loggingPattern, _))
        (logAppender ++ consoleLogAppender ++ logFileAppender).foreach (appender => rootLogger.addAppender (appender) )
    }

    /**
     * Function providing srcList from allURIs (scopt hack)
     * @return the n-1 first elements of allURIs if it contains more than one element,
     *         allURIs otherwise.
     */
    private def getSrcsList: Seq[URI] = {
        if (allURIs.size > 1) allURIs.dropRight(1) else allURIs
    }

    /**
     * Function providing dst from allURIs (scopt hack)
     * @return the last element of allURIs is it contains more than one element,
     *         None otherwise
     */
    private def getDst: Option[URI] = {
        if (allURIs.size > 1) Some(allURIs.last) else None
    }

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
            Some(s"$errorMessageHeader\n${errorMessages.map(s => s"\t\t$s").mkString("\n")}")
        }
    }

    /********************************************************************************
     * Parameters validation functions
     */

    def validateLogLevels: Option[String] = {
        val errors =
            (if (! validLogLevels.contains(applicationLogLevel)) Seq(s"Invalid app-log-level: $applicationLogLevel") else Nil) ++
                (if (! validLogLevels.contains(rootLogLevel)) Seq(s"Invalid all-log-level: $rootLogLevel") else Nil)

        prepareErrorMessage(errors, "Error validating log levels:")
    }

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
        try {
            if (uri.getScheme == null || uri.getScheme.isEmpty) Some(s"$uri does not specify scheme")
            else if (uri.getPath == null || !uri.getPath.startsWith("/")) Some(s"$uri is not absolute")
            else {
                val fs = getFS(uri)
                val path = new Path(uri)
                // Using globStatus to check for path existence/files to copy as src can be a glob
                val globResult = fs.globStatus(path)
                if (globResult != null)
                    if (isSrc || fs.isDirectory(path)) None
                    else Some(s"$uri dst dir cannot be created")
                else if (!isSrc && Try(fs.mkdirs(path)).isSuccess) None
                else Some(s"$uri does not exist")
            }
        } catch {
            case e: IOException => Some(s"${e.getMessage}")
        }
    }

    /**
     * Validate srcsList - They should all be a valid URIs of glob patterns returning non-null result.
     * They also all should have the same scheme.
     *
     * Note: Globs returning null are patterns without special characters not matching any file.
     *       Patterns with special characters not matching any file return an empty list.
     *
     * @return None if validation succeeds, Some(error-message) otherwise.
     */
    def validateSrcsList: Option[String] = {
        val srcList = getSrcsList // scopt hack, see [[getSrcsList]]
        val sameSchemeError = {
            if (srcList.tail.forall(src => src.getScheme == srcList.head.getScheme)) {
                None
            } else {
                Some("not all src have same scheme")
            }
        }
        val errors = srcList.flatMap(src => validateURI(src, isSrc = true)) ++ sameSchemeError.toSeq

        prepareErrorMessage(errors, "Error validating src list:")
    }

    /**
     * Validate dst - should be a valid URI for an existing directory
     *
     * @return None if validation succeeds, Some(error-message) otherwise.
     */
    def validateDst: Option[String] = {
        // As dst is an option we apply validation only if it is defined and succeed otherwise
        val dst = getDst // scopt hack, see [[getDst]]
        val errors = dst.flatMap(dstVal => validateURI(dstVal, isSrc = false)).toSeq
        prepareErrorMessage(errors, "Error validating dst:")
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

    def validateChown: Option[String] = {
        val errorMessages = chown.map(chownValue => {
            val parsingErrors = {
                val parts = chownValue.split(":")
                val partsNumber = parts.length
                if (partsNumber == 1 || partsNumber == 2) {
                    val username = parts(0)
                    val usernameErrors = {
                        if (username.length > 0) {
                            (if (mappingNamePattern.findFirstIn(username).isEmpty) Seq(s"Invalid username: $username") else Nil) ++
                                (if (username.length > maxNameSize) Seq(s"Invalid username size (max 32): $username") else Nil)
                        } else {
                            if (parts.length == 1) Seq(s"Invalid empty username") else Nil
                        }
                    }

                    val groupnameErrors = {
                        if (partsNumber == 2) {
                            val groupname = parts(1)
                            if (groupname.length > 0) {
                                (if (mappingNamePattern.findFirstIn(groupname).isEmpty) Seq(s"Invalid groupname: $groupname") else Nil) ++
                                    (if (groupname.length > maxNameSize) Seq(s"Invalid groupname size (max 32): $groupname") else Nil)
                            } else {
                                // No need to check for empty username as ":".split(":") returns emtpy array
                                Nil
                            }
                        } else {
                            Nil
                        }
                    }

                    usernameErrors ++ groupnameErrors

                } else {
                    Seq(s"Invalid chown format: $chownValue")
                }
            }

            (if (usermap.nonEmpty || groupmap.nonEmpty) Seq("chown and usermap/groupmap cannot be used simultaneously") else Nil) ++
                parsingErrors

        }).getOrElse(Seq.empty)

        prepareErrorMessage(errorMessages, s"Error validating chown:")
    }

    /**
     * Function validating a list of user or group mappings.
     * Mappings should be in the format: 'pattern:value' where pattern will be matched against
     * src username (only * is accepted as a wildcard and matches any character 0 or more times)
     * and value is the new value that will be assigned if pattern matches.
     *
     * @param mappings the list of mappings to validate
     * @param name the name of the parameter being checked (should be usermap or groupmap)
     * @return None if validation succeeds, Some(error-message) otherwise.
     */
    def validateMapping(mappings: Seq[String], name: String): Option[String] = {

        val errorMessages = mappings.flatMap(mapping => {
            val parts = mapping.split(":")
            if (parts.length == 2 && parts(0).nonEmpty && parts(1).nonEmpty) {
                val pattern = parts(0)
                val value = parts(1)
                (if (mappingPatternPattern.findFirstIn(pattern).isEmpty) Seq(s"Invalid mapping pattern: $pattern") else Nil) ++
                    (if (mappingNamePattern.findFirstIn(value).isEmpty) Seq(s"Invalid mapping value: $value") else Nil) ++
                    (if (pattern.length > maxNameSize) Seq(s"Invalid mapping pattern size (max 32): $pattern") else Nil) ++
                    (if (value.length > maxNameSize) Seq(s"Invalid mapping value size (max 32): $value") else Nil)
            } else {
                Seq(s"Invalid mapping format: $mapping")
            }
        })

        prepareErrorMessage(errorMessages, s"Error validating $name:")
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
     *  - recurse and copyDirs shouldn't be set simultaneously
     * @return None if validation succeeds, Some(error-message) otherwise.
     */
    def validateFlags: Option[String] = {
        val errorMessages = {
            (if (ignoreTimes && sizeOnly) Seq("skip-times and use size-only cannot be used simultaneously") else Nil) ++
                (if (deleteExcluded && !deleteExtraneous) Seq("delete-excluded must be used in conjunction with delete") else Nil) ++
                (if (recurse && copyDirs) Seq("recurse and dirs cannot be used simultaneously") else Nil)
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
            validateLogLevels,
            validateSrcsList,
            validateDst,
            validateChmods,
            validateChown,
            validateMapping(usermap, "usermap"),
            validateMapping(groupmap, "groupmap"),
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
     * chmod commands). The rebuilt command string is computed from the fully validated
     * list of chmod commands as follow: the full command list is fold,
     * concatenating in an option the files or directory commands with the 'F' or 'D' prefix
     * removed if any.
     * The ChmodParser is then created if the fold has generated some command.
     *
     * @param chmods the validated sequence of commands containing either files and dirs commands
     * @return the generated comma-separated chmod command for either files or dirs, if any
     */
    private def getChmodParser(chmods: Seq[String]): Option[ChmodParser] = {
        val rebuiltString = chmods.foldLeft(None.asInstanceOf[Option[String]])((acc, mod) => {
            // Removing rsync specific F and D to build a standard chmod command
            val updatedMod = if (mod.head == 'F' || mod.head == 'D') mod.tail else mod
            if (acc.isEmpty) {
                Some(updatedMod)
            } else {
                Some(s"${acc.get},$updatedMod")
            }
        })
        rebuiltString.map(new ChmodParser(_))
    }

    /**
     * Function providing username/groupname mapping pattern value from chown
     * @param idx either 0 for username or 1 for groupname mapping pattern
     * @return None if no define chown or emtpy part, Some(pattern) otherwise
     */
    private def getChownPart(idx: Int): Option[String] = {
        chown.flatMap(chownValue => {
            val parts = chownValue.split(":")
            if (idx < parts.length && parts(idx).length > 0) Some(s"*:${parts(idx)}") else None
        })
    }
    private def getChownUsernamePattern: Option[String] = getChownPart(0)
    private def getChownGroupnamePattern: Option[String] = getChownPart(1)


    /**
     * Parse a user/group mapping into a regex (* -> .*) and a value.
     * Expected separator is ':'
     * @param mapping the mapping to parse
     * @return the parsed (regex, value) pair
     */
    def getParsedMapping(mapping: String): (Regex, String) = {
        val parts = mapping.split(":")
        val (pattern, value) = (parts(0), parts(1))
        (new Regex(pattern.replaceAllLiterally("*", ".*")), value)
    }

    /**
     * Parse a filter rule into an HdfsRsyncFilterRule, the scala object allowing to
     * apply the rule on paths.
     *
     * Note: We use a hack to facilitate matching glob patterns: we use a PathMatcher
     *       from the default filesystem. Match-check will be then done using a fake
     *       java.nio.file.path.
     *
     * @param filterRule the filter rule to parse
     * @return the created HdfsRsyncFilterRule
     */
    def getParsedFilterRule(filterRule: String): HdfsRsyncFilterRule = {
        filterRule match {
            case filterRulePattern(rawType, rawModifiers, rawPattern) =>
                val anchoredToRoot = rawPattern.startsWith("/")
                val fullPathCheck = rawPattern.dropRight(1).contains('/') || rawPattern.contains("**")
                val forceFullPathCheck = rawModifiers.contains('/')

                val updatedPattern = {
                    // Adding wildcard before pattern to match relative path over
                    // full-path when necessary
                    if (fullPathCheck && !anchoredToRoot) {
                        s"glob:**$rawPattern"
                    } else {
                        s"glob:$rawPattern"
                    }
                }
                val pattern = FileSystems.getDefault.getPathMatcher(updatedPattern)

                new HdfsRsyncFilterRule(
                    ruleType = if (rawType == "+") Include() else Exclude(),
                    pattern = pattern,
                    oppositeMatch = rawModifiers.contains('!'),
                    fullPathCheck = fullPathCheck || forceFullPathCheck,
                    forceFullPathCheck = forceFullPathCheck,
                    anchoredToBasePath = anchoredToRoot,
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
        configureLogging()

        val srcsList = getSrcsList // scopt hack, see [[getSrcsList]]
        val dst = getDst           // scopt hack, see [[getDst]]
        this.copy(
            srcsList = srcsList,
            srcFs = getFS(srcsList.head),
            // Add * to trailing slash to mimic rsync not copying src last folder but its content
            srcPathsList = srcsList.map(src => {
                if (src.toString.endsWith("/")) new Path(s"$src/*") else new Path(src)
            }),

            dst = dst,
            // Only initialize dstFS if dst is defined
            dstFs = dst.map(d => {
                val fs = getFS(d)
                fs.setWriteChecksum(false)
                fs
            }).orNull,
            dstPath = dst.map(d => new Path(d)).orNull,

            // It's important to use a negative match of inversePrefix here as we want to keep both
            // prefixed commands and not-prefixed commands applying to both.
            chmodFiles = getChmodParser(chmodCommands.filter(!_.startsWith("D"))),
            chmodDirs = getChmodParser(chmodCommands.filter(!_.startsWith("F"))),

            // Use either usermap/groupmap or chown defined value (values are validated, this will work)
            parsedUsermap = (usermap ++ getChownUsernamePattern).map(getParsedMapping),
            parsedGroupmap = (groupmap ++ getChownGroupnamePattern).map(getParsedMapping),

            parsedFilterRules = filterRules.map(getParsedFilterRule)

        )
    }
}
