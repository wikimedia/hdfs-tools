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

import java.net.URI

import org.apache.log4j.Level
import scopt.OptionParser

import scala.io.Source

object HdfsRsyncCLI {

    /**
     * Function loading filter rules from a file, one rule or pattern per line.
     * Empty lines and lines starting with a # are discarded.
     *
     * @param filepath the path of the file to read
     * @param config the config to update
     * @param ruleTypeOpt the rule-type to add as header to the parsed line if any
     * @return the updated config
     */
    private def loadFilterRulesFromFile(
        filepath: String,
        config: HdfsRsyncConfig,
        ruleTypeOpt: Option[HdfsRsyncFilterRuleType] = None
    ): HdfsRsyncConfig = config.copy(
        filterRules = config.filterRules ++
            Source.fromFile(filepath).getLines.flatMap(line => {
                val trimmed = line.trim
                if (trimmed.nonEmpty && ! trimmed.startsWith("#")) {
                    // If defined, ruleTypeOpt will provide + or - for include or exclude
                    // See [[HdfsRsyncFilterRuleType]] trait
                    Seq((ruleTypeOpt ++ Seq(trimmed)).mkString(" "))
                } else {
                    Nil
                }

            })
    )

    /**
     * Command line options parser
     */
    val argsParser = new OptionParser[HdfsRsyncConfig]("hdfs-rsync") {
        head("hdfs-rsync", "Provides an rsync equivalent to copy data between HDFS and local FS.")

        note(
            """
              |To prevent mistakes between local and remote filesystems, src and dst have to be provided as
              |fully qualified absolute URIs, for instance file:/home or hdfs:///user/hive.
              |
              |src should be globs leading to some existing files and dst should be a folder, existing
              |or at a path where it can be created.
              |
              |Note: a trailing slash in src (as in /example/src/) is changed to a pattern matching only the
              |      directory content (as in /example/src/*) mimicking standard rsync behavior.
              |
              |Note: The behavior of standard rsync when there are multiple sources and colliding folders or
              |      files is to use the first-source values, for directories permissions and time-modifications
              |      as their content is merged, and for files content and metadata. When collision occur on files
              |      and folders, instead of keeping folders we raise an exception.
              |
              |User and group mappings should be in the format: 'pattern:value'
              | * pattern will be matched against src username and '*' is accepted as a wildcard and matches any
              |   character 0 or more times
              | * value is the new value that will be assigned if pattern matches
              |
              |Syntax for filter/include/exclude rules is similar to the standard rsync one:
              | * One rule per command-line option.
              | * Order matters as inclusion/exclusion is done picking the first matching pattern.
              | * Include/exclude options expect patterns only (use filter if you need modifiers)
              | * Extraneous files to be deleted on dst follow include/exclude rules. Use --delete-excluded
              |   to NOT use the rules on those files.
              | * the filter rule format is: RULE[MODIFIERS] PATTERN
              |     - RULE is either '+' (include) or '-' (exclude).
              |     - MODIFIERS are optional and can be '!' (negative pattern match) or '/' (match pattern
              |       against full path even if no '/' or ''), or both.
              |     - PATTERN is the pattern to match.
              |   Note: A single space is expected between the rule and modifier char sequence and the pattern.
              |   Note: Use quotes around the rules when you use wildcard patterns to prevent the shell
              |         interpreting them.
              |
              | About patterns:
              | * Pattern wildcard characters are: '*', '**', '?', (see rsync doc). You can escape wildcards
              |   using '\'. We don't reproduce the character-class wildcards as their definition is not
              |   present in documentation.
              | * Patterns starting with a '/' are anchored, meaning they match only from the root of the
              |   transfer  (similar to '^' in regex).
              | * Patterns with a trailing '/' match only directories.
              | * Patterns containing '/' (not counting trailing '/') or '**' are matched against the full
              |   path (including leading directories). Otherwise it is matched only against the final
              |   component of the filename.
            """.stripMargin
        )

        help("help")
            .text("Prints this usage text")

        opt[Unit]("dry-run")
            .optional()
            .action((_, c) => c.copy(dryRun = true))
            .text("Only log instead of actually taking actions (default: false)")

        opt[String]("app-log-level")
            .optional()
            .action((x, c) => c.copy(applicationLogLevel = x))
            .text("Set application log level (default: INFO)")

        opt[String]("all-log-level")
            .optional()
            .action((x, c) => c.copy(rootLogLevel = x))
            .text("Set root logger level (default: ERROR)")

        opt[String]("log-file")
            .optional()
            .action((x, c) => c.copy(logFile = Some(x)))
            .text("File to write logs instead of sending them to stdout")



        opt[Unit]('r', "recursive")
            .optional()
            .action((_, c) => c.copy(recurse = true))
            .text("Recurse into directories (default: false)")

        opt[Unit]('d', "dirs")
            .optional()
            .action((_, c) => c.copy(copyDirs = true))
            .text("Copy directories without recursion (default: false)")

        opt[Unit]('m', "prune-empty-dirs")
            .optional()
            .action((_, c) => c.copy(pruneEmptyDirs = true))
            .text("prune empty directory chains (default: false)")



        opt[Unit]("resolve-conflicts")
            .optional()
            .action((_, c) => c.copy(resolveConflicts = true))
            .text("Resolve multi-source files/directories conflicts (default: false)\n" +
                "\t\tNote: times and perms conflicts of merged-folders are resolved by default using the defined strategy.")

        opt[Unit]("use-most-recent-time")
            .optional()
            .action((_, c) => c.copy(useMostRecentModifTimes = true))
            .text("Use most recent modificationTime object instead of first listed src in conflicts (default: false)")



        opt[Unit]("existing")
            .optional()
            .action((_, c) => c.copy(existing = false))
            .text("Skip creating new files in dst (default: false)")

        opt[Unit]("ignore-existing")
            .optional()
            .action((_, c) => c.copy(ignoreExisting = false))
            .text("skip updating files that exist in dst (default: false)")

        opt[Unit]('u', "update")
            .optional()
            .action((_, c) => c.copy(update = false))
            .text("Skip files that are newer in dst (default: false)")



        opt[Unit]("size-only")
            .optional()
            .action((_, c) => c.copy(sizeOnly = true))
            .text("Skip files that match in size (default: false)")

        opt[Long]("times-diff-threshold")
            .optional()
            .action((x, c) => c.copy(acceptedTimesDiffMs = x))
            .text("Milliseconds by which modificationTimes can differ and still be considered equal (default: 1000)")

        opt[Unit]('I', "ignore-times")
            .optional()
            .action((_, c) => c.copy(ignoreTimes = true))
            .text("Don't skip files that match size and time (default: false)")



        opt[Unit]("delete")
            .optional()
            .action((_, c) => c.copy(deleteExtraneous = true))
            .text("Delete extraneous files from dst dirs (default: false)")

        opt[Unit]("delete-excluded")
            .optional()
            .action((_, c) => c.copy(deleteExcluded = true))
            .text("Delete extraneous files from dst dirs even if present in exclude rule (default: false)")



        opt[Unit]('t', "times")
            .optional()
            .action((_, c) => c.copy(preserveTimes = true))
            .text("Preserve modification times (default: false)")



        opt[Unit]('p', "perms")
            .optional()
            .action((_, c) => c.copy(preservePerms = true))
            .text("Preserve permissions (default: false)")

        opt[Seq[String]]("chmod")
            .optional()
            .unbounded()
            .action((x, c) => c.copy(chmodCommands = c.chmodCommands ++ x.map(_.trim)))
            .text("affect file and/or directory permissions")



        opt[Unit]('o', "owner")
            .optional()
            .action((_, c) => c.copy(preserveOwner = true))
            .text("preserve owner (super-user only) (default: false)")

        opt[Seq[String]]("usermap")
            .optional()
            .unbounded()
            .action((x, c) => c.copy(usermap = c.usermap ++ x))
            .text("custom username mapping (only applied with --owner) (default: empty)")

        opt[Unit]('g', "group")
            .optional()
            .action((_, c) => c.copy(preserveGroup = true))
            .text("Preserve group (default: false)")

        opt[Seq[String]]("groupmap")
            .optional()
            .unbounded()
            .action((x, c) => c.copy(groupmap = c.groupmap ++ x))
            .text("custom groupname mapping (only applied with --group) (default: empty)")

        opt[String]("chown")
            .optional()
            .action((x, c) => c.copy(chown = Some(x)))
            .text("simple username/groupname mapping (only applied with --owner/--group) (default: None)")



        opt[String]("filter")
            .optional()
            .unbounded()
            .action((x, c) => c.copy(filterRules = c.filterRules :+ x.trim))
            .text("Add a filter rule")

        opt[String]("filter-from-file")
            .optional()
            .unbounded()
            .action((x, c) => loadFilterRulesFromFile(x, c, None))
            .text("Add all filter rules from file")

        opt[String]("include")
            .optional()
            .unbounded()
            .action((x, c) => c.copy(filterRules = c.filterRules :+ Include().makeRule(x.trim)))
            .text("Add inclusion pattern (this is an alias for: --filter '+ PATTERN')")

        opt[String]("include-from-file")
            .optional()
            .unbounded()
            .action((x, c) => loadFilterRulesFromFile(x, c, Some(Include())))
            .text("Add all inclusion patterns from file")

        opt[String]("exclude")
            .optional()
            .unbounded()
            .action((x, c) => c.copy(filterRules = c.filterRules :+ Exclude().makeRule(x.trim)))
            .text("Add exclusion pattern (this is an alias for: --filter '- PATTERN')")

        opt[String]("exclude-from-file")
            .optional()
            .unbounded()
            .action((x, c) => loadFilterRulesFromFile(x, c, Some(Exclude())))
            .text("Add all exclusion patterns from file")



        // This is a hack to overcome scopt limitiation of having to put unbounded argument
        // as last option. We store all URIs in allURIs and the take the first n-1 ones for
        // srcList and the last one for dst.
        arg[URI]("src...[dst]")
            .unbounded()
            .action((x, c) => c.copy(allURIs = c.allURIs :+ x))
            .text("Fully qualified URI, one or more sources followed by zero or one destination")



        checkConfig(_.validate.map(Left(_)).getOrElse(Right(())))

    }

    def main(args: Array[String]): Unit = {
        argsParser.parse(args, HdfsRsyncConfig()) match {
            case Some(configNotInitialized) =>
                val config = configNotInitialized.initialize
                val rsync = new HdfsRsyncExec(config)
                rsync.apply()
            case None => sys.exit(1)
        }
    }

}