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

object HdfsRsyncCLI {

    /**
     * Command line options parser
     */
    val argsParser = new OptionParser[HdfsRsyncConfig]("hdfs-rsync") {
        head("hdfs-rsync tool", "Provides rsync equivalent to copy data between HDFS and local FS.")

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

        opt[Unit]('v', "verbose")
            .optional()
            .action((_, c) => c.copy(logLevel = Level.DEBUG))
            .text("Add verbosity to logging (DEBUG)")

        opt[Unit]('q', "quiet")
            .optional()
            .action((_, c) => c.copy(logLevel = Level.WARN))
            .text("Remove verbosity from logging (WARN)")

        opt[Unit]('r', "recursive")
            .optional()
            .action((_, c) => c.copy(recurse = true))
            .text("Recurse into directories (default: false)")

        opt[Unit]('p', "perms")
            .optional()
            .action((_, c) => c.copy(preservePerms = true))
            .text("Preserve permissions (default: false)")

        opt[Unit]('t', "times")
            .optional()
            .action((_, c) => c.copy(preserveTimes = true))
            .text("Preserve modification times (default: false)")

        opt[Long]("times-diff")
            .optional()
            .action((x, c) => c.copy(acceptedTimesDiffMs = x))
            .text("Milliseconds by which modificationTimes can differ and still be considered equal (default: 1000)")

        opt[Unit]("size-only")
            .optional()
            .action((_, c) => c.copy(sizeOnly = true))
            .text("Skip files that match in size (default: false)")

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

        opt[Seq[String]]("chmod")
            .optional()
            .unbounded()
            .action((x, c) => {
                c.copy(chmodCommands = c.chmodCommands ++ x.map(_.trim))
            })
            .text("affect file and/or directory permissions")

        opt[String]("filter")
            .optional()
            .unbounded()
            .action((x, c) => c.copy(filterRules = c.filterRules :+ x.trim))
            .text("Add a filter rule")

        opt[String]("include")
            .optional()
            .unbounded()
            .action((x, c) => c.copy(filterRules = c.filterRules :+ s"+ ${x.trim}"))
            .text("Add inclusion pattern (this is an alias for: --filter '+ PATTERN')")

        opt[String]("exclude")
            .optional()
            .unbounded()
            .action((x, c) => c.copy(filterRules = c.filterRules :+ s"- ${x.trim}"))
            .text("Add exclusion pattern (this is an alias for: --filter '- PATTERN')")

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