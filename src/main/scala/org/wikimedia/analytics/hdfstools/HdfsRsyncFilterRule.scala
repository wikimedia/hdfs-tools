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


import java.nio.file.{FileSystems, PathMatcher}

import org.apache.hadoop.fs.{FileStatus, Path}


/**
 * Enum to represent if a rule is an exclusion or inclusion rule
 */
sealed trait HdfsRsyncFilterRuleType
case class Include() extends HdfsRsyncFilterRuleType
case class Exclude() extends HdfsRsyncFilterRuleType

/**
 * Class defining a FilterRule that can be applied to match a FileStatus
 * for a given BasePath (root-of-transfer).
 *
 * @param ruleType either Include() or Exclude()
 * @param pattern the glob pattern to match
 * @param oppositeMatch to return true if the pattern doesn't match and vice versa
 * @param fullPathCheck to match the pattern against the full path instead of filename
 * @param anchoredToBasePath to match the pattern from the BasePath (the folder
     *                       containing the file at recursion step 0)
 * @param directoryOnly to match directories only
 */
class HdfsRsyncFilterRule(
    val ruleType: HdfsRsyncFilterRuleType,
    pattern: PathMatcher,
    oppositeMatch: Boolean,
    fullPathCheck: Boolean,
    forceFullPathCheck: Boolean,
    anchoredToBasePath: Boolean,
    directoryOnly: Boolean
) {

    /**
     * Function matching the rule against a fileStatus for a given BasePath.
     *
     * Note: We use a hack to facilitate matching the glob pattern: we use a PathMatcher
     *       from the default filesystem. Match-check will be then done using a fake
     *       java.nio.file.path.
     *
     * @param fileStatus the fileStatus to check
     * @param basePath the base path of the checked file (root-of-the-transfer)
     * @return true if the rule matches, false otherwise.
     */
    def matches(fileStatus: FileStatus, basePath: Path): Boolean = {
        // Get path without scheme
        val fullPath = fileStatus.getPath.toUri.getPath
        val pathToCheck = {
            if (fullPathCheck) {
                if (!forceFullPathCheck && anchoredToBasePath) {
                    // Trim full path to use only the path portion that is after basePath
                    fullPath.replaceAll(s"^${basePath.toUri.getPath}", "")
                } else {
                    fullPath
                }
            } else {
                // If not fullPathCheck, only use filename
                fileStatus.getPath.getName
            }
        }

        // Using XOR operator to inverse result if oppositeModifier is true
        oppositeMatch ^ (
            pattern.matches(FileSystems.getDefault.getPath(pathToCheck)) &&
                ((directoryOnly && fileStatus.isDirectory) || !directoryOnly)
            )
    }
}
