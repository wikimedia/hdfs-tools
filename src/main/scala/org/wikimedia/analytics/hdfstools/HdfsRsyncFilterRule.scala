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


import java.nio.file.{FileSystems, FileSystem, PathMatcher}

import org.apache.hadoop.fs.{FileStatus, Path}


sealed trait RuleType
case class Include() extends RuleType
case class Exclude() extends RuleType

class HdfsRsyncFilterRule(
    ruleType: RuleType,
    pattern: PathMatcher,
    oppositeMatch: Boolean,
    fullPathCheck: Boolean,
    anchoredToRoot: Boolean,
    directoryOnly: Boolean
) {
    def matches(fileStatus: FileStatus, transferRoot: Path): Boolean = {
        // Get path without scheme
        val fullPath = fileStatus.getPath.toUri.getPath
        val refPath = {
            if (fullPathCheck) {
                if (anchoredToRoot) {
                    // Similarly, remove scheme from transferRoot to trim
                    fullPath.replaceAll(s"^${transferRoot.toUri.getPath}", "")
                } else {
                    fullPath
                }
            } else {
                fileStatus.getPath.getName
            }
        }

        // Using XOR operator to inverse result if oppositeModifier is true
        oppositeMatch ^ (
            pattern.matches(FileSystems.getDefault.getPath(refPath)) &&
                ((directoryOnly && fileStatus.isDirectory) || !directoryOnly)
            )
    }
}
