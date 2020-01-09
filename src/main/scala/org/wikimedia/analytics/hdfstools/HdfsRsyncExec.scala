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


import org.apache.hadoop.fs.permission.{ChmodParser, FsPermission}
import org.apache.hadoop.fs.{FileStatus, FileUtil, Path}
import org.apache.log4j.{Level, Logger}

import scala.collection.immutable.ListMap
import scala.util.Try
import scala.util.matching.Regex


/**
 * Class executing rsync given the provided config
 *
 * As a convention, we use dst to name the parent directories of a copy,
 * and target the exact target of a copy in dst (with same filename part)
 */
class HdfsRsyncExec(config: HdfsRsyncConfig) {

    /**
     * Logging initialization: Use level from config
     */
    val log: Logger = {
        val l = Logger.getLogger(this.getClass)
        l.setLevel(Level.toLevel(config.applicationLogLevel))
        l
    }


    /********************************************************************************
     * Functions for files/dirs copy / update / skip
     */

    /**
     * This function is called when a directory needs to be created in dst.
     *
     * @param target the dst directory path to create
     * @param overwriteFile whether the directory creation should replace an existing file or not
     * @return Left(Path) of target in case of dryrun (no FileStatus available), Right(FileStatus)  of the
     *         created directory otherwise
     */
    private def createDirectory(target: Path, overwriteFile: Boolean): Either[Path, FileStatus] = {
        val msgHeader = if (overwriteFile) "OVERWRITE_DIR" else "CREATE_DIR"
        if (config.dryRun) {
            log.info(s"$msgHeader [dryrun] - $target")
            Left(target)
        } else {
            log.debug(s"$msgHeader - $target")
            if (overwriteFile) {
                config.dstFs.delete(target, false)
            }
            config.dstFs.mkdirs(target)
            Right(config.dstFs.getFileStatus(target))
        }
    }

    /**
     * This function is called when a source file (directory with --dirs) to be copied to dst.
     *
     * @param src the source file/directory to copy
     * @param target the target in dst for the copied src file
     * @param isUpdate whether the copy is an update or a file creation.
     * @return Left(Path) of target in case of dryrun (no FileStatus available), Right(FileStatus)
     *         of the copied file otherwise.
     */
    private def copy(src: FileStatus, target: Path, isUpdate: Boolean): Either[Path, FileStatus] = {
        val srcPath = src.getPath
        val msgHeader = if (isUpdate) "UPDATE_FILE" else "COPY_FILE"
        if (config.dryRun) {
            log.info(s"$msgHeader [dryrun] - $srcPath --> $target")
            Left(target)
        } else {
            log.debug(s"$msgHeader - $srcPath --> $target")
            // booleans: deleteSource = false, overwrite = true
            FileUtil.copy(config.srcFs, srcPath, config.dstFs, target, false, true, config.hadoopConf)
            Right(config.dstFs.getFileStatus(target))
        }
    }


    /********************************************************************************
     * Functions for files/directories metadata updates (permissions / modification time)
     */

    /**
     * This function compares two FileStatuses modificationTimestamps, with an approximation defined
     * in config. We need to accept approximation in modificationTime as different filesystem precisions
     * lead to inappropriate inequality and therefore to more data transfer.
     *
     * @param left the left FileStatus to compare
     * @param right the right to compare
     * @return 0 if left and right are considered equal, a negative value if left is smaller and
     *         a positive value if left is bigger.
     */
    private def approxCompareTimestamps(left: FileStatus, right: FileStatus): Int = {
        val diff = left.getModificationTime - right.getModificationTime
        if (math.abs(diff) <= config.acceptedTimesDiffMs) 0 else diff.signum
    }

    /**
     * This function is called to update modificationTime of the target file.
     * Target modificationTime is given the same value as the src one.
     *
     * @param src the src from which to get modificationTimestamp
     * @param target The target to update in dst, as either a Left(path) in case of dryrun
     *               (new file are not copied in dryrun mode, so they are represented as Path
     *               instead of FileStatus), or a Right(FileStatus) when target is available
     *               (we could still be in dryrun mode).
     */
    private def preserveModificationTime(src: FileStatus, target: Either[Path, FileStatus]): Unit = {
        val srcPath = src.getPath
        target match {
            case Left(targetPath) =>
                if (config.dryRun) {
                    log.info(s"UPDATE_TIMES [dryrun] - $srcPath --> $targetPath")
                }
            case Right(targetFileStatus) =>
                val targetPath = targetFileStatus.getPath
                val newModifTimes = src.getModificationTime
                if (approxCompareTimestamps(src, targetFileStatus) == 0) {
                    if (config.dryRun) {
                        log.info(s"UPDATE_TIMES [dryrun] - $srcPath --> $targetPath [$newModifTimes]")
                    } else {
                        log.debug(s"UPDATE_TIMES - $srcPath --> $targetPath [$newModifTimes]")
                        config.dstFs.setTimes(targetPath, newModifTimes, -1)
                    }
                }
        }
    }

    /**
     * This function is called when permissions need to be updated.
     * It applies the permission update using:
     *  - src permissions as a base in case of preservation, currently existing permissions otherwise
     *  - the ChmodParser to update the base permissions
     *
     * @param src the src from which to get base permissions if needed
     * @param target the target to update in dst, as either a Left(path) in case of dryrun
     *               (new objects are not existing in dryrun mode, so they are represented as Path
     *               instead of FileStatus), or a Right(FileStatus) when target is available
     *               (we could still be in dryrun mode).
     * @param chmodParser the ChmodParser to use to update permissions (if any)
     */
    private def updatePerms(
        src: FileStatus,
        target: Either[Path, FileStatus],
        chmodParser: Option[ChmodParser]
    ): Unit = {
        val srcPath = src.getPath
        target match {
            case Left(targetPath) =>
                if (config.dryRun) {
                    log.info(s"UPDATE_PERMS [dryrun] -  $srcPath --> $targetPath")
                }
            case Right(targetFileStatus) =>
                val baseFileStatus = if (config.preservePerms) src else targetFileStatus
                val newPerm = {
                    if (chmodParser.isDefined) {
                        new FsPermission(chmodParser.get.applyNewPermission(baseFileStatus))
                    } else {
                        baseFileStatus.getPermission
                    }
                }
                if (targetFileStatus.getPermission != newPerm) {
                    val targetPath = targetFileStatus.getPath
                    if (config.dryRun) {
                        log.info(s"UPDATE_PERMS [dryrun] -  $srcPath --> $targetPath [$newPerm]")
                    } else {
                        log.debug(s"UPDATE_PERMS -  $srcPath --> $targetPath [$newPerm]")
                        config.dstFs.setPermission(targetPath, newPerm)
                    }
                }

        }
    }

    /**
     * Function updating owner/group target if needed. Use src owner/group if no mapping
     * is found, otherwise use mapping value.
     * @param src the src from which to get owner/group
     * @param target the target to update in dst, as either a Left(path) in case of dryrun
     *               (new objects are not existing in dryrun mode, so they are represented as Path
     *               instead of FileStatus), or a Right(FileStatus) when target is available
     *               (we could still be in dryrun mode)
     */
    def updateOwnerAndGroup(src: FileStatus, target: Either[Path, FileStatus]): Unit = {

        def newValue(
            preserve: Boolean,
            srcValue: String,
            dstValue: String,
            mappings: Seq[(Regex, String)]
        ): String = {
            if (preserve) {
                mappings
                    .find { case (p, _) => p.findFirstIn(srcValue).isDefined }
                    .map { case (_, v) => v }
                    .getOrElse(srcValue)
            } else {
                dstValue
            }
        }

        val srcPath = src.getPath
        target match {
            case Left(targetPath) =>
                if (config.dryRun) {
                    log.info(s"UPDATE_OWNER_GROUP [dryrun] -  $srcPath --> $targetPath")
                }
            case Right(targetFileStatus) =>
                val dstOwner = targetFileStatus.getOwner
                val dstGroup = targetFileStatus.getGroup

                val newOwner = newValue(config.preserveOwner, src.getOwner, dstOwner, config.parsedUsermap)
                val newGroup = newValue(config.preserveGroup, src.getGroup, dstGroup, config.parsedGroupmap)

                if (dstOwner != newOwner || dstGroup != newGroup) {
                    val targetPath = targetFileStatus.getPath
                    if (config.dryRun) {
                        log.info(s"UPDATE_OWNER_GROUP [dryrun] -  $srcPath --> $targetPath [$newOwner:$newGroup]")
                    } else {
                        log.debug(s"UPDATE_OWNER_GROUP -  $srcPath --> $targetPath [$newOwner:$newGroup]")
                        config.dstFs.setOwner(targetPath, newOwner, newGroup)
                    }
                }
        }
    }

    def updateMetadataAsNeeded(src: FileStatus, target: Either[Path, FileStatus], isNew: Boolean): Unit = {
        if (config.preserveTimes) {
            preserveModificationTime(src, target)
        }

        val chmodParser = if (src.isDirectory) config.chmodDirs else config.chmodFiles
        if (config.preservePerms || (isNew && chmodParser.isDefined)) {
            updatePerms(src, target, chmodParser)
        }

        if (config.preserveOwner || config.preserveGroup) {
            updateOwnerAndGroup(src, target)
        }
    }


    /********************************************************************************
     * Main processing functions (and helpers)
     *  - Process a single file/directory from src to dst
     *  - Process an entire-directory level by listing and comparing the lists
     */

    /**
     * This alias is meant to facilitate the understanding of where a
     * BasePath is conveyed by opposition to a dst or target (often an
     * Option[Path] as well).
     */
    type BasePath = Option[Path]

    /**
     * Function verifying if an src and an existingTarget are considered different.
     * This is used to decided whether to update an already existing target, or
     * to not touch it (skip).
     *
     * @param src the src to compare
     * @param existingTarget the existing target to compare (not optional)
     * @return true if src and existingTarget are considered different
     */
    private def areDifferent(src: FileStatus, existingTarget: FileStatus): Boolean = {
        config.ignoreTimes ||                                      // Force copy/update OR
            src.getLen != existingTarget.getLen ||                 // Size is different OR
            (!config.sizeOnly &&                                   // (Use modificationTimes AND
                approxCompareTimestamps(src, existingTarget) == 0) // modificationTimes are different)
    }

    /**
     * Function processing a single src file, with its associated target in dst, and its
     * potential existingTarget for reference to the possibly already existing object in dst.
     * The target file will be copied if new, updated if already existing and different,
     * or omitted if already existing and not different (configuration dependent).
     *
     * The returned value will be applied metadata changes (modification-times, permissions
     * and ownership). Some is returned when the file is copied or omitted because no
     * change is needed, None when the file skipped  because of configuration settings.
     *
     * @param src the src FileStatus to serve as base
     * @param target the target for the possibly copied/updated files.
     * @param existingTarget the reference to the already existing file at target
     *                       path in dst, if any.
     * @return Left(Path) of target in case of skip or dryrun (no FileStatus available),
     *         Right(FileStatus) of the processed target otherwise
     */
    private def copyAsNeeded(
        src: FileStatus,
        target: Path,
        existingTarget: Option[FileStatus],
        isNew: Boolean
    ): Option[Either[Path, FileStatus]] = {
        // Skipping facility
        def skip(msgTag: String): Option[Either[Path, FileStatus]] = {
            log.debug(s"SKIP_FILE [$msgTag] - ${src.getPath} --> $target")
            None
        }

        // copy new file if config says so
        if (isNew) {
            if (!config.existing) {
                Some(copy(src, target, isUpdate = false))
            } else {
                skip("existing")
            }
        } else if (areDifferent(src, existingTarget.get)) {
            if (!config.ignoreExisting) {
                // If config.update is true, copy only if src is newer than dst
                if (!config.update || approxCompareTimestamps(src, existingTarget.get) < 0) {
                    Some(copy(src, target, isUpdate = true))
                } else {
                    skip("update")
                }
            } else {
                skip("ignore-existing")
            }
        } else {
            // If no diff, target can be updated so shouldn't be None
            log.debug(s"SAME_FILE - ${src.getPath} --> $target")
            Some(Right(existingTarget.get))
        }
    }

    /**
     * Function processing a single src directory, with its associated target in dst, and its
     * potential existingTarget for reference to the possibly already existing object in dst.
     * If not in recurse mode the directory is skipped, otherwise the target will be created if new,
     * overwritten if already existing as a file, or skipped if already existing as a directory.
     * Note: existing and ignoreExisting flags only apply to files
     *
     * @param src the src FileStatus to serve as base
     * @param target the target for the possibly copied/updated files.
     * @param existingTarget the reference to the already existing file at target
     *                       path in dst, if any.
     * @return Left(Path) of target in case of skip or dryrun (no FileStatus available),
     *         Right(FileStatus) of the processed target otherwise
     */
    private def createAsNeeded(
        src: FileStatus,
        target: Path,
        existingTarget: Option[FileStatus],
        isNew: Boolean
    ): Option[Either[Path, FileStatus]] = {
        // Only update directories in recurse mode (copyDirs mode processed in copyAsNeeded)
        if (config.recurse) {
            if (isNew) {
                Some(createDirectory(target, overwriteFile = false))
            } else if (existingTarget.get.isFile) {
                Some(createDirectory(target, overwriteFile = true))
            } else { // Creation not needed but target to proceed
                log.debug(s"SKIP_DIR - ${src.getPath} --> $target")
                Some(Right(existingTarget.get))
            }
        } else {
            log.debug(s"SKIP_DIR [no-recurse]- ${src.getPath} --> $target")
            None
        }
    }



    /**
     * Function either logging the action to be taken on src if no target,
     * or applying [[copyAsNeeded]] or [[copyAsNeeded]] on existing target.
     * @param src the src FileStatus
     * @param targetOpt the optional target where to process src
     * @param existingTarget the reference to the already existing file at target
     *                       path in dst, if any.
     * @return None if targetOpt is None, or Left(Path) of target in case of skip or dryrun
     *         (no FileStatus available), Right(FileStatus) of the processed target otherwise
     */
    private def processSrc(
        src: FileStatus,
        targetOpt: Option[Path],
        existingTarget: Option[FileStatus]
    ): Option[Either[Path, FileStatus]] = {
        val srcPath = src.getPath
        targetOpt match {
            case None =>
                if (config.copyDirs || src.isFile) {
                    log.info(s"COPY_FILE [no-dst] - $srcPath")
                } else {
                    log.info(s"CREATE_DIR [no-dst] - $srcPath")
                }
                None
            case Some(target) =>
                val isNew = existingTarget.isEmpty
                // Use copy for files or if copyDirs is true
                if (src.isFile || config.copyDirs) {
                    copyAsNeeded(src, target, existingTarget, isNew)
                } else { // directory case
                    createAsNeeded(src, target, existingTarget, isNew)
                }
        }
    }

    /**
     * Function deleting files and directories in dstList that are not present in srcList,
     * if they are not excluded by filter-rules.
     *
     * Note: No need to convey dst BasePath along even if it is needed for filter-rules.
     *       root-of-transfer config.dst being a single folder by construction, it is the
     *       BasePath of every file/folder it contains.
     *
     * @param srcsList the list of direct children (files and/or directories) of the
     *                 parentSrc directory, organized in a Map[filename, Seq(FileStatus, BasePath)]
     * @param dstList the list of direct children (files and/or directories) of the
     *                parentDst directory (if any), organized in a Map[filename, FileStatus]
     *                (empty if no dst)
     */
    private def deleteExtraneousDsts(
        srcsList: Map[String, Seq[(FileStatus, BasePath)]],
        dstList: Map[String, FileStatus]
    ): Unit = {
        dstList.keySet.foreach(dstName => {
            if (! srcsList.contains(dstName)) {
                // Apply filter-rules
                // We use config.parentDst as the root-of-transfer for dst as it never changes (not true for src)
                val dst = dstList(dstName)
                val matchingRule = config.parsedFilterRules.find(rule => rule.matches(dst, config.dstPath))

                if (matchingRule.isDefined && matchingRule.get.ruleType == Exclude() && !config.deleteExcluded) {
                    log.debug(s"EXCLUDE_DST - ${dst.getPath}")
                } else {
                    if (config.dryRun)
                        log.info(s"DELETE_DST [dryrun] - ${dst.getPath}")
                    else {
                        log.debug(s"DELETE_DST - ${dst.getPath}")
                        config.dstFs.delete(dst.getPath, true) // recursive deletion
                    }
                }
            }
        })
    }

    /**
     * Function iterating over a list of src directories paths and listing
     * their content, carrying srcs associated BasePath to their children.
     * Note: each src children is sorted by path to make the processing order
     *       easier to follow.
     *
     * @param parentSrcs the list of parent directory paths with their BasePath
     * @return the flatten list of children from each parent with their associated BasePath
     */
    private def getSrcsChildren(parentSrcs: Seq[(Path, BasePath)]): Seq[(FileStatus, BasePath)] = {
        parentSrcs.flatMap { case (src, basePath) =>
            // Root of the tree
            if (basePath.isEmpty) {
                // use glob to get directory content and set basePath from listed files
                Try(config.srcFs.globStatus(src).toSeq).getOrElse(Seq.empty)
                    .map(s => (s, Some(s.getPath.getParent)))
                    .sortBy(_._1.getPath.toString)

            // Directory accessed through recursion
            } else {
                // use listStatus and keep basePath unchanged
                config.srcFs.listStatus(src)
                    .sortBy(_.getPath.toString)
                    .map((_, basePath))
            }
        }
    }

    /**
     * Function grouping a list of srcs by their filename and keeping a list of the related srcs
     * as value. The order of insertion in the map is retained to process srcs in
     * listed-src-and-path order.
     *
     * @param srcsChildren the list of srcs to group
     * @return the insertion-ordered map of (filename, Seq(src, basePath))
     */
    private def groupSrcsChildren(
        srcsChildren: Seq[(FileStatus, BasePath)]
    ): Map[String, Seq[(FileStatus, BasePath)]] = {

        srcsChildren.foldLeft (ListMap.empty[String, Seq[(FileStatus, BasePath)]])(
            (map, childSrcAndBasePath) => {
                val (childSrc, _) = childSrcAndBasePath
                val childName = childSrc.getPath.getName
                if (!map.contains(childName)) {
                    map + (childName -> Seq(childSrcAndBasePath))
                } else {
                    map + (childName -> (map(childName) :+ childSrcAndBasePath))
                }
            }
        )
    }

    /**
     * Function reordering values of the grouped-srcs-children if needed base on config.
     * The order of the values of the map is naturally the src-parameters one, which is
     * used by default in conflict resolution. If config.useMostRecentModifTimes is set,
     * the values are reordered to reflect the config.
     *
     * @param groupedSrcsChildren the grouped-srcs-children map with values ordered in
     *                            src-parameters order
     * @return the map with values reordered by most-recent-modif-time if config says so
     */
    private def reorderByFilenameSrcsChildrenAsNeeded(
        groupedSrcsChildren: Map[String, Seq[(FileStatus, BasePath)]]
    ): Map[String, Seq[(FileStatus, BasePath)]] ={

        if (config.useMostRecentModifTimes) {
            groupedSrcsChildren.map { case (filename, srcsAndBasePaths) =>
                // Sort by -modifTime to obtain most recent first
                (filename, srcsAndBasePaths.sortBy { case (src, basePath) => - src.getModificationTime })
            }
        } else {
            groupedSrcsChildren
        }
    }

    /**
     * Function listing direct children (files and/or directories) of a list of sources.
     * It maintains for every src its BasePath (root of the transfer) to possibly use it
     * in filter-rules.
     * The listed files and directories are organised in a map keyed by filename, containing
     * Seq(fileStatus, basePath) as values. Values are Seq to correctly handle merging multiple
     * source directories sharing the same name.
     * Note: The map is actually a ListMap to keep the source-parameter insertion order to facilitate
     *       following the processing.
     *
     * @param parentSrcs the sources to scan
     * @return the map of FileStatuses/BasePath sequences keyed by filenames.
     */
    private def getSrcsList(parentSrcs: Seq[(Path, BasePath)]): Map[String, Seq[(FileStatus, BasePath)]] = {

        // Sorted files list from every source of the list
        val srcsChildren = getSrcsChildren(parentSrcs)

        // Group by filename keeping source order in OrderedMap (for pretty printing)
        val groupedSrcsChildren = groupSrcsChildren(srcsChildren)

        // Sort each child file srcs-list by modificationTime if useMostRecentModifTimes.
        // Otherwise keep src-parameters order (valid both for useFirstInSrcList or conflict failure)
        reorderByFilenameSrcsChildrenAsNeeded(groupedSrcsChildren)
    }

    /**
     * Function listing direct children (files and/or directories) of a dst.
     * Result is organised as a Map(filename, filesStatus)
     * @param dst the dst path to scan
     * @return the map of FileStatuses keyed by filenames.
     */
    private def getDstList(dst: Option[Path]): Map[String, FileStatus] = {
        dst.toSeq.flatMap(path => {
            // We need this check that dst exists since in dryrun mode
            // dst dirs are not created but we still recurse into their
            // src counterpart.
            if (config.dstFs.exists(path)) {
                config.dstFs.listStatus(path).map(f => (f.getPath.getName, f))
            } else {
                Seq.empty
            }
        }).toMap
    }

    /**
     * Function filtering a list of sources belonging to the same target, each coming
     * with its BasePath. The filtering is done by applying filter-rules defined in configuration.
     *
     * @param srcs the list of (FileStatus, BasePath) to filter
     * @return the filtered list
     */
    private def applyFilterRules(srcs: Seq[(FileStatus, BasePath)]): Seq[(FileStatus, BasePath)] = {
        srcs.filter { case (src, basePath) =>
            val matchingRule = config.parsedFilterRules.find(rule => rule.matches(src, basePath.get))
            if (matchingRule.isDefined && matchingRule.get.ruleType == Exclude()) {
                log.debug(s"EXCLUDE_SRC - ${src.getPath}")
                false
            } else {
                true
            }
        }
    }

    /**
     * Function deleting an existing dst path directory if it is empty
     *
     * @param dstOpt the optional dst directory path
     */
    private def pruneEmptyAsNeeded(dstOpt: Option[Path]): Unit = {
        dstOpt.foreach(dst => {
            if (config.pruneEmptyDirs && config.dstFs.listStatus(dst).isEmpty) {
                log.debug(s"PRUNE_DIR - $dst")
                config.dstFs.delete(dst, true)
            }
        })
    }

    /**
     * This function handles src-conflict resolution based on srcs to process and provided config.
     * It filters the srcs list accordingly to config flags and provides it to [[processSrc]].
     *
     * If config.recurse, srcs should all be directories or config.resolveConflicts should be set
     * If srcs contains a signle element, no conflit, process the single element
     * If config.resolveConflicts, use the first element of srcs even if not alone. In that case
     * srcs order is important and is defined as parameter-list-src by default, or
     * most-recent-modif-time if config.useMostRecentModifTimes is set.
     *
     * If srcs contains conflicts and config.resolveConflicts is not set,
     * an [[IllegalStateException]] is thrown.
     *
     * @param srcs the list of files sharing the same filename (and therefore target).
     *             Note that each src also keep track of its BasePath (usefull for filter-rules)
     * @param targetOpt the target of the directorie(s) or file (if any).
     * @param existingTarget the reference to the already existing object at target path.
     */
    private def processSrcs(
        srcs: Seq[(FileStatus, BasePath)],
        targetOpt: Option[Path],
        existingTarget: Option[FileStatus]
    ): Unit = {
        val allDirs =  srcs.forall { case (s, _) => s.isDirectory }

        if ((config.recurse && allDirs) || srcs.size == 1 || config.resolveConflicts) {
            val (src, _) = srcs.head

            val targetToUpdateOpt = processSrc(src, targetOpt, existingTarget)

            // Recursion is needed BEFORE applying metadata changes,
            // for modificationTime not being overwritten
            if (src.isDirectory && config.recurse) {
                val directories = if (allDirs) srcs else Seq(srcs.head)
                applyRecursive(directories.map { case (fs, bp) => (fs.getPath, bp) }, targetOpt)
                pruneEmptyAsNeeded(targetOpt)
            }

            // Only update metadata of defined target
            targetToUpdateOpt.foreach(targetToUpdate => {
                updateMetadataAsNeeded(src, targetToUpdate, isNew = existingTarget.isEmpty)
            })

        } else {
            throw new IllegalStateException(
                "SRC_CONFLICT - Trying to copy multiple objects with the same filename at the same destination")
        }
    }

    /**
     * Function applying rsync at a directory-tree level.
     * It lists content from src and dst, applies dst extraneous deletion (if any),
     * and calls the [[processSrcs]] function to handle
     * filename-coherent src lists.
     *
     * @param parentSrcs the parent srcs list from which content is to be rsynced
     * @param parentDst the parent dst to rsync into
     */
    private def applyRecursive(
        parentSrcs: Seq[(Path, BasePath)],
        parentDst: Option[Path]
    ): Unit = {
        val srcsList = getSrcsList(parentSrcs)
        val dstList = getDstList(parentDst)

        // Clean the parentDst directory from files not in parentSrc if specified in config
        if (config.deleteExtraneous) {
            deleteExtraneousDsts(srcsList, dstList)
        }

        // Update dst from src (create new folders, copy new files, update existing etc)
        srcsList.keySet.foreach(srcName => {
            // Srcs contains possibly multiple sources for a single destination
            // (same directory relatively to the transfer root, same filename)
            val srcs = srcsList(srcName)

            // ExistingTarget is the reference to the possibly already existing
            // file/directory in dst with filename. It will be used to check
            // for file equivalence and prevent copying.
            val existingTarget = dstList.get(srcName)

            // Target is the reference to the object that needs to be created/copied/updated.
            // If existingTarget exists, target is redundant. However if the is no existingTarget
            // the target is our reference to the new object to be.
            val target = parentDst.map(d => new Path(s"$d/$srcName"))

            // Apply filter-rules to srcs
            val filteredSrcList = applyFilterRules(srcs)

            // Process the reminder of srcs not filtered out by rules, with target and
            // existingTarget
            if (filteredSrcList.nonEmpty) {
                processSrcs(filteredSrcList, target, existingTarget)
            }
        })
    }

    /**
     * Main execution function.
     * Applies the recursive function on src and dst as defined in config
     */
    def apply(): Unit = {
        applyRecursive(config.srcPathsList.map(p => (p, None)), config.dst.map(_ => config.dstPath))
    }

}
