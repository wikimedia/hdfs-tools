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
import org.apache.log4j.{ConsoleAppender, Logger, PatternLayout}

import scala.util.Try


/**
 * Class executing rsync given the provided config
 *
 * As a convention, we use dst to name the parent directories of a copy,
 * and target the exact target of a copy in dst (with same filename part)
 */
class HdfsRsyncExec(config: HdfsRsyncConfig) {

    /**
     * Logging initialization
     *  - Use level from config
     *  - Use console-logging if no config appender is defined
     */
    val log: Logger = {
        val l = Logger.getLogger(this.getClass)
        l.setLevel(config.logLevel)
        if (config.logAppender.isEmpty) {
            l.addAppender(
                new ConsoleAppender(
                    new PatternLayout("%d{yyyy-MM-dd'T'HH:mm:ss.SSS} %p %c{1} %m%n"), ConsoleAppender.SYSTEM_OUT))
        } else {
            config.logAppender.foreach(appender => l.addAppender(appender))
        }
        l
    }


    /********************************************************************************
     * Functions for files/dirs copy / update / skip
     */

    /**
     * This function is called when a directory from src is not yet present in dst.
     * the directory is created at target path in dst.
     *
     * @param target the dst directory path to create
     * @return Left(Path) in case of dryrun (no FileStatus available), Right(FileStatus) otherwise
     */
    private def createDir(target: Path): Either[Path, FileStatus] = {
        if (config.dryRun) {
            log.info(s"CREATE_DIR [dryrun] - $target")
            Left(target)
        } else {
            log.debug(s"CREATE_DIR - $target")
            config.dstFs.mkdirs(target)
            Right(config.dstFs.getFileStatus(target))
        }
    }

    /**
     * This function is called when a source file is not yet present in dst.
     * The src file is copied to target path in dst.
     *
     * @param src the source file to copy
     * @param target the target in dst for the copied src file
     * @return Left(Path) in case of dryrun (no FileStatus available), Right(FileStatus) otherwise.
     */
    private def copyNewFile(src: FileStatus, target: Path): Either[Path, FileStatus] = {
        if (config.dryRun) {
            log.info(s"COPY_FILE [dryrun] - ${src.getPath} --> $target")
            Left(target)
        } else {
            log.debug(s"COPY_FILE - ${src.getPath} --> $target")
            FileUtil.copy(config.srcFs, src.getPath, config.dstFs, target, false, true, config.hadoopConf)
            Right(config.dstFs.getFileStatus(target))
        }
    }

    /**
     * This function is called when a source file is present in dst and needs to be updated.
     * The update is a full overwriting copy of src to target.
     *
     * @param src the source file for the update
     * @param target the target in dst for the copied src file.
     * @return Left(Path) in case of dryrun (no up-to-date FileStatus available),
     *         Right(FileStatus) otherwise.
     */
    private def updateFile(src: FileStatus, target: Path): Either[Path, FileStatus] = {
        if (config.dryRun) {
            log.info(s"UPDATE_FILE [dryrun] - ${src.getPath} --> $target")
            Left(target)
        } else {
            log.debug(s"UPDATE_FILE - ${src.getPath} --> $target")
            FileUtil.copy(config.srcFs, src.getPath, config.dstFs, target, false, true, config.hadoopConf)
            Right(config.dstFs.getFileStatus(target))
        }
    }

    /**
     * This function is called when a source file present in dst is skipped (considered
     * not changed, depending on configuration). This function only logs.
     *
     * @param src the skipped source file
     * @param target the skipped target in dst
     * @return Right(FileStatus) as skip implies that the target exists
     */
    private def skipFile(src: FileStatus, target: Path): Either[Path, FileStatus] = {
        log.debug(s"SKIP_FILE - ${src.getPath} --> $target")
        Right(config.dstFs.getFileStatus(target))
    }


    /********************************************************************************
     * Functions for files/directories metadata updates (permissions / modification time)
     */

    /**
     * This function checks whether modificationTimestamp of src and dst can be considered
     * different in regard of the approximation defined in config.
     * We need to accept approximation in modificationTime as different filesystem precisions
     * lead to inappropriate inequality and therefore to more data transfer.
     *
     * @param src the src to check modificationTimestamp
     * @param target the target to check modificationTimestamp
     * @return true if the modificationTimestamps are different by more than the accepted approximation
     */
    private def approxDiffTimestamps(src: FileStatus, target: FileStatus): Boolean = {
        Math.abs(src.getModificationTime - target.getModificationTime) >= config.acceptedTimesDiffMs
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
        if (config.dryRun && target.isLeft) {
            log.info(s"UPDATE_TIMES [dryrun] - ${src.getPath} --> ${target.left.get}")
        } else {
            //  Always in Right() case, dryrun-left already checked, and no dryrun means right
            val targetFileStatus = target.right.get
            if (approxDiffTimestamps(src, targetFileStatus)) {
                if (config.dryRun) {
                    log.info(s"UPDATE_TIMES [dryrun] - ${src.getPath} --> ${target.right.get.getPath}")
                } else {
                    log.debug(s"UPDATE_TIMES - ${src.getPath} --> ${target.right.get.getPath}")
                    config.dstFs.setTimes(targetFileStatus.getPath, src.getModificationTime, -1)
                }
            }
        }
    }

    /**
     * This function is called when a file's permissions need to be updated.
     * It applies the permission update using:
     *  - src permissions as a base in case of perms preservation (currently existing
     *    permissions otherwise)
     *  - the files ChmodParser to update the base permissions
     *
     * @param src the src from which to get base permissions if needed
     * @param target the target to update in dst, as either a Left(path) in case of dryrun
     *               (new file are not copied in dryrun mode, so they are represented as Path
     *               instead of FileStatus), or a Right(FileStatus) when target is available
     *               (we could still be in dryrun mode).
     * @param chmodParser the ChmodParser to use to update permissions (if any)
     */
    private def updateFilePerms(src: FileStatus, target: Either[Path, FileStatus], chmodParser: Option[ChmodParser]): Unit = {
        if (config.dryRun && target.isLeft) {
            log.info(s"UPDATE_FILE_PERMS [dryrun] -  ${src.getPath} --> ${target.left.get}")
        } else { //  Always in  Right() case, dryrun-left already checked, and no dryrun means right
        val targetFileStatus = target.right.get
            val baseFileStatus = if (config.preservePerms) src else targetFileStatus
            val newPerm = {
                if (chmodParser.isDefined) {
                    new FsPermission(chmodParser.get.applyNewPermission(baseFileStatus))
                } else {
                    baseFileStatus.getPermission
                }
            }
            if (targetFileStatus.getPermission != newPerm) {
                if (config.dryRun) {
                    log.info(s"UPDATE_FILE_PERMS [dryrun] -  ${src.getPath} --> ${target.right.get.getPath}")
                } else {
                    log.debug(s"UPDATE_FILE_PERMS -  ${src.getPath} --> ${target.right.get.getPath}")
                    config.dstFs.setPermission(targetFileStatus.getPath, newPerm)
                }
            }
        }
    }

    /**
     * This function is called when a directories's permissions need to be updated.
     * Because more than one src directories can be merged into a target one, we don't
     * preserve perms but we apply a ChmodParser.
     *
     * @param target the target to update in dst, as either a Left(path) in case of dryrun
     *               (new directories are not created in dryrun mode, so we represent them as
     *               Path instead of FileStatus), or a Right(FileStatus) when the target is
     *               available (we could still be in dryrun mode).
     * @param chmodParser the ChmodParser to use to update permissions
     */
    private def updateDirPerms(target: Either[Path, FileStatus], chmodParser: ChmodParser): Unit = {
        if (config.dryRun && target.isLeft) {
            log.info(s"UPDATE_DIR_PERMS [dryrun] - ${target.left.get}")
        } else { //  Always in  Right() case, dryrun-left already checked, and no dryrun means right
        val targetFileStatus = target.right.get
            val newPerm = new FsPermission(chmodParser.applyNewPermission(targetFileStatus))
            if (targetFileStatus.getPermission != newPerm) {
                if (config.dryRun) {
                    log.info(s"UPDATE_DIR_PERMS [dryrun] -  ${target.right.get.getPath}")
                } else {
                    log.debug(s"UPDATE_DIR_PERMS -  ${target.right.get.getPath}")
                    config.dstFs.setPermission(targetFileStatus.getPath, newPerm)
                }
            }
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
        config.ignoreTimes ||                              // Force copy/update OR
            src.getLen != existingTarget.getLen ||         // Size is different OR
            (!config.sizeOnly &&                           // (Use modificationTimes AND
                approxDiffTimestamps(src, existingTarget)) //     modificationTimes are different)
    }

    /**
     * Function processing a single src file, with its associated target in dst, and
     * potential existingTarget for reference to the possibly already existing object in dst.
     * The src file will be copied if new, updated if already existing and different,
     * or skipped if already existing and not different (configuration dependent).
     * Once target is up-to-date with src, permissions and modificationTimes are
     * updated if configuration says so.
     *
     * @param src the src FileStatus to serve as base
     * @param target the target for the possibly copied/updated files.
     * @param existingTarget the reference to the already existing file at target
     *                       path in dst, if any.
     */
    private def processFile(
        src: FileStatus,
        target: Option[Path],
        existingTarget: Option[FileStatus]
    ): Unit = {
        // If no target is provided, just log
        if (target.isEmpty) {
            log.info(s"COPY_FILE [no-dst] - ${src.getPath}")
        } else {
            val isNew = existingTarget.isEmpty
            val targetToUpdate = {
                if (isNew) {
                    copyNewFile(src, target.get)
                } else if (areDifferent(src, existingTarget.get)) {
                    updateFile(src, target.get)
                } else {
                    skipFile(src, target.get)
                }
            }
            if (config.preserveTimes) {
                preserveModificationTime(src, targetToUpdate)
            }
            // Update perms if needed
            if (config.preservePerms || (isNew && (isNew && config.chmodFiles.isDefined))) {
                updateFilePerms(src, targetToUpdate, config.chmodFiles)
            }
        }
    }

    /**
     * Function processing a single directory with a target and existingTarget.
     * The target directory is created if it doesn't exist (existingTarget is undefined),
     * and the permissions are updated depending on configuration.
     *
     * @param target the target for the possibly newly created directory
     * @param existingTarget the reference to the already existing directory
     *                       at target path in dst, if any.
     */
    private def processDir(
        target: Option[Path],
        existingTarget: Option[FileStatus]
    ): Unit = {
        // Don't log directory info if no target is provided, we only log for files
        if (target.isDefined) {
            val isNew = existingTarget.isEmpty
            val targetToUpdate = {
                if (isNew) {
                    createDir(target.get)
                } else {
                    Right(existingTarget.get)
                }
            }
            if (config.chmodDirs.isDefined && (config.preservePerms || isNew )) {
                updateDirPerms(targetToUpdate, config.chmodDirs.get)
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
     * Function listing direct children (files and/or directories) of a list of sources.
     * It maintains for every src its BasePath (root of the transfer) to possibly use it
     * in filter-rules.
     * The listed files and directories are organised in a map keyed by filename, containing
     * Seq(fileStatus, basePath) as values. Values are Seq to correctly handle merging multiple
     * source directories sharing the same name.
     *
     * @param srcs the sources to scan
     * @return the map of FileStatuses/BasePath sequences keyed by filenames.
     */
    private def getSrcsList(srcs: Seq[(Path, BasePath)]): Map[String, Seq[(FileStatus, BasePath)]] = {

        srcs.flatMap((srcAndBasePath: (Path, Option[Path])) => {
            val (src, basePath) = srcAndBasePath
            // Root of the tree
            // use glob to get directory content and set basePath from listed files
            if (basePath.isEmpty) {
                Try(config.srcFs.globStatus(src).toSeq).getOrElse(Seq.empty)
                    .map(s => (s, Some(s.getPath.getParent)))
            } else {
                // Directory accessed through recusion
                // use listStatus and keep basePath unchanged
                config.srcFs.listStatus(src).map(s => (s, basePath))
            }
        }).groupBy { case (fileStatus, basePath) => fileStatus.getPath.getName }
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
     * This function either processes a directory (create if needed, recurse if flag is set and
     * all sources are directories), or processes a file if it is alone in its list.
     * If the list contains incoherent objects (both files and directories, or multiple files),
     * an [[IllegalStateException]] is thrown.
     *
     * @param srcs the list of files sharing the same filename (and therefore target).
     *             Note that each src also keep track of its BasePath (usefull for filter-rules)
     * @param target the target of the directorie(s) or file (if any).
     * @param existingTarget the reference to the already existing object at target path.
     */
    private def mergeOrProcessCoherentSrcsList(
        srcs: Seq[(FileStatus, BasePath)],
        target: Option[Path],
        existingTarget: Option[FileStatus]
    ): Unit = {
        // If srcs contains only directories, merge them into target (create target if needed,
        // and recurse into srcs content to merge it into target)
        if (srcs.forall { case (s, _) => s.isDirectory }) {
            if (config.recurse) {
                // Create directory if it doesn't exist, then recurse
                processDir(target, existingTarget)
                applyRecursive(srcs.map { case (f, basePath) => (f.getPath, basePath)}, target)
            } else {
                // Skip directories if recursion is off
                srcs.foreach {case (f, _) => log.debug(s"SKIP_DIR - ${f.getPath}")}
            }
        // if srcs contains only a single file
        } else if (srcs.forall { case (f, _) => f.isFile }) {
            if (srcs.size == 1) {
                // process the file if it is alone in the list
                val (src, _) = srcs.head
                processFile(src, target, existingTarget)
            } else {
                throw new IllegalStateException("Trying to copy multiple files with the same name at the same destination")
            }
        } else {
            throw new IllegalStateException("Trying to copy both files and directories with the same name at the same destination")
        }
    }

    /**
     * Function applying rsync at a directory-tree level.
     * It lists content from src and dst, applies dst extraneous deletion (if any),
     * and calls the [[mergeOrProcessCoherentSrcsList]] function to handle
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
                mergeOrProcessCoherentSrcsList(filteredSrcList, target, existingTarget)
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
