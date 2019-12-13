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
 * As a convention, we use dst to name the parent folder of a copy,
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
     * Functions for files/folders copy / update / skip
     */

    /**
     * This function is called when a folder from src is not yet present in dst.
     *
     * @param target the dst folder path to create
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
     *
     * @param src the source file FileStatus for the copy/creation
     * @param target the dst path of the newly created object (should contain the same filename as src)
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
     * The update is a full overwriting copy from src to target.
     *
     * @param src the source file FileStatus for the update
     * @param target the dst path of the updated object (should contain the same filename as src)
     * @return Left(Path) in case of dryrun (no FileStatus available), Right(FileStatus) otherwise.
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
     * @param src the skipped source file FileStatus
     * @param target the skipped dst path
     * @return Right(FileStatus) as skip means the target exists
     */
    private def skipFile(src: FileStatus, target: Path): Either[Path, FileStatus] = {
        log.debug(s"SKIP_FILE - ${src.getPath} --> $target")
        Right(config.dstFs.getFileStatus(target))
    }


    /********************************************************************************
     * Functions for files/folders metadata updates (permissions / modification time)
     */


    /**
     * This function checks whether modificationTimestamp of src and dst can be considered
     * different in regard of the approximation defined in config.
     * We need to accept approximation in modificationTime as different filesystem precisions
     * lead to inappropriate inequality and therefore to more data transfer.
     *
     * @param src the src Filestatus to check
     * @param target the target Filestatus to check
     * @return true if the modificationTimestamps are different by more than the accepted approximation
     */
    private def approxDiffTimestamps(src: FileStatus, target: FileStatus): Boolean = {
        Math.abs(src.getModificationTime - target.getModificationTime) >= config.acceptedTimesDiffMs
    }

    /**
     * This function is called to update modificationTime of the target object.
     * Target modificationTime is given the same value as the src one (times option).
     *
     * @param src the src FileStatus from which to get modificationTimestamp
     * @param target The dst target to update, as either a Left(path) in case of dryrun (new file
     *               not acutally copied, so no FileStatus available), or a Right(FileStatus)
     *               when available (could be dryrun nonetheless).
     */
    private def preserveModificationTimes(src: FileStatus, target: Either[Path, FileStatus]): Unit = {
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
     *  - src as a base permission in case of perms preservation, or already existing target
     *    permissions otherwise
     *  - the files or dirs ChmodParser to update the base permissions depending on file type
     *
     * @param src the src FileStatus from which to get base permissions if needed
     * @param target The dst target to update, as either a Left(path) in case of dryrun (new file
     *               not acutally copied, so no FileStatus available), or a Right(FileStatus)
     *               when available (could be dryrun nonetheless).
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
     * This function is called when a folder's permissions need to be updated.
     * Because more than one src folder can be merged into a target one, we don't preserve perms.
     * We nonetheless apply a ChmodParser.
     *
     * @param target The dst target to update, as either a Left(path) in case of dryrun (new folder
     *               not actually created, so no FileStatus available), or a Right(FileStatus)
     *               when available (could be dryrun nonetheless).
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
     *  - Process a single pair src/dst
     *  - Process a folder-tree level
     */

    /**
     * Function processing a single file from src, target and foundTarget.
     * The src file can be either copied if new, updated if already existing and different,
     * or skipped if already exisitng and not different based on settings.
     * Permissions and modificationTimes are then updated depending on configuration.
     *
     * @param src the src FileStatus to serve as base
     * @param target the target where to copy/create new object if necessary (if any)
     * @param foundTarget the found target in dst (same filename as src) if any
     * @return Some path if dst was defined (can be a non-existent file in case of dryrun),
     *         None if no dst was provided (logs only).
     */
    private def processFile(
        src: FileStatus,
        target: Option[Path],
        foundTarget: Option[FileStatus]
    ): Unit = {
        // If no destination is provided, just log the file to copy
        if (target.isEmpty) {
            log.info(s"COPY_FILE [no-dst] - ${src.getPath}")
        } else {
            val isNew = foundTarget.isEmpty
            val targetToUpdate = {
                if (isNew) {
                    copyNewFile(src, target.get)
                } else if (config.ignoreTimes || // Force copy/update
                    src.getLen != foundTarget.get.getLen || // Size is different
                    (!config.sizeOnly &&                    // Use modificationTime
                        approxDiffTimestamps(src, foundTarget.get))) {
                    updateFile(src, target.get)
                } else {
                    skipFile(src, target.get)
                }
            }
            if (config.preserveTimes) {
                preserveModificationTimes(src, targetToUpdate)
            }
            // Update perms if needed
            if (config.preservePerms || (isNew && (isNew && config.chmodFiles.isDefined))) {
                updateFilePerms(src, targetToUpdate, config.chmodFiles)
            }
        }
    }

    /**
     * Function processing a single folder with a target and foundTarget.
     * The target folder is created if it doesn't exist, and its permissions are
     * updated depending on configuration.
     *
     * @param target the target where to create the new folderif necessary (if any)
     * @param foundTarget the found target in dst (same filename as src) if any
     * @return Some path if dst was defined (can be a non-existent file in case of dryrun),
     *         None if no dst was provided (logs only).
     */
    private def processDir(
        target: Option[Path],
        foundTarget: Option[FileStatus]
    ): Unit = {
        // Don't log dir info if no dest provided, only file
        if (target.isDefined) {
            val isNew = foundTarget.isEmpty
            val targetToUpdate = {
                if (isNew) {
                    createDir(target.get)
                } else {
                    Right(foundTarget.get)
                }
            }
            if (config.chmodDirs.isDefined && (config.preservePerms || isNew )) {
                updateDirPerms(targetToUpdate, config.chmodDirs.get)
            }
        }
    }


    /**
     * Function deleting files and folders in dst that are not present in src, if config says so.
     *
     * Note: No need to pass dstBasePath (needed for filter-rules), it never changes for dst,
     *       it always is config.dst.
     *
     * @param srcFilesAndFolders the list of files and folders in the worked src folder,
     *                           as a Map[filename, FileStatus]
     * @param dstFilesAndFolders the list of files and folders in the worked dst folder,
     *                           as a Map[filename, FileStatus]
     */
    private def deleteExtraneousDstAsNeeded(
        srcFilesAndFolders: Map[String, Seq[(FileStatus, Option[Path])]],
        dstFilesAndFolders: Map[String, FileStatus]
    ): Unit = {
        if (config.deleteExtraneous) {
            dstFilesAndFolders.keySet.foreach(dstName => {
                if (! srcFilesAndFolders.contains(dstName)) {
                    // Apply filter-rules
                    // We use config.dstPath as the root-of-transfer for dst as it never changes (not true for src)
                    val dst = dstFilesAndFolders(dstName)
                    val matchingRule = config.parsedFilterRules.find(rule => rule.matches(dst, config.dstPath))

                    if (matchingRule.isDefined && matchingRule.get.ruleType == Exclude() && !config.deleteExcluded) {
                        log.debug(s"EXCLUDE_DST - ${dst.getPath}")
                    } else {
                        if (config.dryRun)
                            log.info(s"DELETE_DST [dryrun] - ${dst.getPath}")
                        else {
                            log.debug(s"DELETE_DST - ${dst.getPath}")
                            config.dstFs.delete(dst.getPath, true) // delete folders recursively
                        }
                    }
                }
            })
        }
    }

    /**
     * Function listing files and folders from a list of sources.
     * It maintains for every file its basePath (root of the transfer)
     * to possibly use it in filter-rules.
     * The found files and folders are orgnised in a map keyed by filename, containing
     * Seq(filestatus, basePath) as values. We need sequences as values to correctly
     * handing merging sources folder when they have the same name.
     *
     * @param srcPathList the list of sources to scan
     * @return the map of filestatuses and basedPath sequences keyed by filenames.
     */
    private def getSrcFilesAndFolders(
        srcPathList: Seq[(Path, Option[Path])]
    ): Map[String, Seq[(FileStatus, Option[Path])]] = {

        srcPathList.flatMap((srcAndBasePath: (Path, Option[Path])) => {
            val (src, basePath) = srcAndBasePath
            // Root of the tree, use glob to get folder content and set basePath
            // from listed files
            if (basePath.isEmpty) {
                Try(config.srcFs.globStatus(src).toSeq).getOrElse(Seq.empty)
                    .map(s => (s, Some(s.getPath.getParent)))
            } else {
                // In a recursion folder, use listStatus and keep basePath unchanged
                config.srcFs.listStatus(src).map(s => (s, basePath))
            }
        }).groupBy { case (fileStatus, basePath) => fileStatus.getPath.getName }
    }

    /**
     * Function listing the files and folders from a dstPath. Result is organised in a
     * map of (filename, filesStatus)
     * @param dstPath the dst path to scan
     * @return the map of fileStatuses keyed by filenames.
     */
    private def getDstFilesAndFolders(dstPath: Option[Path]): Map[String, FileStatus] = {
        dstPath.toSeq.flatMap(path => {
            // We need this check that dst exists for dryrun mode
            // as dst dirs are not created as we recurse into them
            if (config.dstFs.exists(path)) {
                config.dstFs.listStatus(path).map(f => (f.getPath.getName, f))
            } else {
                Seq.empty
            }
        }).toMap
    }

    /**
     * Function filtering a list of source and basePath by applying filter-rules defined in configuration.
     *
     * @param srcList the list of (FileStatus, basePath) to filer
     * @return the filtered list
     */
    private def applyFilterRules(srcList: Seq[(FileStatus, Option[Path])]): Seq[(FileStatus, Option[Path])] = {
        srcList.filter { case (src, basePath) =>
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
     * This function either processes a folder into target and recurse if all sources are
     * directories and the recursion flag is on (directories are skipped if flag is off),
     * or processes a file if it is alone in its list.
     * If the list contains incoherent objects (both files and folders, or multiple files),
     * an [[IllegalStateException]] is thrown.
     *
     * @param srcList the srcList of files sharing the same filename and target.
     * @param target the target were to merge folders or copy/update files (if any).
     * @param foundTarget the found target in dst (if any)
     */
    def mergeOrProcessCoherentSrcList(
        srcList: Seq[(FileStatus, Option[Path])],
        target: Option[Path],
        foundTarget: Option[FileStatus]
    ): Unit = {
        // If srcList contains only directories
        if (srcList.forall { case (s, _) => s.isDirectory }) {
            if (config.recurse) {
                // Create directory if it doesn't exist and recurse
                processDir(target, foundTarget)
                applyRecursive(srcList.map { case (f, basePath) => (f.getPath, basePath)}, target)
            } else {
                // Skip folders if recursion is off
                srcList.foreach {case (f, _) => log.debug(s"SKIP_DIR - ${f.getPath}")}
            }
        // if srcList contains only files
        } else if (srcList.forall { case (f, _) => f.isFile }) {
            if (srcList.size == 1) {
                // process the file if it is alone in the list
                val (src, _) = srcList.head
                processFile(src, target, foundTarget)
            } else {
                throw new IllegalStateException("Trying to copy multiple files with the same name at the same destination")
            }
        } else {
            throw new IllegalStateException("Trying to copy both files and folders with the same name at the same destination")
        }
    }

    /**
     * Function applying rsync at a folder-tree level.
     * It lists content from src and dst, applies dst extraneous deletion (if any),
     * and calls the [[mergeOrProcessCoherentSrcList]] function to handle
     * filename-coherent src lists.
     *
     * @param srcPathList the src path list to rsync
     * @param dstPath the dst path to rsync to
     */
    private def applyRecursive(
        srcPathList: Seq[(Path, Option[Path])],
        dstPath: Option[Path]
    ): Unit = {
        val srcFilesAndFolders = getSrcFilesAndFolders(srcPathList)
        val dstFilesAndFolders = getDstFilesAndFolders(dstPath)

        // First clean the dst directory from files not in src (if delete flag and rules allow)
        deleteExtraneousDstAsNeeded(srcFilesAndFolders, dstFilesAndFolders)

        // Then update dst from src
        srcFilesAndFolders.keySet.foreach(srcName => {
            val srcList = srcFilesAndFolders(srcName)
            val foundTarget = dstFilesAndFolders.get(srcName)
            val target = dstPath.map(d => new Path(s"$d/$srcName"))

            // Apply filter-rules
            val filteredSrcList = applyFilterRules(srcList)

            if (filteredSrcList.nonEmpty) {
                mergeOrProcessCoherentSrcList(filteredSrcList, target, foundTarget)
            }
        })
    }

    /**
     * Main execution function.
     * Applies the recursive function on src and dst as defined in config
     */
    def apply(): Unit = {
        applyRecursive(config.srcPathList.map(p => (p, None)), config.dst.map(_ => config.dstPath))
    }

}
