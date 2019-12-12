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
        //
        if (config.logAppender.isEmpty) {
            l.addAppender(
                new ConsoleAppender(
                    new PatternLayout("%d{yyyy-MM-dd'T'HH:mm:ss.SSS} %p %c{1} %m%n"), ConsoleAppender.SYSTEM_OUT))
        } else {
            config.logAppender.foreach(appender => l.addAppender(appender))
        }

        l
    }

    //*******************************************
    // Functions for files/folders copy / update / skip
    //*******************************************

    /**
     * This function is called when a source file/folder is not yet present in dst.
     * If the copy parameter is set to true, a copy is made from src to target, otherwise
     * a folder is created in dst at target.
     * We need to differentiate those cases as copying a folder also copies its content,
     * and we don't want that when doing recursion into folders.
     *
     * @param src the source file/folder FileStatus for the copy/creation
     * @param target the dst path of the newly created object (should contain the same filename as src)
     * @param copy whether to copy a file or folder or create an empty folder
     * @return Left(Path) in case of dryrun (no FileStatus available), Right(FileStatus) otherwise.
     */
    private def createOrCopyNew(src: FileStatus, target: Path, copy: Boolean): Either[Path, FileStatus] = {
        if (config.dryRun) {
            if (copy) {
                log.info(s"COPY_FILE [dryrun] - ${src.getPath} --> $target")
            } else {
                log.info(s"CREATE_FOLDER [dryrun] - ${src.getPath} --> $target")
            }
            Left(target)
        } else {
            if (copy) {
                log.debug(s"COPY_FILE - ${src.getPath} --> $target")
                FileUtil.copy(config.srcFs, src.getPath, config.dstFs, target, false, true, config.hadoopConf)
            } else {
                log.debug(s"CREATE_FOLDER - ${src.getPath} --> $target")
                config.dstFs.mkdirs(target)
            }
            Right(config.dstFs.getFileStatus(target))
        }
    }

    /**
     * This function is called when a source file/folder is present in dst and needs to be updated.
     * If the copy parameter is set to true, an overwriting copy is made from src to target is made,
     * otherwise the dst target object is just touched (modificationTime update).
     * We need to differentiate those cases as copying a folder also copies its content,
     * and we don't want that when doing recursion into folders
     *
     * @param src the source file/folder FileStatus for the update
     * @param target the dst path of the updated object (should contain the same filename as src)
     * @param copy whether to copy a file or folder or touch a folder
     * @return Left(Path) in case of dryrun (no FileStatus available), Right(FileStatus) otherwise.
     */
    private def copyUpdate(src: FileStatus, target: Path, copy: Boolean): Either[Path, FileStatus] = {
        if (config.dryRun) {
            if (copy) {
                log.info(s"UPDATE_FILE [dryrun] - ${src.getPath} --> $target")
            } else {
                log.info(s"UPDATE_FOLDER [dryrun] - ${src.getPath} --> $target")
            }
            Left(target)
        } else {
            if (copy) {
                log.debug(s"UPDATE_FILE - ${src.getPath} --> $target")
                FileUtil.copy(config.srcFs, src.getPath, config.dstFs, target, false, true, config.hadoopConf)
            } else {
                log.debug(s"UPDATE_FOLDER - ${src.getPath} --> $target")
                config.dstFs.setTimes(target, System.currentTimeMillis(), -1)
            }
            Right(config.dstFs.getFileStatus(target))
        }
    }

    /**
     * This function is called when a source file/folder is present in dst is skipped (considered
     * not changed, depending on configuration). This function only logs.
     *
     * @param src the skipped source file/folder FileStatus
     * @param target the skipped dst path
     * @param copy whether the action would have been copy or not
     * @return Right(FileStatus) as skip means the target exists
     */
    private def skip(src: FileStatus, target: Path, copy: Boolean): Either[Path, FileStatus] = {
        if (copy) {
            log.debug(s"SKIP_FILE - ${src.getPath} --> $target")
        } else {
            log.debug(s"SKIP_FOLDER - ${src.getPath} --> $target")
        }
        Right(config.dstFs.getFileStatus(target))
    }

    //*******************************************
    // Functions for files/folders metadata updates (permissions / modification time)
    //*******************************************

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
     * This function is called to update or not modificationTime of the target object.
     * If an update is needed, target modificationTime is given the same value as the src one (times option).
     *
     * @param src the src FileStatus from which to get modificationTimestamp
     * @param target The dst target to update, as either a Left(path) in case of dryrun (new file
     *               not acutally copied, so no FileStatus available), or a Right(FileStatus)
     *               when available (could be dryrun nonetheless).
     */
    private def preserveModificationTimesAsNeeded(src: FileStatus, target: Either[Path, FileStatus]): Unit = {
        if (config.preserveTimes)
            if (config.dryRun && target.isLeft) {
                log.info(s"UPDATE_TIMES [dryrun] - ${src.getPath} --> ${target.left.get}")
            } else { //  Always in Right() case, dryrun-left already checked, and no dryrun means right
            val targetFileStatus = target.right.get
                if (approxDiffTimestamps(src, targetFileStatus)) {
                    if (config.dryRun) {
                        log.info(s"UPDATE_TIMES [dryrun] - ${src.getPath} --> ${target.right.get}")
                    } else {
                        log.debug(s"UPDATE_TIMES - ${src.getPath} --> ${target.right.get}")
                        config.dstFs.setTimes(targetFileStatus.getPath, src.getModificationTime, -1)
                    }
                }
            }
    }

    /**
     * This function is called from the base permissions-update function.
     * It actually applies the permission update using:
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
    private def updatePerms(src: FileStatus, target: Either[Path, FileStatus], chmodParser: Option[ChmodParser]): Unit = {
        if (config.dryRun && target.isLeft) {
            log.info(s"UPDATE_PERMS [dryrun] -  ${src.getPath} --> ${target.left.get}")
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
                    log.info(s"UPDATE_PERMS [dryrun] -  ${src.getPath} --> ${target.right.get}")
                } else {
                    log.debug(s"UPDATE_PERMS -  ${src.getPath} --> ${target.right.get}")
                    config.dstFs.setPermission(targetFileStatus.getPath, newPerm)
                }
            }
        }
    }

    /**
     * This function is called to update or not permissions of the target.
     * Update happens when:
     *  - perms preservation is on (perms option)
     *  - chmod commands are defined and new file/folder are created.
     *  - both
     *
     * @param src the src FileStatus from which to get permissions if needed
     * @param target The dst target to update, as either a Left(path) in case of dryrun (new file
     *               not acutally copied, so no FileStatus available), or a Right(FileStatus)
     *               when available (could be dryrun nonetheless).
     * @param isDir whether the target is directory or not (needed for chmod directory-specific updates)
     * @param isNew whether the target is new or not, as chmod without perms-preservation is applied
     *              to new files/folders only
     */
    private def updatePermsAsNeeded(src: FileStatus, target: Either[Path, FileStatus], isDir: Boolean, isNew: Boolean): Unit = {
        // Only apply chmod updates to new files if perms preservation is off
        if (config.preservePerms ||
            (isNew && ((isDir && config.chmodDirs.isDefined) || (! isDir && config.chmodFiles.isDefined)))) {

            if (isDir) {
                updatePerms(src, target, config.chmodDirs)
            } else {
                updatePerms(src, target, config.chmodFiles)
            }
        }
    }

    //*******************************************
    // Main processing functions (and helpers)
    //  - Process a single pair src/dst
    //  - Process a folder-tree level
    //*******************************************

    //
    //
    /**
     * Function processing a single src (file or folder), for a given dst (folder where to copy/update)
     * with an existing target being found or not (same filename as src in dst).
     *
     * @param src the src FileStatus to serve as base
     * @param dst the dst folder where to copy/create new object if necessary (if any)
     * @param foundTarget the found target in dst (same filename as src) if any
     * @param copy whether to copy a file or folder, or to create/touch a folder
     * @return Some path if dst was defined (can be a non-existent file in case of dryrun),
     *         None if no dst was provided (logs only).
     */
    private def processSrcAndDst(
        src: FileStatus,
        dst: Option[Path],
        foundTarget: Option[FileStatus],
        copy: Boolean
    ): Option[Path] = {
        if (config.dst.isEmpty) {
            if (copy) log.info(s"COPY_FILE [no-dst] - ${src.getPath}")
            else log.info(s"CREATE_FOLDER [no-dst] - ${src.getPath}")
            None
        } else {
            val target = new Path(s"${dst.get.toString}/${src.getPath.getName}")
            val isNew = foundTarget.isEmpty
            val targetToUpdate = {
                if (isNew) {
                    createOrCopyNew(src, target, copy)
                } else if (config.ignoreTimes || // Force copy/update
                    src.getLen != foundTarget.get.getLen || // Size is different
                    (!config.sizeOnly &&                    // Use modificationTime
                        approxDiffTimestamps(src, foundTarget.get))) {
                    copyUpdate(src, target, copy)
                } else {
                    skip(src, target, copy)
                }
            }
            preserveModificationTimesAsNeeded(src, targetToUpdate)
            updatePermsAsNeeded(src, targetToUpdate, isDir = src.isDirectory, isNew)
            Some(target)
        }
    }

    /**
     * Function listing files and folders of a src folder or glob (first call before recursion),
     * and building a Map[filename, FileStatus] from it.
     *
     * @param srcPath the src path to scan
     * @param recursionTreeRoot whether to use a glob if true or a folder scan if false
     * @return the Map[filename, FileStatus] of the folder/glob content
     */
    private def getSrcFilesAndFolders(srcPath: Path, recursionTreeRoot: Boolean): Map[String, FileStatus] = {
        // Use globStatus only for recursive tree root, as later we'll always have folders.
        // This prevent having to use the /* addition to Path for glob to return content folders.
        if (recursionTreeRoot) {
            val globSrc = config.srcFs.globStatus(srcPath)
            val globSrcChecked = if (globSrc != null) globSrc.toSeq else Seq.empty
            globSrcChecked.map(f => (f.getPath.getName, f)).toMap
        } else {
            config.srcFs.listStatus(srcPath).map(f => (f.getPath.getName, f)).toMap
        }
    }

    /**
     * Function listing files and folders from an optional dst folder and building a
     * Map[filename, FileStatus] from it. The map is empty in case if dst path is not defined
     * or if dst doesn't exist, which can happen in dryrun mode (folder is supposedly created
     * but not really).
     *
     * @param dstPath the optional dst path to scan
     * @return the Map[filename, FileStatus] of the folder content (if any, empty otherwise)
     */
    private def getDstFilesAndFolders(dstPath: Option[Path]): Map[String, FileStatus] = {
        // If no dst or dst doesn't exist, empty map
        if (config.dst.isEmpty || ! config.dstFs.exists(dstPath.get)) {
            Map.empty[String, FileStatus]
        } else {
            val fileStatusList = config.dstFs.listStatus(dstPath.get).toSeq
            fileStatusList.map(f => (f.getPath.getName, f)).toMap
        }
    }

    /**
     * Function deleting files and folders in dst that are not present in src, if config says so
     *
     * @param srcFilesAndFolders the list of files and folders in the worked src folder,
     *                           as a Map[filename, FileStatus]
     * @param dstFilesAndFolders the list of files and folders in the worked dst folder,
     *                           as a Map[filename, FileStatus]
     */
    private def deleteExtraneousDstAsNeeded(
        srcFilesAndFolders: Map[String, FileStatus],
        dstFilesAndFolders: Map[String, FileStatus]
    ): Unit = {
        if (config.deleteExtraneous) {
            dstFilesAndFolders.keySet.foreach(dstName => {
                if (! srcFilesAndFolders.contains(dstName)) {
                    val dst = dstFilesAndFolders(dstName).getPath
                    if (config.dryRun)
                        log.info(s"DELETE_DST [dryrun] - $dst")
                    else {
                        log.debug(s"DELETE_DST - $dst")
                        config.dstFs.delete(dst, true) // delete folders recursively
                    }
                }
            })
        }
    }


    /**
     * Function applying rsync at a folder-tree level.
     * It lists content from src and dst, applies dst extraneous deletion (if any),
     * and applies single-objects function based on objects from src present or not in dst.
     * If recursion is on, the function calls itself recursively when it meets a directory
     * in the src file list.
     *
     * @param srcPath the src path to rsync
     * @param dstPath the dst path to rsync to
     * @param recursiveTreeRoot whether the call is made at the recursive tree root or not (usefull
     *                          to get src files from glob)
     */
    private def applyRecursive(srcPath: Path, dstPath: Option[Path], recursiveTreeRoot: Boolean = false): Unit = {
        val srcContent = getSrcFilesAndFolders(srcPath, recursiveTreeRoot)
        val dstContent = getDstFilesAndFolders(dstPath)

        // First clean the dst directory from files not in src (if delete flag)
        deleteExtraneousDstAsNeeded(srcContent, dstContent)

        // Then copy/update from src to dst
        srcContent.keySet.foreach(srcName => {
            val src = srcContent(srcName)
            val foundTarget = dstContent.get(srcName)

            if (config.recurse && src.isDirectory) {
                val target = processSrcAndDst(src, dstPath, foundTarget, copy = false)
                applyRecursive(src.getPath, target)
            } else {
                processSrcAndDst(src, dstPath, foundTarget, copy = true)
            }
        })
    }

    /**
     * Main execution function.
     * Applies the recursive function on src and dst as defined in config
     */
    def apply(): Unit = {
        applyRecursive(config.srcPath, config.dst.map(_ => config.dstPath), recursiveTreeRoot = true)
    }

}
