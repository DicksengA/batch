package com.dickson.common.storage

import com.dickson.common.Constants
import com.dickson.common.enums.ResUploadType
import com.dickson.common.enums.ResourceType
import com.dickson.common.utils.PropertyUtils
import java.io.IOException
import java.lang.Exception


interface StorageOperate {
    /**
     * if the resource of tenant 's exist, the resource of folder will be created
     * @param tenantCode
     * @throws Exception
     */
    @Throws(Exception::class)
    fun createTenantDirIfNotExists(tenantCode: String?)

    /**
     * get the resource directory of tenant
     * @param tenantCode
     * @return
     */
    fun getResDir(tenantCode: String?): String?

    /**
     * return the udf directory of tenant
     * @param tenantCode
     * @return
     */
    fun getUdfDir(tenantCode: String?): String?

    /**
     * create the directory that the path of tenant wanted to create
     * @param tenantCode
     * @param path
     * @return
     * @throws IOException
     */
    @Throws(IOException::class)
    fun mkdir(tenantCode: String?, path: String?): Boolean

    /**
     * get the path of the resource file
     * @param tenantCode
     * @param fullName
     * @return
     */
    fun getResourceFileName(tenantCode: String?, fullName: String?): String?

    /**
     * get the path of the file
     * @param resourceType
     * @param tenantCode
     * @param fileName
     * @return
     */
    fun getFileName(resourceType: ResourceType?, tenantCode: String?, fileName: String?): String?

    /**
     * predicate  if the resource of tenant exists
     * @param tenantCode
     * @param fileName
     * @return
     * @throws IOException
     */
    @Throws(IOException::class)
    fun exists(tenantCode: String?, fileName: String?): Boolean

    /**
     * delete the resource of  filePath
     * todo if the filePath is the type of directory ,the files in the filePath need to be deleted at all
     * @param tenantCode
     * @param filePath
     * @param recursive
     * @return
     * @throws IOException
     */
    @Throws(IOException::class)
    fun delete(tenantCode: String?, filePath: String?, recursive: Boolean): Boolean

    /**
     * copy the file from srcPath to dstPath
     * @param srcPath
     * @param dstPath
     * @param deleteSource if need to delete the file of srcPath
     * @param overwrite
     * @return
     * @throws IOException
     */
    @Throws(IOException::class)
    fun copy(srcPath: String?, dstPath: String?, deleteSource: Boolean, overwrite: Boolean): Boolean

    /**
     * get the root path of the tenant with resourceType
     * @param resourceType
     * @param tenantCode
     * @return
     */
    fun getDir(resourceType: ResourceType?, tenantCode: String?): String?

    /**
     * upload the local srcFile to dstPath
     * @param tenantCode
     * @param srcFile
     * @param dstPath
     * @param deleteSource
     * @param overwrite
     * @return
     * @throws IOException
     */
    @Throws(IOException::class)
    fun upload(
        tenantCode: String?,
        srcFile: String?,
        dstPath: String?,
        deleteSource: Boolean,
        overwrite: Boolean
    ): Boolean

    /**
     * download the srcPath to local
     * @param tenantCode
     * @param srcFilePath the full path of the srcPath
     * @param dstFile
     * @param deleteSource
     * @param overwrite
     * @throws IOException
     */
    @Throws(IOException::class)
    fun download(tenantCode: String?, srcFilePath: String?, dstFile: String?, deleteSource: Boolean, overwrite: Boolean)

    /**
     * vim the context of filePath
     * @param tenantCode
     * @param filePath
     * @param skipLineNums
     * @param limit
     * @return
     * @throws IOException
     */
    @Throws(IOException::class)
    fun vimFile(tenantCode: String?, filePath: String?, skipLineNums: Int, limit: Int): List<String?>?

    /**
     * delete the files and directory of the tenant
     *
     * @param tenantCode
     * @throws Exception
     */
    @Throws(Exception::class)
    fun deleteTenant(tenantCode: String)

    /**
     * return the storageType
     *
     * @return
     */
    fun returnStorageType(): ResUploadType

    companion object {
        val RESOURCE_UPLOAD_PATH: String = PropertyUtils.getString(Constants.RESOURCE_UPLOAD_PATH, "/dolphinscheduler")
    }
}
