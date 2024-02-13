package com.dickson.common.config

import com.dickson.common.enums.ResUploadType
import com.dickson.common.enums.ResourceType
import com.dickson.common.storage.StorageOperate

class S3Utils {

    companion object{
        @JvmStatic
        fun getInstance():StorageOperate{
            return object :StorageOperate{
                override fun createTenantDirIfNotExists(tenantCode: String?) {
                    TODO("Not yet implemented")
                }

                override fun getResDir(tenantCode: String?): String? {
                    TODO("Not yet implemented")
                }

                override fun getUdfDir(tenantCode: String?): String? {
                    TODO("Not yet implemented")
                }

                override fun mkdir(tenantCode: String?, path: String?): Boolean {
                    TODO("Not yet implemented")
                }

                override fun getResourceFileName(tenantCode: String?, fullName: String?): String? {
                    TODO("Not yet implemented")
                }

                override fun getFileName(resourceType: ResourceType?, tenantCode: String?, fileName: String?): String? {
                    TODO("Not yet implemented")
                }

                override fun exists(tenantCode: String?, fileName: String?): Boolean {
                    TODO("Not yet implemented")
                }

                override fun delete(tenantCode: String?, filePath: String?, recursive: Boolean): Boolean {
                    TODO("Not yet implemented")
                }

                override fun copy(
                    srcPath: String?,
                    dstPath: String?,
                    deleteSource: Boolean,
                    overwrite: Boolean
                ): Boolean {
                    TODO("Not yet implemented")
                }

                override fun getDir(resourceType: ResourceType?, tenantCode: String?): String? {
                    TODO("Not yet implemented")
                }

                override fun upload(
                    tenantCode: String?,
                    srcFile: String?,
                    dstPath: String?,
                    deleteSource: Boolean,
                    overwrite: Boolean
                ): Boolean {
                    TODO("Not yet implemented")
                }

                override fun download(
                    tenantCode: String?,
                    srcFilePath: String?,
                    dstFile: String?,
                    deleteSource: Boolean,
                    overwrite: Boolean
                ) {
                    TODO("Not yet implemented")
                }

                override fun vimFile(
                    tenantCode: String?,
                    filePath: String?,
                    skipLineNums: Int,
                    limit: Int
                ): List<String?>? {
                    TODO("Not yet implemented")
                }

                override fun deleteTenant(tenantCode: String) {
                    TODO("Not yet implemented")
                }

                override fun returnStorageType(): ResUploadType {
                    TODO("Not yet implemented")
                }

            }
        }
    }

}
