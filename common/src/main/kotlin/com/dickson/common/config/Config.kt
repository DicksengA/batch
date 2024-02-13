package com.dickson.common.config

import com.dickson.common.Constants.Companion.RESOURCE_STORAGE_TYPE
import com.dickson.common.Constants.Companion.STORAGE_S3
import com.dickson.common.storage.StorageOperate
import com.dickson.common.utils.PropertyUtils
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.inject.Produces


@ApplicationScoped
class StoreConfiguration {
    @Produces
    fun storageOperate(): StorageOperate? {
        return when (PropertyUtils.getString(RESOURCE_STORAGE_TYPE)) {
            STORAGE_S3 -> S3Utils.getInstance()
            //STORAGE_HDFS -> HadoopUtils.getInstance()
            else -> null
        }
    }
}
