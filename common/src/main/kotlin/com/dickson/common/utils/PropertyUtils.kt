package com.dickson.common.utils

import com.dickson.common.Constants
import com.dickson.common.enums.ResUploadType
import org.slf4j.LoggerFactory
import java.io.IOException
import java.util.*
import java.util.function.BiConsumer
import java.util.function.Consumer
import kotlin.collections.HashMap

class PropertyUtils private constructor() {
    companion object {
        private val COMMON_PROPERTIES_PATH: String = ""
        private val logger = LoggerFactory.getLogger(PropertyUtils::class.java)
        private val properties = Properties()
        @Synchronized
        fun loadPropertyFile(vararg propertyFiles: String?) {
            for (fileName in propertyFiles) {
                try {
                    PropertyUtils::class.java.getResourceAsStream(fileName).use { fis ->
                        properties.load(
                            fis
                        )
                    }
                } catch (e: IOException) {
                    logger.error(e.message, e)
                    System.exit(1)
                }
            }

            // Override from system properties
            System.getProperties().forEach { k: Any, v: Any ->
                val key = k.toString()
                logger.info("Overriding property from system property: {}", key)
                setValue(key, v.toString())
            }
        }

        /**
         * @return judge whether resource upload startup
         */
        val resUploadStartupState: Boolean
            get() {
                val resUploadStartupType = getUpperCaseString(Constants.RESOURCE_STORAGE_TYPE)
                val resUploadType: ResUploadType =
                    ResUploadType.valueOf(if (resUploadStartupType.isNullOrEmpty()) ResUploadType.NONE.name else resUploadStartupType)
                return resUploadType !== ResUploadType.NONE
            }

        /**
         * get property value
         *
         * @param key property name
         * @return property value
         */
        fun getString(key: String): String {
            return properties.getProperty(key.trim { it <= ' ' })
        }

        /**
         * get property value with upper case
         *
         * @param key property name
         * @return property value  with upper case
         */
        fun getUpperCaseString(key: String): String {
            val value = getString(key)
            return if (value.isNullOrEmpty()) value else value.uppercase()
        }

        /**
         * get property value
         *
         * @param key property name
         * @param defaultVal default value
         * @return property value
         */
        fun getString(key: String, defaultVal: String): String {
            val value = getString(key)
            return if (value.isNullOrEmpty()) defaultVal else value
        }

        /**
         * get property value
         *
         * @param key property name
         * @return get property int value , if key == null, then return -1
         */
        fun getInt(key: String): Int {
            return getInt(key, -1)
        }

        /**
         * @param key key
         * @param defaultValue default value
         * @return property value
         */
        fun getInt(key: String, defaultValue: Int): Int {
            val value = getString(key)
            if (value.isNullOrEmpty()) {
                return defaultValue
            }
            try {
                return value.toInt()
            } catch (e: NumberFormatException) {
                logger.info(e.message, e)
            }
            return defaultValue
        }

        /**
         * get property value
         *
         * @param key property name
         * @return property value
         */
        fun getBoolean(key: String): Boolean {
            return getBoolean(key, false)
        }

        /**
         * get property value
         *
         * @param key property name
         * @param defaultValue default value
         * @return property value
         */
        fun getBoolean(key: String, defaultValue: Boolean): Boolean {
            val value = getString(key)
            return if (value.isNullOrEmpty()) defaultValue else java.lang.Boolean.parseBoolean(
                value
            )
        }

        /**
         * get property long value
         *
         * @param key key
         * @param defaultValue default value
         * @return property value
         */
        fun getLong(key: String, defaultValue: Long): Long {
            val value = getString(key)
            if (value.isNullOrEmpty()) {
                return defaultValue
            }
            try {
                return value.toLong()
            } catch (e: NumberFormatException) {
                logger.info(e.message, e)
            }
            return defaultValue
        }

        /**
         * @param key key
         * @return property value
         */
        fun getLong(key: String): Long {
            return getLong(key, -1)
        }

        /**
         * @param key key
         * @param defaultValue default value
         * @return property value
         */
        fun getDouble(key: String, defaultValue: Double): Double {
            val value = getString(key)
            if (value.isNullOrEmpty()) {
                return defaultValue
            }
            try {
                return value.toDouble()
            } catch (e: NumberFormatException) {
                logger.info(e.message, e)
            }
            return defaultValue
        }

        /**
         * get array
         *
         * @param key property name
         * @param splitStr separator
         * @return property value through array
         */
        fun getArray(key: String, splitStr: String?): Array<String?> {
            val value = getString(key)
            return if (value.isNullOrEmpty()) {
                arrayOfNulls(0)
            } else value.split(splitStr!!).toTypedArray()
        }

        /**
         * @param key key
         * @param type type
         * @param defaultValue default value
         * @param <T> T
         * @return get enum value
        </T> */
        fun <T : Enum<T>?> getEnum(
            key: String, type: Class<T>?,
            defaultValue: T
        ): T {
            val value = getString(key)
            if (value.isNullOrEmpty()) {
                return defaultValue
            }
            try {
                return java.lang.Enum.valueOf(type, value)
            } catch (e: IllegalArgumentException) {
                logger.info(e.message, e)
            }
            return defaultValue
        }

        /**
         * get all properties with specified prefix, like: fs.
         *
         * @param prefix prefix to search
         * @return all properties with specified prefix
         */
        fun getPrefixedProperties(prefix: String?): Map<String, String> {
            val matchedProperties: MutableMap<String, String> = HashMap()
            for (propName in properties.stringPropertyNames()) {
                if (propName.startsWith(prefix!!)) {
                    matchedProperties[propName] = properties.getProperty(propName)
                }
            }
            return matchedProperties
        }

        /**
         * set value
         * @param key key
         * @param value value
         */
        fun setValue(key: String?, value: String?) {
            properties.setProperty(key, value)
        }

        fun getPropertiesByPrefix(prefix: String): Map<String, String>? {
            if (prefix.isNullOrEmpty()) {
                return null
            }
            val keys: Set<Any> = properties.keys
            if (keys.isEmpty()) {
                return null
            }
            val propertiesMap: MutableMap<String, String> = HashMap()
            keys.forEach(Consumer { k: Any ->
                if (k.toString().contains(prefix)) {
                    propertiesMap[k.toString().replaceFirst(prefix + ".".toRegex(), "")] =
                        properties.getProperty(k as String)
                }
            })
            return propertiesMap
        }

        init {
            loadPropertyFile(COMMON_PROPERTIES_PATH)
        }
    }

    init {
        throw UnsupportedOperationException("Construct PropertyUtils")
    }
}
