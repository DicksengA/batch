package com.dickson.common.utils

fun isWindows():Boolean{

        val os = System.getProperty("os.name").lowercase()
        return os.contains("win")

}