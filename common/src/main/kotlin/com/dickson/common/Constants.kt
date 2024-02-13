package com.dickson.common

import com.dickson.common.enums.WorkflowExecutionStatus
import com.dickson.common.utils.isWindows
import java.time.Duration
import java.util.regex.Pattern

class Constants private constructor() {
    companion object {
        /**
         * common properties path
         */
        const val COMMON_PROPERTIES_PATH = "/common.properties"

        /**
         * registry properties
         */
        const val REGISTRY_DOLPHINSCHEDULER_MASTERS = "/nodes/master"
        const val REGISTRY_DOLPHINSCHEDULER_WORKERS = "/nodes/worker"
        const val REGISTRY_DOLPHINSCHEDULER_NODE = "/nodes"
        const val REGISTRY_DOLPHINSCHEDULER_LOCK_MASTERS = "/lock/masters"
        const val REGISTRY_DOLPHINSCHEDULER_LOCK_FAILOVER_MASTERS = "/lock/failover/masters"
        const val FORMAT_SS = "%s%s"
        const val FORMAT_S_S = "%s/%s"
        const val FORMAT_S_S_COLON = "%s:%s"
        const val FOLDER_SEPARATOR = "/"
        const val RESOURCE_TYPE_FILE = "resources"
        const val RESOURCE_TYPE_UDF = "udfs"
        const val STORAGE_S3 = "S3"
        const val STORAGE_HDFS = "HDFS"
        const val EMPTY_STRING = ""

        /**
         * resource.hdfs.fs.defaultFS
         */
        const val FS_DEFAULT_FS = "resource.hdfs.fs.defaultFS"

        /**
         * hadoop configuration
         */
        const val HADOOP_RM_STATE_ACTIVE = "ACTIVE"
        const val HADOOP_RESOURCE_MANAGER_HTTPADDRESS_PORT = "resource.manager.httpaddress.port"

        /**
         * yarn.resourcemanager.ha.rm.ids
         */
        const val YARN_RESOURCEMANAGER_HA_RM_IDS = "yarn.resourcemanager.ha.rm.ids"

        /**
         * yarn.application.status.address
         */
        const val YARN_APPLICATION_STATUS_ADDRESS = "yarn.application.status.address"

        /**
         * yarn.job.history.status.address
         */
        const val YARN_JOB_HISTORY_STATUS_ADDRESS = "yarn.job.history.status.address"

        /**
         * hdfs configuration
         * resource.hdfs.root.user
         */
        const val HDFS_ROOT_USER = "resource.hdfs.root.user"

        /**
         * hdfs/s3 configuration
         * resource.storage.upload.base.path
         */
        const val RESOURCE_UPLOAD_PATH = "resource.storage.upload.base.path"

        /**
         * data basedir path
         */
        const val DATA_BASEDIR_PATH = "data.basedir.path"

        /**
         * dolphinscheduler.env.path
         */
        const val DOLPHINSCHEDULER_ENV_PATH = "dolphinscheduler.env.path"

        /**
         * environment properties default path
         */
        const val ENV_PATH = "dolphinscheduler_env.sh"

        /**
         * resource.view.suffixs
         */
        const val RESOURCE_VIEW_SUFFIXES = "resource.view.suffixs"
        const val RESOURCE_VIEW_SUFFIXES_DEFAULT_VALUE =
            "txt,log,sh,bat,conf,cfg,py,java,sql,xml,hql,properties,json,yml,yaml,ini,js"

        /**
         * development.state
         */
        const val DEVELOPMENT_STATE = "development.state"

        /**
         * sudo enable
         */
        const val SUDO_ENABLE = "sudo.enable"

        /**
         * string true
         */
        const val STRING_TRUE = "true"

        /**
         * resource storage type
         */
        const val RESOURCE_STORAGE_TYPE = "resource.storage.type"
        const val AWS_S3_BUCKET_NAME = "resource.aws.s3.bucket.name"
        const val AWS_END_POINT = "resource.aws.s3.endpoint"

        /**
         * comma ,
         */
        const val COMMA = ","

        /**
         * COLON :
         */
        const val COLON = ":"

        /**
         * period .
         */
        const val PERIOD = "."

        /**
         * QUESTION ?
         */
        const val QUESTION = "?"

        /**
         * SPACE " "
         */
        const val SPACE = " "

        /**
         * SINGLE_SLASH /
         */
        const val SINGLE_SLASH = "/"

        /**
         * DOUBLE_SLASH //
         */
        const val DOUBLE_SLASH = "//"

        /**
         * EQUAL SIGN
         */
        const val EQUAL_SIGN = "="

        /**
         * AT SIGN
         */
        const val AT_SIGN = "@"

        /**
         * date format of yyyy-MM-dd HH:mm:ss
         */
        const val YYYY_MM_DD_HH_MM_SS = "yyyy-MM-dd HH:mm:ss"

        /**
         * date format of yyyyMMdd
         */
        const val YYYYMMDD = "yyyyMMdd"

        /**
         * date format of yyyyMMddHHmmss
         */
        const val YYYYMMDDHHMMSS = "yyyyMMddHHmmss"

        /**
         * date format of yyyyMMddHHmmssSSS
         */
        const val YYYYMMDDHHMMSSSSS = "yyyyMMddHHmmssSSS"
        const val IMPORT_SUFFIX = "_import_"
        const val COPY_SUFFIX = "_copy_"

        /**
         * http connect time out
         */
        const val HTTP_CONNECT_TIMEOUT = 60 * 1000

        /**
         * http connect request time out
         */
        const val HTTP_CONNECTION_REQUEST_TIMEOUT = 60 * 1000

        /**
         * httpclient soceket time out
         */
        const val SOCKET_TIMEOUT = 60 * 1000

        /**
         * registry session timeout
         */
        const val REGISTRY_SESSION_TIMEOUT = 10 * 1000

        /**
         * http header
         */
        const val HTTP_HEADER_UNKNOWN = "unKnown"

        /**
         * http X-Forwarded-For
         */
        const val HTTP_X_FORWARDED_FOR = "X-Forwarded-For"

        /**
         * http X-Real-IP
         */
        const val HTTP_X_REAL_IP = "X-Real-IP"

        /**
         * UTF-8
         */
        const val UTF_8 = "UTF-8"

        /**
         * user name regex
         */
        val REGEX_USER_NAME = Pattern.compile("^[a-zA-Z0-9._-]{3,39}$")

        /**
         * read permission
         */
        const val READ_PERMISSION = 2

        /**
         * write permission
         */
        const val WRITE_PERMISSION = 2 * 2

        /**
         * execute permission
         */
        const val EXECUTE_PERMISSION = 1

        /**
         * default admin permission
         */
        const val DEFAULT_ADMIN_PERMISSION = 7

        /**
         * default hash map size
         */
        const val DEFAULT_HASH_MAP_SIZE = 16

        /**
         * all permissions
         */
        const val ALL_PERMISSIONS = READ_PERMISSION or WRITE_PERMISSION or EXECUTE_PERMISSION

        /**
         * max task timeout
         */
        const val MAX_TASK_TIMEOUT = 24 * 3600

        /**
         * worker host weight
         */
        const val DEFAULT_WORKER_HOST_WEIGHT = 100

        /**
         * time unit secong to minutes
         */
        const val SEC_2_MINUTES_TIME_UNIT = 60

        /***
         *
         * rpc port
         */
        const val RPC_PORT = "rpc.port"

        /**
         * forbid running task
         */
        const val FLOWNODE_RUN_FLAG_FORBIDDEN = "FORBIDDEN"

        /**
         * normal running task
         */
        const val FLOWNODE_RUN_FLAG_NORMAL = "NORMAL"
        const val COMMON_TASK_TYPE = "common"
        const val DEFAULT = "default"
        const val PASSWORD = "password"
        const val XXXXXX = "******"
        const val NULL = "NULL"
        const val THREAD_NAME_MASTER_SERVER = "Master-Server"
        const val THREAD_NAME_WORKER_SERVER = "Worker-Server"
        const val THREAD_NAME_ALERT_SERVER = "Alert-Server"

        /**
         * command parameter keys
         */
        const val CMD_PARAM_RECOVER_PROCESS_ID_STRING = "ProcessInstanceId"
        const val CMD_PARAM_RECOVERY_START_NODE_STRING = "StartNodeIdList"
        const val CMD_PARAM_RECOVERY_WAITING_THREAD = "WaitingThreadInstanceId"
        const val CMD_PARAM_SUB_PROCESS = "processInstanceId"
        const val CMD_PARAM_EMPTY_SUB_PROCESS = "0"
        const val CMD_PARAM_SUB_PROCESS_PARENT_INSTANCE_ID = "parentProcessInstanceId"
        const val CMD_PARAM_SUB_PROCESS_DEFINE_CODE = "processDefinitionCode"
        const val CMD_PARAM_START_NODES = "StartNodeList"
        const val CMD_PARAM_START_PARAMS = "StartParams"
        const val CMD_PARAM_FATHER_PARAMS = "fatherParams"

        /**
         * complement data start date
         */
        const val CMDPARAM_COMPLEMENT_DATA_START_DATE = "complementStartDate"

        /**
         * complement data end date
         */
        const val CMDPARAM_COMPLEMENT_DATA_END_DATE = "complementEndDate"

        /**
         * complement data Schedule date
         */
        const val CMDPARAM_COMPLEMENT_DATA_SCHEDULE_DATE_LIST = "complementScheduleDateList"

        /**
         * complement date default cron string
         */
        const val DEFAULT_CRON_STRING = "0 0 0 * * ? *"

        /**
         * sleep 1000ms
         */
        const val SLEEP_TIME_MILLIS = 1000L

        /**
         * short sleep 100ms
         */
        const val SLEEP_TIME_MILLIS_SHORT = 100L
        val SERVER_CLOSE_WAIT_TIME = Duration.ofSeconds(3)

        /**
         * one second mils
         */
        const val SECOND_TIME_MILLIS = 1000L

        /**
         * master task instance cache-database refresh interval
         */
        const val CACHE_REFRESH_TIME_MILLIS = 20 * 1000L

        /**
         * heartbeat for zk info length
         */
        const val HEARTBEAT_FOR_ZOOKEEPER_INFO_LENGTH = 14

        /**
         * jar
         */
        const val JAR = "jar"

        /**
         * hadoop
         */
        const val HADOOP = "hadoop"

        /**
         * -D <property>=<value>
        </value></property> */
        const val D = "-D"

        /**
         * exit code success
         */
        const val EXIT_CODE_SUCCESS = 0

        /**
         * exit code failure
         */
        const val EXIT_CODE_FAILURE = -1

        /**
         * process or task definition failure
         */
        const val DEFINITION_FAILURE = -1
        const val OPPOSITE_VALUE = -1

        /**
         * process or task definition first version
         */
        const val VERSION_FIRST = 1

        /**
         * date format of yyyyMMdd
         */
        const val PARAMETER_FORMAT_DATE = "yyyyMMdd"

        /**
         * date format of yyyyMMddHHmmss
         */
        const val PARAMETER_FORMAT_TIME = "yyyyMMddHHmmss"

        /**
         * system date(yyyyMMddHHmmss)
         */
        const val PARAMETER_DATETIME = "system.datetime"

        /**
         * system date(yyyymmdd) today
         */
        const val PARAMETER_CURRENT_DATE = "system.biz.curdate"

        /**
         * system date(yyyymmdd) yesterday
         */
        const val PARAMETER_BUSINESS_DATE = "system.biz.date"

        /**
         * ACCEPTED
         */
        const val ACCEPTED = "ACCEPTED"

        /**
         * SUCCEEDED
         */
        const val SUCCEEDED = "SUCCEEDED"

        /**
         * ENDED
         */
        const val ENDED = "ENDED"

        /**
         * NEW
         */
        const val NEW = "NEW"

        /**
         * NEW_SAVING
         */
        const val NEW_SAVING = "NEW_SAVING"

        /**
         * SUBMITTED
         */
        const val SUBMITTED = "SUBMITTED"

        /**
         * FAILED
         */
        const val FAILED = "FAILED"

        /**
         * KILLED
         */
        const val KILLED = "KILLED"

        /**
         * RUNNING
         */
        const val RUNNING = "RUNNING"

        /**
         * underline  "_"
         */
        const val UNDERLINE = "_"

        /**
         * application regex
         */
        const val APPLICATION_REGEX = "application_\\d+_\\d+"
        val PID = if (isWindows()) "handle" else "pid"

        /**
         * month_begin
         */
        const val MONTH_BEGIN = "month_begin"

        /**
         * add_months
         */
        const val ADD_MONTHS = "add_months"

        /**
         * month_end
         */
        const val MONTH_END = "month_end"

        /**
         * week_begin
         */
        const val WEEK_BEGIN = "week_begin"

        /**
         * week_end
         */
        const val WEEK_END = "week_end"

        /**
         * timestamp
         */
        const val TIMESTAMP = "timestamp"
        const val SUBTRACT_CHAR = '-'
        const val ADD_CHAR = '+'
        const val MULTIPLY_CHAR = '*'
        const val DIVISION_CHAR = '/'
        const val LEFT_BRACE_CHAR = '('
        const val RIGHT_BRACE_CHAR = ')'
        const val ADD_STRING = "+"
        const val STAR = "*"
        const val DIVISION_STRING = "/"
        const val LEFT_BRACE_STRING = "("
        const val P = 'P'
        const val N = 'N'
        const val SUBTRACT_STRING = "-"
        const val GLOBAL_PARAMS = "globalParams"
        const val LOCAL_PARAMS = "localParams"
        const val SUBPROCESS_INSTANCE_ID = "subProcessInstanceId"
        const val PROCESS_INSTANCE_STATE = "processInstanceState"
        const val PARENT_WORKFLOW_INSTANCE = "parentWorkflowInstance"
        const val CONDITION_RESULT = "conditionResult"
        const val SWITCH_RESULT = "switchResult"
        const val WAIT_START_TIMEOUT = "waitStartTimeout"
        const val DEPENDENCE = "dependence"
        const val TASK_LIST = "taskList"
        const val QUEUE = "queue"
        const val QUEUE_NAME = "queueName"
        const val LOG_QUERY_SKIP_LINE_NUMBER = 0
        const val LOG_QUERY_LIMIT = 4096
        const val BLOCKING_CONDITION = "blockingCondition"
        const val ALERT_WHEN_BLOCKING = "alertWhenBlocking"

        /**
         * master/worker server use for zk
         */
        const val MASTER_TYPE = "master"
        const val WORKER_TYPE = "worker"
        const val DELETE_OP = "delete"
        const val ADD_OP = "add"
        const val ALIAS = "alias"
        const val CONTENT = "content"
        const val DEPENDENT_SPLIT = ":||"
        const val DEPENDENT_ALL_TASK_CODE: Long = 0

        /**
         * preview schedule execute count
         */
        const val PREVIEW_SCHEDULE_EXECUTE_COUNT = 5

        /**
         * kerberos
         */
        const val KERBEROS = "kerberos"

        /**
         * kerberos expire time
         */
        const val KERBEROS_EXPIRE_TIME = "kerberos.expire.time"

        /**
         * java.security.krb5.conf
         */
        const val JAVA_SECURITY_KRB5_CONF = "java.security.krb5.conf"

        /**
         * java.security.krb5.conf.path
         */
        const val JAVA_SECURITY_KRB5_CONF_PATH = "java.security.krb5.conf.path"

        /**
         * hadoop.security.authentication
         */
        const val HADOOP_SECURITY_AUTHENTICATION = "hadoop.security.authentication"

        /**
         * hadoop.security.authentication
         */
        const val HADOOP_SECURITY_AUTHENTICATION_STARTUP_STATE = "hadoop.security.authentication.startup.state"

        /**
         * com.amazonaws.services.s3.enableV4
         */
        const val AWS_S3_V4 = "com.amazonaws.services.s3.enableV4"

        /**
         * loginUserFromKeytab user
         */
        const val LOGIN_USER_KEY_TAB_USERNAME = "login.user.keytab.username"

        /**
         * loginUserFromKeytab path
         */
        const val LOGIN_USER_KEY_TAB_PATH = "login.user.keytab.path"
        const val WORKFLOW_INSTANCE_ID_MDC_KEY = "workflowInstanceId"
        const val TASK_INSTANCE_ID_MDC_KEY = "taskInstanceId"

        /**
         * task log info format
         */
        const val TASK_LOG_INFO_FORMAT = "TaskLogInfo-%s"
        val NOT_TERMINATED_STATES = intArrayOf(
            WorkflowExecutionStatus.SUBMITTED_SUCCESS.getCode(),
            //TaskExecutionStatus.DISPATCH.getCode(),
            WorkflowExecutionStatus.RUNNING_EXECUTION.getCode(),
            WorkflowExecutionStatus.DELAY_EXECUTION.getCode(),
            WorkflowExecutionStatus.READY_PAUSE.getCode(),
            WorkflowExecutionStatus.READY_STOP.getCode(),
            //TaskExecutionStatus.NEED_FAULT_TOLERANCE.getCode()
        )
        val RUNNING_PROCESS_STATE = intArrayOf(
//            TaskExecutionStatus.RUNNING_EXECUTION.getCode(),
//            TaskExecutionStatus.SUBMITTED_SUCCESS.getCode(),
//            TaskExecutionStatus.DISPATCH.getCode(),
            WorkflowExecutionStatus.SERIAL_WAIT.getCode()
        )

        /**
         * status
         */
        const val STATUS = "status"

        /**
         * message
         */
        const val MSG = "msg"

        /**
         * data total
         */
        const val COUNT = "count"

        /**
         * page size
         */
        const val PAGE_SIZE = "pageSize"

        /**
         * current page no
         */
        const val PAGE_NUMBER = "pageNo"

        /**
         *
         */
        const val DATA_LIST = "data"
        const val TOTAL_LIST = "totalList"
        const val CURRENT_PAGE = "currentPage"
        const val TOTAL_PAGE = "totalPage"
        const val TOTAL = "total"

        /**
         * workflow
         */
        const val WORKFLOW_LIST = "workFlowList"
        const val WORKFLOW_RELATION_LIST = "workFlowRelationList"

        /**
         * session user
         */
        const val SESSION_USER = "session.user"
        const val SESSION_ID = "sessionId"

        /**
         * locale
         */
        const val LOCALE_LANGUAGE = "language"

        /**
         * database type
         */
        const val MYSQL = "MYSQL"
        const val HIVE = "HIVE"
        const val ADDRESS = "address"
        const val DATABASE = "database"
        const val OTHER = "other"
        const val USER = "user"
        const val JDBC_URL = "jdbcUrl"

        /**
         * session timeout
         */
        const val SESSION_TIME_OUT = 7200
        const val MAX_FILE_SIZE = 1024 * 1024 * 1024
        const val UDF = "UDF"
        const val CLASS = "class"

        /**
         * dataSource sensitive param
         */
        const val DATASOURCE_PASSWORD_REGEX = "(?<=((?i)password((\\\\\":\\\\\")|(=')))).*?(?=((\\\\\")|(')))"

        /**
         * default worker group
         */
        const val DEFAULT_WORKER_GROUP = "default"

        /**
         * authorize writable perm
         */
        const val AUTHORIZE_WRITABLE_PERM = 7

        /**
         * authorize readable perm
         */
        const val AUTHORIZE_READABLE_PERM = 4
        const val NORMAL_NODE_STATUS = 0
        const val ABNORMAL_NODE_STATUS = 1
        const val BUSY_NODE_STATUE = 2
        const val START_TIME = "start time"
        const val END_TIME = "end time"
        const val START_END_DATE = "startDate,endDate"

        /**
         * system line separator
         */
        val SYSTEM_LINE_SEPARATOR = System.getProperty("line.separator")

        /**
         * datasource encryption salt
         */
        const val DATASOURCE_ENCRYPTION_SALT_DEFAULT = "!@#$%^&*"
        const val DATASOURCE_ENCRYPTION_ENABLE = "datasource.encryption.enable"
        const val DATASOURCE_ENCRYPTION_SALT = "datasource.encryption.salt"

        /**
         * network interface preferred
         */
        const val DOLPHIN_SCHEDULER_NETWORK_INTERFACE_PREFERRED = "dolphin.scheduler.network.interface.preferred"

        /**
         * network IP gets priority, default inner outer
         */
        const val DOLPHIN_SCHEDULER_NETWORK_PRIORITY_STRATEGY = "dolphin.scheduler.network.priority.strategy"

        /**
         * exec shell scripts
         */
        const val SH = "sh"

        /**
         * pstree, get pud and sub pid
         */
        const val PSTREE = "pstree"
        val KUBERNETES_MODE = !System.getenv("KUBERNETES_SERVICE_HOST").isEmpty() && !System.getenv("KUBERNETES_SERVICE_PORT").isEmpty()

        /**
         * dry run flag
         */
        const val DRY_RUN_FLAG_NO = 0
        const val DRY_RUN_FLAG_YES = 1

        /**
         * data.quality.error.output.path
         */
        const val DATA_QUALITY_ERROR_OUTPUT_PATH = "data-quality.error.output.path"
        const val CACHE_KEY_VALUE_ALL = "'all'"

        /**
         * use for k8s
         */
        const val NAMESPACE = "namespace"
        const val CLUSTER = "cluster"
        const val LIMITS_CPU = "limitsCpu"
        const val LIMITS_MEMORY = "limitsMemory"
        const val K8S_LOCAL_TEST_CLUSTER_CODE = 0L

        /**
         * schedule timezone
         */
        const val SCHEDULE_TIMEZONE = "schedule_timezone"
        const val RESOURCE_FULL_NAME_MAX_LENGTH = 128

        /**
         * tenant
         */
        const val TENANT_FULL_NAME_MAX_LENGTH = 30

        /**
         * schedule time  the amount of date data is too large, affecting the memory, so set 100
         */
        const val SCHEDULE_TIME_MAX_LENGTH = 100

        /**
         * password max and min LENGTH
         */
        const val USER_PASSWORD_MAX_LENGTH = 20
        const val USER_PASSWORD_MIN_LENGTH = 2
        const val FUNCTION_START_WITH = "$"
        const val DEFAULT_QUEUE_ID = 1

        /**
         * Security authentication types (supported types: PASSWORD,LDAP)
         */
        const val SECURITY_CONFIG_TYPE = "securityConfigType"
        const val SECURITY_CONFIG_TYPE_PASSWORD = "PASSWORD"
        const val SECURITY_CONFIG_TYPE_LDAP = "LDAP"
    }

    init {
        throw UnsupportedOperationException("Construct Constants")
    }
}
