"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.ErrorGroups = exports.AttendanceErrorTypes = exports.StorageErrorTypes = exports.CacheErrorTypes = exports.SyncErrorTypes = exports.NetworkErrorTypes = exports.DataErrorTypes = exports.AuthenticationErrorTypes = exports.FileErrorTypes = exports.DataConflictErrorTypes = exports.ValidationErrorTypes = exports.SystemErrorTypes = exports.PermissionErrorTypes = exports.UserErrorTypes = exports.TokenErrorTypes = exports.RequestErrorTypes = void 0;
var RequestErrorTypes;
(function (RequestErrorTypes) {
    RequestErrorTypes["INVALID_PARAMETERS"] = "INVALID_PARAMETERS";
    RequestErrorTypes["MISSING_PARAMETERS"] = "MISSING_PARAMETERS";
    RequestErrorTypes["REQUEST_FAILED"] = "REQUEST_FAILED";
    RequestErrorTypes["MALFORMED_REQUEST"] = "MALFORMED_REQUEST";
    RequestErrorTypes["PAYLOAD_TOO_LARGE"] = "PAYLOAD_TOO_LARGE";
})(RequestErrorTypes || (exports.RequestErrorTypes = RequestErrorTypes = {}));
var TokenErrorTypes;
(function (TokenErrorTypes) {
    TokenErrorTypes["TOKEN_UNAUTHORIZED"] = "TOKEN_UNAUTHORIZED";
    TokenErrorTypes["TOKEN_MISSING"] = "TOKEN_MISSING";
    TokenErrorTypes["TOKEN_INVALID_FORMAT"] = "TOKEN_INVALID_FORMAT";
    TokenErrorTypes["TOKEN_EXPIRED"] = "TOKEN_EXPIRED";
    TokenErrorTypes["TOKEN_MALFORMED"] = "TOKEN_MALFORMED";
    TokenErrorTypes["TOKEN_INVALID_SIGNATURE"] = "TOKEN_INVALID_SIGNATURE";
    TokenErrorTypes["TOKEN_WRONG_ROLE"] = "TOKEN_WRONG_ROLE";
    TokenErrorTypes["TOKEN_REVOKED"] = "TOKEN_REVOKED";
    TokenErrorTypes["TOKEN_NOT_ACTIVE_YET"] = "TOKEN_NOT_ACTIVE_YET";
})(TokenErrorTypes || (exports.TokenErrorTypes = TokenErrorTypes = {}));
var UserErrorTypes;
(function (UserErrorTypes) {
    UserErrorTypes["USER_NOT_FOUND"] = "USER_NOT_FOUND";
    UserErrorTypes["USER_INACTIVE"] = "USER_INACTIVE";
    UserErrorTypes["INVALID_CREDENTIALS"] = "INVALID_CREDENTIALS";
    UserErrorTypes["USER_ROLE_MISMATCH"] = "USER_ROLE_MISMATCH";
    UserErrorTypes["USER_SUSPENDED"] = "USER_SUSPENDED";
    UserErrorTypes["USER_DELETED"] = "USER_DELETED";
})(UserErrorTypes || (exports.UserErrorTypes = UserErrorTypes = {}));
var PermissionErrorTypes;
(function (PermissionErrorTypes) {
    PermissionErrorTypes["ROLE_BLOCKED"] = "ROLE_BLOCKED";
    PermissionErrorTypes["INSUFFICIENT_PERMISSIONS"] = "INSUFFICIENT_PERMISSIONS";
    PermissionErrorTypes["ROLE_NOT_FOUND"] = "ROLE_NOT_FOUND";
    PermissionErrorTypes["PERMISSION_DENIED"] = "PERMISSION_DENIED";
})(PermissionErrorTypes || (exports.PermissionErrorTypes = PermissionErrorTypes = {}));
var SystemErrorTypes;
(function (SystemErrorTypes) {
    SystemErrorTypes["EXTERNAL_SERVICE_ERROR"] = "EXTERNAL_SERVICE_ERROR";
    SystemErrorTypes["DATABASE_ERROR"] = "DATABASE_ERROR";
    SystemErrorTypes["UNKNOWN_ERROR"] = "UNKNOWN_ERROR";
    SystemErrorTypes["SERVER_ERROR"] = "SERVER_ERROR";
    SystemErrorTypes["SERVICE_UNAVAILABLE"] = "SERVICE_UNAVAILABLE";
    SystemErrorTypes["MAINTENANCE_MODE"] = "MAINTENANCE_MODE";
    SystemErrorTypes["RATE_LIMIT_EXCEEDED"] = "RATE_LIMIT_EXCEEDED";
    SystemErrorTypes["CONFIGURATION_ERROR"] = "CONFIGURATION_ERROR";
})(SystemErrorTypes || (exports.SystemErrorTypes = SystemErrorTypes = {}));
var ValidationErrorTypes;
(function (ValidationErrorTypes) {
    ValidationErrorTypes["INVALID_DNI"] = "INVALID_DNI";
    ValidationErrorTypes["INVALID_GENDER"] = "INVALID_GENDER";
    ValidationErrorTypes["INVALID_PHONE"] = "INVALID_PHONE";
    ValidationErrorTypes["INVALID_EMAIL"] = "INVALID_EMAIL";
    ValidationErrorTypes["INVALID_USERNAME"] = "INVALID_USERNAME";
    ValidationErrorTypes["INVALID_NAME"] = "INVALID_NAME";
    ValidationErrorTypes["INVALID_LASTNAME"] = "INVALID_LASTNAME";
    ValidationErrorTypes["STRING_TOO_LONG"] = "STRING_TOO_LONG";
    ValidationErrorTypes["FIELD_REQUIRED"] = "FIELD_REQUIRED";
    ValidationErrorTypes["INVALID_FORMAT"] = "INVALID_FORMAT";
    ValidationErrorTypes["REQUIRED_FIELDS"] = "REQUIRED_FIELDS";
    ValidationErrorTypes["INVALID_REFERENCE"] = "INVALID_REFERENCE";
    ValidationErrorTypes["VALUE_ALREADY_EXISTS"] = "VALUE_ALREADY_EXISTS";
    ValidationErrorTypes["INVALID_DATE_FORMAT"] = "INVALID_DATE_FORMAT";
    ValidationErrorTypes["DATE_OUT_OF_RANGE"] = "DATE_OUT_OF_RANGE";
    ValidationErrorTypes["INVALID_TIME_FORMAT"] = "INVALID_TIME_FORMAT";
    ValidationErrorTypes["INVALID_ENUM_VALUE"] = "INVALID_ENUM_VALUE";
})(ValidationErrorTypes || (exports.ValidationErrorTypes = ValidationErrorTypes = {}));
var DataConflictErrorTypes;
(function (DataConflictErrorTypes) {
    DataConflictErrorTypes["VALUE_ALREADY_IN_USE"] = "CONFLICTO_VALOR_YA_EN_USO";
    DataConflictErrorTypes["RECORD_NOT_FOUND"] = "CONFLICTO_REGISTRO_NO_ENCONTRADO";
    DataConflictErrorTypes["RELATED_DATA_EXISTS"] = "CONFLICTO_DATOS_RELACIONADOS_EXISTEN";
    DataConflictErrorTypes["DATABASE_CONSTRAINT"] = "CONFLICTO_RESTRICCI\u00D3N_BASE_DATOS";
    DataConflictErrorTypes["CONCURRENT_MODIFICATION"] = "CONFLICTO_MODIFICACI\u00D3N_CONCURRENTE";
    DataConflictErrorTypes["VERSION_MISMATCH"] = "CONFLICTO_VERSI\u00D3N_NO_COINCIDE";
    DataConflictErrorTypes["DEPENDENCY_EXISTS"] = "CONFLICTO_DEPENDENCIA_EXISTE";
})(DataConflictErrorTypes || (exports.DataConflictErrorTypes = DataConflictErrorTypes = {}));
var FileErrorTypes;
(function (FileErrorTypes) {
    FileErrorTypes["FILE_MISSING"] = "FILE_MISSING";
    FileErrorTypes["INVALID_FILE_TYPE"] = "INVALID_FILE_TYPE";
    FileErrorTypes["FILE_TOO_LARGE"] = "FILE_TOO_LARGE";
    FileErrorTypes["FILE_UPLOAD_FAILED"] = "FILE_UPLOAD_FAILED";
    FileErrorTypes["FILE_DELETE_FAILED"] = "FILE_DELETE_FAILED";
    FileErrorTypes["FILE_CORRUPTED"] = "FILE_CORRUPTED";
    FileErrorTypes["FILE_PROCESSING_FAILED"] = "FILE_PROCESSING_FAILED";
    FileErrorTypes["INSUFFICIENT_STORAGE"] = "INSUFFICIENT_STORAGE";
})(FileErrorTypes || (exports.FileErrorTypes = FileErrorTypes = {}));
var AuthenticationErrorTypes;
(function (AuthenticationErrorTypes) {
    AuthenticationErrorTypes["MAX_ATTEMPTS_EXCEEDED"] = "MAX_ATTEMPTS_EXCEEDED";
    AuthenticationErrorTypes["VERIFICATION_FAILED"] = "VERIFICATION_FAILED";
    AuthenticationErrorTypes["CHALLENGE_REQUIRED"] = "CHALLENGE_REQUIRED";
    AuthenticationErrorTypes["OTP_INVALID"] = "OTP_INVALID";
    AuthenticationErrorTypes["ACCOUNT_LOCKED"] = "ACCOUNT_LOCKED";
    AuthenticationErrorTypes["TEMPORARY_BLOCKED"] = "TEMPORARY_BLOCKED";
    AuthenticationErrorTypes["OTP_EXPIRED"] = "OTP_EXPIRED";
    AuthenticationErrorTypes["OTP_ALREADY_USED"] = "OTP_ALREADY_USED";
    AuthenticationErrorTypes["AUTHENTICATION_REQUIRED"] = "AUTHENTICATION_REQUIRED";
})(AuthenticationErrorTypes || (exports.AuthenticationErrorTypes = AuthenticationErrorTypes = {}));
var DataErrorTypes;
(function (DataErrorTypes) {
    DataErrorTypes["RECORD_NOT_FOUND"] = "RECORD_NOT_FOUND";
    DataErrorTypes["NO_DATA_AVAILABLE"] = "NO_DATA_AVAILABLE";
    DataErrorTypes["DATA_NOT_EXISTS"] = "DATA_NOT_EXISTS";
    DataErrorTypes["INVALID_DATA_FORMAT"] = "INVALID_DATA_FORMAT";
    DataErrorTypes["DATA_CORRUPTED"] = "DATA_CORRUPTED";
    DataErrorTypes["DATA_INCONSISTENT"] = "DATA_INCONSISTENT";
})(DataErrorTypes || (exports.DataErrorTypes = DataErrorTypes = {}));
var NetworkErrorTypes;
(function (NetworkErrorTypes) {
    NetworkErrorTypes["NETWORK_ERROR"] = "NETWORK_ERROR";
    NetworkErrorTypes["CONNECTION_TIMEOUT"] = "CONNECTION_TIMEOUT";
    NetworkErrorTypes["TIMEOUT_ERROR"] = "TIMEOUT_ERROR";
    NetworkErrorTypes["CONNECTION_REFUSED"] = "CONNECTION_REFUSED";
    NetworkErrorTypes["DNS_ERROR"] = "DNS_ERROR";
    NetworkErrorTypes["OFFLINE"] = "OFFLINE";
    NetworkErrorTypes["POOR_CONNECTION"] = "POOR_CONNECTION";
})(NetworkErrorTypes || (exports.NetworkErrorTypes = NetworkErrorTypes = {}));
var SyncErrorTypes;
(function (SyncErrorTypes) {
    SyncErrorTypes["SYNC_ERROR"] = "SYNC_ERROR";
    SyncErrorTypes["SYNC_CONFLICT"] = "SYNC_CONFLICT";
    SyncErrorTypes["SYNC_TIMEOUT"] = "SYNC_TIMEOUT";
    SyncErrorTypes["SYNC_FAILED"] = "SYNC_FAILED";
    SyncErrorTypes["SYNC_INTERRUPTED"] = "SYNC_INTERRUPTED";
    SyncErrorTypes["SYNC_DATA_MISMATCH"] = "SYNC_DATA_MISMATCH";
})(SyncErrorTypes || (exports.SyncErrorTypes = SyncErrorTypes = {}));
var CacheErrorTypes;
(function (CacheErrorTypes) {
    CacheErrorTypes["CACHE_ERROR"] = "CACHE_ERROR";
    CacheErrorTypes["CACHE_MISS"] = "CACHE_MISS";
    CacheErrorTypes["CACHE_EXPIRED"] = "CACHE_EXPIRED";
    CacheErrorTypes["CACHE_CORRUPTED"] = "CACHE_CORRUPTED";
    CacheErrorTypes["CACHE_FULL"] = "CACHE_FULL";
    CacheErrorTypes["CACHE_UNAVAILABLE"] = "CACHE_UNAVAILABLE";
})(CacheErrorTypes || (exports.CacheErrorTypes = CacheErrorTypes = {}));
var StorageErrorTypes;
(function (StorageErrorTypes) {
    StorageErrorTypes["STORAGE_FULL"] = "STORAGE_FULL";
    StorageErrorTypes["STORAGE_ERROR"] = "STORAGE_ERROR";
    StorageErrorTypes["STORAGE_UNAVAILABLE"] = "STORAGE_UNAVAILABLE";
    StorageErrorTypes["STORAGE_CORRUPTED"] = "STORAGE_CORRUPTED";
    StorageErrorTypes["QUOTA_EXCEEDED"] = "QUOTA_EXCEEDED";
    StorageErrorTypes["INDEXEDDB_ERROR"] = "INDEXEDDB_ERROR";
})(StorageErrorTypes || (exports.StorageErrorTypes = StorageErrorTypes = {}));
var AttendanceErrorTypes;
(function (AttendanceErrorTypes) {
    AttendanceErrorTypes["ATTENDANCE_ALREADY_MARKED"] = "ATTENDANCE_ALREADY_MARKED";
    AttendanceErrorTypes["ATTENDANCE_WINDOW_CLOSED"] = "ATTENDANCE_WINDOW_CLOSED";
    AttendanceErrorTypes["INVALID_ATTENDANCE_TIME"] = "INVALID_ATTENDANCE_TIME";
    AttendanceErrorTypes["ATTENDANCE_NOT_FOUND"] = "ATTENDANCE_NOT_FOUND";
    AttendanceErrorTypes["ATTENDANCE_LOCKED"] = "ATTENDANCE_LOCKED";
    AttendanceErrorTypes["SCHEDULE_CONFLICT"] = "SCHEDULE_CONFLICT";
})(AttendanceErrorTypes || (exports.AttendanceErrorTypes = AttendanceErrorTypes = {}));
exports.ErrorGroups = {
    CRITICAL_ERRORS: [
        TokenErrorTypes.TOKEN_EXPIRED,
        TokenErrorTypes.TOKEN_REVOKED,
        AuthenticationErrorTypes.ACCOUNT_LOCKED,
        UserErrorTypes.USER_SUSPENDED,
        UserErrorTypes.USER_DELETED,
    ],
    CONNECTIVITY_ERRORS: [
        NetworkErrorTypes.NETWORK_ERROR,
        NetworkErrorTypes.CONNECTION_TIMEOUT,
        NetworkErrorTypes.TIMEOUT_ERROR,
        NetworkErrorTypes.CONNECTION_REFUSED,
        NetworkErrorTypes.OFFLINE,
    ],
    SYNC_REQUIRED_ERRORS: [
        SyncErrorTypes.SYNC_CONFLICT,
        SyncErrorTypes.SYNC_DATA_MISMATCH,
        DataErrorTypes.DATA_INCONSISTENT,
        CacheErrorTypes.CACHE_CORRUPTED,
    ],
    STORAGE_CLEANUP_ERRORS: [
        StorageErrorTypes.STORAGE_FULL,
        StorageErrorTypes.QUOTA_EXCEEDED,
        CacheErrorTypes.CACHE_FULL,
    ],
    USER_CORRECTABLE_ERRORS: [
        ...Object.values(ValidationErrorTypes),
        RequestErrorTypes.INVALID_PARAMETERS,
        RequestErrorTypes.MISSING_PARAMETERS,
    ],
};
