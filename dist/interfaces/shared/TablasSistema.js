"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.TablasSistema = exports.TablasLocal = exports.TablasRemoto = void 0;
var TablasRemoto;
(function (TablasRemoto) {
    TablasRemoto["Tabla_Directivos"] = "T_Directivos";
    TablasRemoto["Tabla_Auxiliares"] = "T_Auxiliares";
    TablasRemoto["Tabla_Profesores_Primaria"] = "T_Profesores_Primaria";
    TablasRemoto["Tabla_Profesores_Secundaria"] = "T_Profesores_Secundaria";
    TablasRemoto["Tabla_Personal_Administrativo"] = "T_Personal_Administrativo";
    TablasRemoto["Tabla_Responsables"] = "T_Responsables";
    TablasRemoto["Tabla_Estudiantes"] = "T_Estudiantes";
    TablasRemoto["Tabla_Relaciones_E_R"] = "T_Relaciones_E_R";
    TablasRemoto["Tabla_Asistencia_Primaria_1"] = "T_A_E_P_1";
    TablasRemoto["Tabla_Asistencia_Primaria_2"] = "T_A_E_P_2";
    TablasRemoto["Tabla_Asistencia_Primaria_3"] = "T_A_E_P_3";
    TablasRemoto["Tabla_Asistencia_Primaria_4"] = "T_A_E_P_4";
    TablasRemoto["Tabla_Asistencia_Primaria_5"] = "T_A_E_P_5";
    TablasRemoto["Tabla_Asistencia_Primaria_6"] = "T_A_E_P_6";
    TablasRemoto["Tabla_Asistencia_Secundaria_1"] = "T_A_E_S_1";
    TablasRemoto["Tabla_Asistencia_Secundaria_2"] = "T_A_E_S_2";
    TablasRemoto["Tabla_Asistencia_Secundaria_3"] = "T_A_E_S_3";
    TablasRemoto["Tabla_Asistencia_Secundaria_4"] = "T_A_E_S_4";
    TablasRemoto["Tabla_Asistencia_Secundaria_5"] = "T_A_E_S_5";
    TablasRemoto["Tabla_Aulas"] = "T_Aulas";
    TablasRemoto["Tabla_Cursos_Horario"] = "T_Cursos_Horario";
    TablasRemoto["Tabla_Eventos"] = "T_Eventos";
    TablasRemoto["Tabla_Comunicados"] = "T_Comunicados";
    TablasRemoto["Tabla_Control_Entrada_Profesores_Primaria"] = "T_Control_Entrada_Mensual_Profesores_Primaria";
    TablasRemoto["Tabla_Control_Salida_Profesores_Primaria"] = "T_Control_Salida_Mensual_Profesores_Primaria";
    TablasRemoto["Tabla_Control_Entrada_Profesores_Secundaria"] = "T_Control_Entrada_Mensual_Profesores_Secundaria";
    TablasRemoto["Tabla_Control_Salida_Profesores_Secundaria"] = "T_Control_Salida_Mensual_Profesores_Secundaria";
    TablasRemoto["Tabla_Control_Entrada_Auxiliar"] = "T_Control_Entrada_Mensual_Auxiliar";
    TablasRemoto["Tabla_Control_Salida_Auxiliar"] = "T_Control_Salida_Mensual_Auxiliar";
    TablasRemoto["Tabla_Control_Entrada_Personal_Administrativo"] = "T_Control_Entrada_Mensual_Personal_Administrativo";
    TablasRemoto["Tabla_Control_Salida_Personal_Administrativo"] = "T_Control_Salida_Mensual_Personal_Administrativo";
    TablasRemoto["Tabla_Fechas_Importantes"] = "T_Fechas_Importantes";
    TablasRemoto["Tabla_Horarios_Asistencia"] = "T_Horarios_Asistencia";
    TablasRemoto["Tabla_Ajustes_Sistema"] = "T_Ajustes_Generales_Sistema";
    TablasRemoto["Tabla_Bloqueo_Roles"] = "T_Bloqueo_Roles";
    TablasRemoto["Tabla_Registro_Fallos"] = "T_Registro_Fallos_Sistema";
    TablasRemoto["Tabla_Codigos_OTP"] = "T_Codigos_OTP";
    TablasRemoto["Tabla_Ultima_Modificacion"] = "T_Ultima_Modificacion_Tablas";
})(TablasRemoto || (exports.TablasRemoto = TablasRemoto = {}));
var TablasLocal;
(function (TablasLocal) {
    TablasLocal["Tabla_Auxiliares"] = "auxiliares";
    TablasLocal["Tabla_Profesores_Primaria"] = "profesores_primaria";
    TablasLocal["Tabla_Profesores_Secundaria"] = "profesores_secundaria";
    TablasLocal["Tabla_Personal_Administrativo"] = "personal_administrativo";
    TablasLocal["Tabla_Responsables"] = "responsables";
    TablasLocal["Tabla_Estudiantes"] = "estudiantes";
    TablasLocal["Tabla_Relaciones_E_R"] = "relaciones_e_r";
    TablasLocal["Tabla_Asistencia_Primaria_1"] = "asistencias_e_p_1";
    TablasLocal["Tabla_Asistencia_Primaria_2"] = "asistencias_e_p_2";
    TablasLocal["Tabla_Asistencia_Primaria_3"] = "asistencias_e_p_3";
    TablasLocal["Tabla_Asistencia_Primaria_4"] = "asistencias_e_p_4";
    TablasLocal["Tabla_Asistencia_Primaria_5"] = "asistencias_e_p_5";
    TablasLocal["Tabla_Asistencia_Primaria_6"] = "asistencias_e_p_6";
    TablasLocal["Tabla_Asistencia_Secundaria_1"] = "asistencias_e_s_1";
    TablasLocal["Tabla_Asistencia_Secundaria_2"] = "asistencias_e_s_2";
    TablasLocal["Tabla_Asistencia_Secundaria_3"] = "asistencias_e_s_3";
    TablasLocal["Tabla_Asistencia_Secundaria_4"] = "asistencias_e_s_4";
    TablasLocal["Tabla_Asistencia_Secundaria_5"] = "asistencias_e_s_5";
    TablasLocal["Tabla_Aulas"] = "aulas";
    TablasLocal["Tabla_Cursos_Horario"] = "cursos_horario";
    TablasLocal["Tabla_Eventos"] = "eventos";
    TablasLocal["Tabla_Comunicados"] = "comunicados";
    TablasLocal["Tabla_Control_Entrada_Profesores_Primaria"] = "control_entrada_profesores_primaria";
    TablasLocal["Tabla_Control_Salida_Profesores_Primaria"] = "control_salida_profesores_primaria";
    TablasLocal["Tabla_Control_Entrada_Profesores_Secundaria"] = "control_entrada_profesores_secundaria";
    TablasLocal["Tabla_Control_Salida_Profesores_Secundaria"] = "control_salida_profesores_secundaria";
    TablasLocal["Tabla_Control_Entrada_Auxiliar"] = "control_entrada_auxiliar";
    TablasLocal["Tabla_Control_Salida_Auxiliar"] = "control_salida_auxiliar";
    TablasLocal["Tabla_Control_Entrada_Personal_Administrativo"] = "control_entrada_personal_administrativo";
    TablasLocal["Tabla_Control_Salida_Personal_Administrativo"] = "control_salida_personal_administrativo";
    TablasLocal["Tabla_Fechas_Importantes"] = "fechas_importantes";
    TablasLocal["Tabla_Horarios_Asistencia"] = "horarios_asistencia";
    TablasLocal["Tabla_Ajustes_Sistema"] = "ajustes_generales_sistema";
    TablasLocal["Tabla_Bloqueo_Roles"] = "bloqueo_roles";
    TablasLocal["Tabla_Registro_Fallos"] = "registro_fallos_sistema";
    TablasLocal["Tabla_Codigos_OTP"] = "codigos_otp";
    TablasLocal["Tabla_Ultima_Modificacion"] = "ultima_modificacion_tablas";
    TablasLocal["Tabla_Ultima_Actualizacion"] = "ultima_actualizacion_tablas_locales";
    TablasLocal["Tabla_Datos_Usuario"] = "user_data";
    TablasLocal["Tabla_Solicitudes_Offline"] = "offline_requests";
    TablasLocal["Tabla_Metadatos_Sistema"] = "system_meta";
    TablasLocal["Tabla_Asistencias_Tomadas_Hoy"] = "asistencias_tomadas_hoy";
})(TablasLocal || (exports.TablasLocal = TablasLocal = {}));
exports.TablasSistema = {
    DIRECTIVOS: {
        nombreRemoto: TablasRemoto.Tabla_Directivos,
        descripcion: "Directores y subdirectores de la institución",
        sincronizable: false,
    },
    AUXILIARES: {
        nombreRemoto: TablasRemoto.Tabla_Auxiliares,
        nombreLocal: TablasLocal.Tabla_Auxiliares,
        descripcion: "Personal auxiliar de la institución",
        sincronizable: true,
    },
    PROFESORES_PRIMARIA: {
        nombreRemoto: TablasRemoto.Tabla_Profesores_Primaria,
        nombreLocal: TablasLocal.Tabla_Profesores_Primaria,
        descripcion: "Profesores del nivel primaria",
        sincronizable: true,
    },
    PROFESORES_SECUNDARIA: {
        nombreRemoto: TablasRemoto.Tabla_Profesores_Secundaria,
        nombreLocal: TablasLocal.Tabla_Profesores_Secundaria,
        descripcion: "Profesores del nivel secundaria",
        sincronizable: true,
    },
    PERSONAL_ADMINISTRATIVO: {
        nombreRemoto: TablasRemoto.Tabla_Personal_Administrativo,
        nombreLocal: TablasLocal.Tabla_Personal_Administrativo,
        descripcion: "Personal administrativo de la institución",
        sincronizable: true,
    },
    RESPONSABLES: {
        nombreRemoto: TablasRemoto.Tabla_Responsables,
        nombreLocal: TablasLocal.Tabla_Responsables,
        descripcion: "Padres de familia o apoderados",
        sincronizable: true,
    },
    ESTUDIANTES: {
        nombreRemoto: TablasRemoto.Tabla_Estudiantes,
        nombreLocal: TablasLocal.Tabla_Estudiantes,
        descripcion: "Estudiantes de la institución",
        sincronizable: true,
    },
    RELACIONES_E_R: {
        nombreRemoto: TablasRemoto.Tabla_Relaciones_E_R,
        nombreLocal: TablasLocal.Tabla_Relaciones_E_R,
        descripcion: "Relaciones entre estudiantes y responsables",
        sincronizable: true,
    },
    ASISTENCIA_P_1: {
        nombreRemoto: TablasRemoto.Tabla_Asistencia_Primaria_1,
        nombreLocal: TablasLocal.Tabla_Asistencia_Primaria_1,
        descripcion: "Asistencia de estudiantes de 1° de primaria",
        sincronizable: true,
    },
    ASISTENCIA_P_2: {
        nombreRemoto: TablasRemoto.Tabla_Asistencia_Primaria_2,
        nombreLocal: TablasLocal.Tabla_Asistencia_Primaria_2,
        descripcion: "Asistencia de estudiantes de 2° de primaria",
        sincronizable: true,
    },
    ASISTENCIA_P_3: {
        nombreRemoto: TablasRemoto.Tabla_Asistencia_Primaria_3,
        nombreLocal: TablasLocal.Tabla_Asistencia_Primaria_3,
        descripcion: "Asistencia de estudiantes de 3° de primaria",
        sincronizable: true,
    },
    ASISTENCIA_P_4: {
        nombreRemoto: TablasRemoto.Tabla_Asistencia_Primaria_4,
        nombreLocal: TablasLocal.Tabla_Asistencia_Primaria_4,
        descripcion: "Asistencia de estudiantes de 4° de primaria",
        sincronizable: true,
    },
    ASISTENCIA_P_5: {
        nombreRemoto: TablasRemoto.Tabla_Asistencia_Primaria_5,
        nombreLocal: TablasLocal.Tabla_Asistencia_Primaria_5,
        descripcion: "Asistencia de estudiantes de 5° de primaria",
        sincronizable: true,
    },
    ASISTENCIA_P_6: {
        nombreRemoto: TablasRemoto.Tabla_Asistencia_Primaria_6,
        nombreLocal: TablasLocal.Tabla_Asistencia_Primaria_6,
        descripcion: "Asistencia de estudiantes de 6° de primaria",
        sincronizable: true,
    },
    ASISTENCIA_S_1: {
        nombreRemoto: TablasRemoto.Tabla_Asistencia_Secundaria_1,
        nombreLocal: TablasLocal.Tabla_Asistencia_Secundaria_1,
        descripcion: "Asistencia de estudiantes de 1° de secundaria",
        sincronizable: true,
    },
    ASISTENCIA_S_2: {
        nombreRemoto: TablasRemoto.Tabla_Asistencia_Secundaria_2,
        nombreLocal: TablasLocal.Tabla_Asistencia_Secundaria_2,
        descripcion: "Asistencia de estudiantes de 2° de secundaria",
        sincronizable: true,
    },
    ASISTENCIA_S_3: {
        nombreRemoto: TablasRemoto.Tabla_Asistencia_Secundaria_3,
        nombreLocal: TablasLocal.Tabla_Asistencia_Secundaria_3,
        descripcion: "Asistencia de estudiantes de 3° de secundaria",
        sincronizable: true,
    },
    ASISTENCIA_S_4: {
        nombreRemoto: TablasRemoto.Tabla_Asistencia_Secundaria_4,
        nombreLocal: TablasLocal.Tabla_Asistencia_Secundaria_4,
        descripcion: "Asistencia de estudiantes de 4° de secundaria",
        sincronizable: true,
    },
    ASISTENCIA_S_5: {
        nombreRemoto: TablasRemoto.Tabla_Asistencia_Secundaria_5,
        nombreLocal: TablasLocal.Tabla_Asistencia_Secundaria_5,
        descripcion: "Asistencia de estudiantes de 5° de secundaria",
        sincronizable: true,
    },
    AULAS: {
        nombreRemoto: TablasRemoto.Tabla_Aulas,
        nombreLocal: TablasLocal.Tabla_Aulas,
        descripcion: "Aulas o secciones de la institución",
        sincronizable: true,
    },
    CURSOS_HORARIO: {
        nombreRemoto: TablasRemoto.Tabla_Cursos_Horario,
        nombreLocal: TablasLocal.Tabla_Cursos_Horario,
        descripcion: "Horarios de cursos",
        sincronizable: true,
    },
    EVENTOS: {
        nombreRemoto: TablasRemoto.Tabla_Eventos,
        nombreLocal: TablasLocal.Tabla_Eventos,
        descripcion: "Eventos y celebraciones del calendario escolar",
        sincronizable: true,
    },
    COMUNICADOS: {
        nombreRemoto: TablasRemoto.Tabla_Comunicados,
        nombreLocal: TablasLocal.Tabla_Comunicados,
        descripcion: "Comunicados institucionales",
        sincronizable: true,
    },
    CONTROL_ENTRADA_PROF_PRIMARIA: {
        nombreRemoto: TablasRemoto.Tabla_Control_Entrada_Profesores_Primaria,
        nombreLocal: TablasLocal.Tabla_Control_Entrada_Profesores_Primaria,
        descripcion: "Control de entrada de profesores de primaria",
        sincronizable: true,
    },
    CONTROL_SALIDA_PROF_PRIMARIA: {
        nombreRemoto: TablasRemoto.Tabla_Control_Salida_Profesores_Primaria,
        nombreLocal: TablasLocal.Tabla_Control_Salida_Profesores_Primaria,
        descripcion: "Control de salida de profesores de primaria",
        sincronizable: true,
    },
    CONTROL_ENTRADA_PROF_SECUNDARIA: {
        nombreRemoto: TablasRemoto.Tabla_Control_Entrada_Profesores_Secundaria,
        nombreLocal: TablasLocal.Tabla_Control_Entrada_Profesores_Secundaria,
        descripcion: "Control de entrada de profesores de secundaria",
        sincronizable: true,
    },
    CONTROL_SALIDA_PROF_SECUNDARIA: {
        nombreRemoto: TablasRemoto.Tabla_Control_Salida_Profesores_Secundaria,
        nombreLocal: TablasLocal.Tabla_Control_Salida_Profesores_Secundaria,
        descripcion: "Control de salida de profesores de secundaria",
        sincronizable: true,
    },
    CONTROL_ENTRADA_AUXILIAR: {
        nombreRemoto: TablasRemoto.Tabla_Control_Entrada_Auxiliar,
        nombreLocal: TablasLocal.Tabla_Control_Entrada_Auxiliar,
        descripcion: "Control de entrada de auxiliares",
        sincronizable: true,
    },
    CONTROL_SALIDA_AUXILIAR: {
        nombreRemoto: TablasRemoto.Tabla_Control_Salida_Auxiliar,
        nombreLocal: TablasLocal.Tabla_Control_Salida_Auxiliar,
        descripcion: "Control de salida de auxiliares",
        sincronizable: true,
    },
    CONTROL_ENTRADA_ADMIN: {
        nombreRemoto: TablasRemoto.Tabla_Control_Entrada_Personal_Administrativo,
        nombreLocal: TablasLocal.Tabla_Control_Entrada_Personal_Administrativo,
        descripcion: "Control de entrada de personal administrativo",
        sincronizable: true,
    },
    CONTROL_SALIDA_ADMIN: {
        nombreRemoto: TablasRemoto.Tabla_Control_Salida_Personal_Administrativo,
        nombreLocal: TablasLocal.Tabla_Control_Salida_Personal_Administrativo,
        descripcion: "Control de salida de personal administrativo",
        sincronizable: true,
    },
    FECHAS_IMPORTANTES: {
        nombreRemoto: TablasRemoto.Tabla_Fechas_Importantes,
        nombreLocal: TablasLocal.Tabla_Fechas_Importantes,
        descripcion: "Fechas importantes del año escolar",
        sincronizable: true,
    },
    HORARIOS_ASISTENCIA: {
        nombreRemoto: TablasRemoto.Tabla_Horarios_Asistencia,
        nombreLocal: TablasLocal.Tabla_Horarios_Asistencia,
        descripcion: "Configuración de horarios para toma de asistencia",
        sincronizable: true,
    },
    AJUSTES_SISTEMA: {
        nombreRemoto: TablasRemoto.Tabla_Ajustes_Sistema,
        nombreLocal: TablasLocal.Tabla_Ajustes_Sistema,
        descripcion: "Ajustes generales del sistema",
        sincronizable: true,
    },
    BLOQUEO_ROLES: {
        nombreRemoto: TablasRemoto.Tabla_Bloqueo_Roles,
        nombreLocal: TablasLocal.Tabla_Bloqueo_Roles,
        descripcion: "Bloqueo temporal de roles en el sistema",
        sincronizable: true,
    },
    REGISTRO_FALLOS: {
        nombreRemoto: TablasRemoto.Tabla_Registro_Fallos,
        nombreLocal: TablasLocal.Tabla_Registro_Fallos,
        descripcion: "Registro de errores y fallos del sistema",
        sincronizable: false,
    },
    CODIGOS_OTP: {
        nombreRemoto: TablasRemoto.Tabla_Codigos_OTP,
        nombreLocal: TablasLocal.Tabla_Codigos_OTP,
        descripcion: "Códigos de verificación de un solo uso",
        sincronizable: false,
    },
    ULTIMA_MODIFICACION: {
        nombreRemoto: TablasRemoto.Tabla_Ultima_Modificacion,
        nombreLocal: TablasLocal.Tabla_Ultima_Modificacion,
        descripcion: "Registro de últimas modificaciones de cada tabla",
        sincronizable: true,
    },
    ULTIMA_ACTUALIZACION_LOCAL: {
        nombreLocal: TablasLocal.Tabla_Ultima_Actualizacion,
        descripcion: "Registro de última actualización de tablas locales",
        sincronizable: false,
    },
    DATOS_USUARIO: {
        nombreLocal: TablasLocal.Tabla_Datos_Usuario,
        descripcion: "Datos de sesión del usuario actual",
        sincronizable: false,
    },
    SOLICITUDES_OFFLINE: {
        nombreLocal: TablasLocal.Tabla_Solicitudes_Offline,
        descripcion: "Cola de solicitudes pendientes en modo offline",
        sincronizable: false,
    },
    METADATOS_SISTEMA: {
        nombreLocal: TablasLocal.Tabla_Metadatos_Sistema,
        descripcion: "Metadatos y configuraciones del sistema local",
        sincronizable: false,
    },
    ASISTENCIAS_TOMADAS_HOY: {
        nombreLocal: TablasLocal.Tabla_Asistencias_Tomadas_Hoy,
        descripcion: "Cache temporal de asistencias consultadas desde Redis para el día actual",
        sincronizable: false,
    },
};
exports.default = exports.TablasSistema;
