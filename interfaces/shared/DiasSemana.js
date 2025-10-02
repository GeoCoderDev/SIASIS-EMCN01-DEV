"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.diasSemanaTextosCortos = exports.diasSemanaTextos = exports.DiasSemana = void 0;
var DiasSemana;
(function (DiasSemana) {
    DiasSemana[DiasSemana["Domingo"] = 0] = "Domingo";
    DiasSemana[DiasSemana["Lunes"] = 1] = "Lunes";
    DiasSemana[DiasSemana["Martes"] = 2] = "Martes";
    DiasSemana[DiasSemana["Miercoles"] = 3] = "Miercoles";
    DiasSemana[DiasSemana["Jueves"] = 4] = "Jueves";
    DiasSemana[DiasSemana["Viernes"] = 5] = "Viernes";
    DiasSemana[DiasSemana["Sabado"] = 6] = "Sabado";
})(DiasSemana || (exports.DiasSemana = DiasSemana = {}));
exports.diasSemanaTextos = {
    [DiasSemana.Domingo]: "Domingo",
    [DiasSemana.Lunes]: "Lunes",
    [DiasSemana.Martes]: "Martes",
    [DiasSemana.Miercoles]: "Miércoles",
    [DiasSemana.Jueves]: "Jueves",
    [DiasSemana.Viernes]: "Viernes",
    [DiasSemana.Sabado]: "Sábado",
};
exports.diasSemanaTextosCortos = {
    [DiasSemana.Domingo]: "Dom",
    [DiasSemana.Lunes]: "Lun",
    [DiasSemana.Martes]: "Mar",
    [DiasSemana.Miercoles]: "Mié",
    [DiasSemana.Jueves]: "Jue",
    [DiasSemana.Viernes]: "Vie",
    [DiasSemana.Sabado]: "Sáb",
};
