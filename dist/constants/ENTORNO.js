"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.ENTORNO = void 0;
const Entornos_1 = require("../interfaces/shared/Entornos");
exports.ENTORNO = process.env.ENTORNO || Entornos_1.Entorno.CERTIFICACION;
