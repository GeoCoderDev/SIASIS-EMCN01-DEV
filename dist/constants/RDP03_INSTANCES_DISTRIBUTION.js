"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.RDP03_INSTANCES_DATABASE_URL_MAP = void 0;
const RDP03Instancias_1 = require("../interfaces/shared/RDP03Instancias");
exports.RDP03_INSTANCES_DATABASE_URL_MAP = new Map([
    [RDP03Instancias_1.RDP03.INS1, process.env.RDP03_INS1_DATABASE_URL],
    [RDP03Instancias_1.RDP03.INS2, process.env.RDP03_INS2_DATABASE_URL],
    [RDP03Instancias_1.RDP03.INS3, process.env.RDP03_INS3_DATABASE_URL],
    [RDP03Instancias_1.RDP03.INS4, process.env.RDP03_INS4_DATABASE_URL],
    [RDP03Instancias_1.RDP03.INS5, process.env.RDP03_INS5_DATABASE_URL],
]);
