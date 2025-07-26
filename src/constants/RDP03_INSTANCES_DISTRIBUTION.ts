import { RDP03 } from "../interfaces/shared/RDP03Instancias";

// Mapa que relaciona cada instancia con su URL de conexión
export const RDP03_INSTANCES_DATABASE_URL_MAP: Map<RDP03, string | undefined> =
  new Map([
    [RDP03.INS1, process.env.RDP03_INS1_DATABASE_URL],
    [RDP03.INS2, process.env.RDP03_INS2_DATABASE_URL],
    [RDP03.INS3, process.env.RDP03_INS3_DATABASE_URL],
    [RDP03.INS4, process.env.RDP03_INS4_DATABASE_URL],
    [RDP03.INS5, process.env.RDP03_INS5_DATABASE_URL],
    // Aquí se pueden agregar más instancias fácilmente en el futuro
  ]);
