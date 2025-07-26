import { Entorno } from "../interfaces/shared/Entornos";

export const ENTORNO =
  (process.env.ENTORNO! as Entorno) || Entorno.CERTIFICACION;
