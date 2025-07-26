import { MongoClient, MongoClientOptions } from "mongodb";
import { RDP03 } from "../../interfaces/shared/RDP03Instancias";
import { RDP03_INSTANCES_DATABASE_URL_MAP } from "../../constants/RDP03_INSTANCES_DISTRIBUTION";
import { MongoOperation } from "../../interfaces/shared/EMCN01/EMCN01Payload";

// Tipos para los resultados
interface ReplicationResult {
  instancia: string;
  success: boolean;
  operacion: string;
  coleccion: string;
  documentosAfectados?: number;
  error?: string;
  duracion?: number;
}

// Configuración MongoDB optimizada para replicación
const mongoOptions: MongoClientOptions = {
  maxPoolSize: parseInt(process.env.MONGO_MAX_POOL_SIZE || "5", 10),
  minPoolSize: parseInt(process.env.MONGO_MIN_POOL_SIZE || "1", 10),
  maxIdleTimeMS: 30000,
  serverSelectionTimeoutMS: parseInt(
    process.env.MONGO_SERVER_SELECTION_TIMEOUT || "5000",
    10
  ),
  connectTimeoutMS: parseInt(
    process.env.MONGO_CONNECTION_TIMEOUT || "10000",
    10
  ),
  heartbeatFrequencyMS: 10000,
  retryWrites: true,
  retryReads: false, // Para replicación, no reintentar lecturas
};

// Recuperar datos del evento
const mongoOperationJson = process.env.MONGO_OPERATION;
const instanciasAActualizarJson = process.env.INSTANCIAS_A_ACTUALIZAR;
const timestamp = process.env.TIMESTAMP;

if (!mongoOperationJson) {
  console.error("Error: No se proporcionó la operación MongoDB");
  process.exit(1);
}

// Parsear parámetros
let mongoOperation: MongoOperation;
let instanciasAActualizar: RDP03[] = [];

try {
  mongoOperation = JSON.parse(mongoOperationJson);

  if (instanciasAActualizarJson) {
    instanciasAActualizar = JSON.parse(instanciasAActualizarJson) as RDP03[];
  }
} catch (error) {
  console.error("Error al parsear parámetros:", error);
  process.exit(1);
}

// Función para ejecutar una operación MongoDB
async function executeMongoOperation(
  client: MongoClient,
  operation: MongoOperation,
  dbName: string = "siasis_asuncion_8"
): Promise<{ success: boolean; result?: any; error?: string }> {
  try {
    const db = client.db(dbName);
    const collection = db.collection(operation.collection);

    let result: any;

    switch (operation.operation) {
      case "insertOne":
        result = await collection.insertOne(operation.data, operation.options);
        return {
          success: true,
          result: {
            insertedCount: 1,
            insertedId: result.insertedId,
          },
        };

      case "insertMany":
        result = await collection.insertMany(operation.data, operation.options);
        return {
          success: true,
          result: {
            insertedCount: result.insertedCount,
            insertedIds: result.insertedIds,
          },
        };

      case "updateOne":
        result = await collection.updateOne(
          operation.filter || {},
          operation.data,
          operation.options
        );
        return {
          success: true,
          result: {
            matchedCount: result.matchedCount,
            modifiedCount: result.modifiedCount,
            upsertedCount: result.upsertedCount,
          },
        };

      case "updateMany":
        result = await collection.updateMany(
          operation.filter || {},
          operation.data,
          operation.options
        );
        return {
          success: true,
          result: {
            matchedCount: result.matchedCount,
            modifiedCount: result.modifiedCount,
            upsertedCount: result.upsertedCount,
          },
        };

      case "deleteOne":
        result = await collection.deleteOne(
          operation.filter || {},
          operation.options
        );
        return {
          success: true,
          result: {
            deletedCount: result.deletedCount,
          },
        };

      case "deleteMany":
        result = await collection.deleteMany(
          operation.filter || {},
          operation.options
        );
        return {
          success: true,
          result: {
            deletedCount: result.deletedCount,
          },
        };

      case "replaceOne":
        result = await collection.replaceOne(
          operation.filter || {},
          operation.data,
          operation.options
        );
        return {
          success: true,
          result: {
            matchedCount: result.matchedCount,
            modifiedCount: result.modifiedCount,
            upsertedCount: result.upsertedCount,
          },
        };

      // Operaciones de lectura (no deberían llegar aquí en replicación, pero por completitud)
      case "find":
        result = await collection
          .find(operation.filter || {}, operation.options)
          .toArray();
        return {
          success: true,
          result: {
            documents: result,
            count: result.length,
          },
        };

      case "findOne":
        result = await collection.findOne(
          operation.filter || {},
          operation.options
        );
        return {
          success: true,
          result: {
            document: result,
          },
        };

      case "aggregate":
        result = await collection
          .aggregate(operation.pipeline || [], operation.options)
          .toArray();
        return {
          success: true,
          result: {
            documents: result,
            count: result.length,
          },
        };

      case "countDocuments":
        result = await collection.countDocuments(
          operation.filter || {},
          operation.options
        );
        return {
          success: true,
          result: {
            count: result,
          },
        };

      default:
        return {
          success: false,
          error: `Operación no soportada: ${operation.operation}`,
        };
    }
  } catch (error: any) {
    return {
      success: false,
      error: error.message,
    };
  }
}

// Función para obtener el número de documentos afectados del resultado
function getAffectedDocumentsCount(operationType: string, result: any): number {
  switch (operationType) {
    case "insertOne":
      return result.insertedCount || 0;
    case "insertMany":
      return result.insertedCount || 0;
    case "updateOne":
    case "updateMany":
      return result.modifiedCount || 0;
    case "deleteOne":
    case "deleteMany":
      return result.deletedCount || 0;
    case "replaceOne":
      return result.modifiedCount || 0;
    case "find":
    case "aggregate":
      return result.count || 0;
    case "countDocuments":
      return result.count || 0;
    default:
      return 0;
  }
}

async function replicateToMongoDBInstances(): Promise<void> {
  console.log("🚀 Iniciando replicación MongoDB EMCN01");
  console.log(`📊 Timestamp de operación: ${timestamp}`);
  console.log(`🎯 Instancias a actualizar: ${instanciasAActualizar.length}`);
  console.log(
    `🔧 Operación a replicar: ${mongoOperation.operation} en colección ${mongoOperation.collection}`
  );

  // Mostrar detalles de la operación si estamos en modo debug
  if (process.env.ENTORNO === "D") {
    console.log("📝 Detalles de la operación:");
    console.log(`   - Operación: ${mongoOperation.operation}`);
    console.log(`   - Colección: ${mongoOperation.collection}`);
    if (mongoOperation.filter) {
      console.log(`   - Filtro: ${JSON.stringify(mongoOperation.filter)}`);
    }
    if (mongoOperation.data) {
      console.log(
        `   - Datos: ${JSON.stringify(mongoOperation.data).substring(0, 200)}${
          JSON.stringify(mongoOperation.data).length > 200 ? "..." : ""
        }`
      );
    }
    if (mongoOperation.options) {
      console.log(`   - Opciones: ${JSON.stringify(mongoOperation.options)}`);
    }
  }

  const results: ReplicationResult[] = [];

  for (const instancia of instanciasAActualizar) {
    const dbUrl = RDP03_INSTANCES_DATABASE_URL_MAP.get(instancia);

    if (!dbUrl) {
      console.warn(
        `⚠️ URL no disponible para instancia ${instancia}, omitiendo`
      );
      results.push({
        instancia,
        success: false,
        operacion: mongoOperation.operation,
        coleccion: mongoOperation.collection,
        error: "URL no configurada",
      });
      continue;
    }

    console.log(`🔄 Replicando en instancia ${instancia}...`);

    const client = new MongoClient(dbUrl, mongoOptions);

    try {
      // Conectar al cliente
      await client.connect();

      const start = Date.now();

      // Ejecutar la operación
      const operationResult = await executeMongoOperation(
        client,
        mongoOperation
      );

      const duration = Date.now() - start;

      if (operationResult.success) {
        const documentosAfectados = getAffectedDocumentsCount(
          mongoOperation.operation,
          operationResult.result
        );

        console.log(
          `✅ Operación completada en ${instancia}: ${documentosAfectados} documentos afectados en ${duration}ms`
        );

        results.push({
          instancia,
          success: true,
          operacion: mongoOperation.operation,
          coleccion: mongoOperation.collection,
          documentosAfectados,
          duracion: duration,
        });
      } else {
        console.error(
          `❌ Error en instancia ${instancia}: ${operationResult.error}`
        );
        results.push({
          instancia,
          success: false,
          operacion: mongoOperation.operation,
          coleccion: mongoOperation.collection,
          error: operationResult.error,
          duracion: duration,
        });
      }
    } catch (error: any) {
      console.error(
        `❌ Error de conexión en instancia ${instancia}:`,
        error.message
      );
      results.push({
        instancia,
        success: false,
        operacion: mongoOperation.operation,
        coleccion: mongoOperation.collection,
        error: `Error de conexión: ${error.message}`,
      });
    } finally {
      // Cerrar la conexión
      try {
        await client.close();
      } catch (closeError) {
        console.warn(
          `⚠️ Error cerrando conexión para ${instancia}:`,
          closeError
        );
      }
    }
  }

  console.log("\n📊 Resumen de replicación MongoDB:");
  console.table(
    results.map((r) => ({
      Instancia: r.instancia,
      Estado: r.success ? "✅ Éxito" : "❌ Error",
      Operación: r.operacion,
      Colección: r.coleccion,
      "Docs Afectados": r.documentosAfectados || 0,
      "Duración (ms)": r.duracion || 0,
      Error: r.error || "N/A",
    }))
  );

  // Estadísticas finales
  const exitosos = results.filter((r) => r.success);
  const fallidos = results.filter((r) => !r.success);

  console.log(`\n📈 Estadísticas finales:`);
  console.log(`   ✅ Exitosos: ${exitosos.length}/${results.length}`);
  console.log(`   ❌ Fallidos: ${fallidos.length}/${results.length}`);

  if (exitosos.length > 0) {
    const totalDocumentos = exitosos.reduce(
      (sum, r) => sum + (r.documentosAfectados || 0),
      0
    );
    const promedioTiempo =
      exitosos.reduce((sum, r) => sum + (r.duracion || 0), 0) / exitosos.length;
    console.log(`   📊 Total documentos afectados: ${totalDocumentos}`);
    console.log(`   ⏱️ Tiempo promedio: ${Math.round(promedioTiempo)}ms`);
  }

  // Verificar si hubo errores críticos
  if (fallidos.length > 0) {
    console.error(
      `\n🚨 Se encontraron ${fallidos.length} errores durante la replicación`
    );

    // Si más del 50% falló, considerar como fallo crítico
    if (fallidos.length > results.length / 2) {
      console.error("🔥 Fallo crítico: Más del 50% de las instancias fallaron");
      process.exit(1);
    } else {
      console.warn("⚠️ Replicación parcial: Algunas instancias fallaron");
      // No salir con error si es un fallo parcial
    }
  }
}

// Ejecutar la función principal
replicateToMongoDBInstances()
  .then(() => {
    console.log("\n🎉 Replicación MongoDB completada con éxito");
    process.exit(0);
  })
  .catch((error) => {
    console.error("\n💥 Error fatal en replicación MongoDB:", error);
    process.exit(1);
  });
