"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const mongodb_1 = require("mongodb");
const RDP03_INSTANCES_DISTRIBUTION_1 = require("../../constants/RDP03_INSTANCES_DISTRIBUTION");
const Entornos_1 = require("../../interfaces/shared/Entornos");
function sanitizeMongoOperation(operation) {
    const fieldsToConvertToString = [
        'Id_Aula',
    ];
    function convertFieldsToString(obj) {
        if (obj === null || obj === undefined)
            return obj;
        if (Array.isArray(obj)) {
            return obj.map(item => convertFieldsToString(item));
        }
        if (typeof obj === 'object') {
            const converted = {};
            for (const [key, value] of Object.entries(obj)) {
                if (fieldsToConvertToString.includes(key) && value !== null && value !== undefined) {
                    converted[key] = String(value);
                }
                else if (typeof value === 'object') {
                    converted[key] = convertFieldsToString(value);
                }
                else {
                    converted[key] = value;
                }
            }
            return converted;
        }
        return obj;
    }
    const sanitizedOperation = { ...operation };
    if (sanitizedOperation.filter) {
        sanitizedOperation.filter = convertFieldsToString(sanitizedOperation.filter);
    }
    if (sanitizedOperation.data) {
        sanitizedOperation.data = convertFieldsToString(sanitizedOperation.data);
    }
    if (sanitizedOperation.pipeline) {
        sanitizedOperation.pipeline = convertFieldsToString(sanitizedOperation.pipeline);
    }
    return sanitizedOperation;
}
const mongoOptions = {
    maxPoolSize: parseInt(process.env.MONGO_MAX_POOL_SIZE || "3", 10),
    minPoolSize: parseInt(process.env.MONGO_MIN_POOL_SIZE || "1", 10),
    maxIdleTimeMS: 30000,
    serverSelectionTimeoutMS: parseInt(process.env.MONGO_SERVER_SELECTION_TIMEOUT || "5000", 10),
    connectTimeoutMS: parseInt(process.env.MONGO_CONNECTION_TIMEOUT || "8000", 10),
    heartbeatFrequencyMS: 10000,
    retryWrites: true,
    retryReads: false,
};
const mongoOperationJson = process.env.MONGO_OPERATION;
const instanciasAActualizarJson = process.env.INSTANCIAS_A_ACTUALIZAR;
const timestamp = process.env.TIMESTAMP;
if (!mongoOperationJson) {
    console.error("Error: No se proporcionó la operación MongoDB");
    process.exit(1);
}
let mongoOperation;
let instanciasAActualizar = [];
try {
    const rawOperation = JSON.parse(mongoOperationJson);
    mongoOperation = sanitizeMongoOperation(rawOperation);
    if (instanciasAActualizarJson) {
        instanciasAActualizar = JSON.parse(instanciasAActualizarJson);
    }
    if (process.env.ENTORNO === Entornos_1.Entorno.DESARROLLO) {
        console.log("Operación sanitizada:");
        console.log(JSON.stringify(mongoOperation, null, 2));
    }
}
catch (error) {
    console.error("Error al parsear parámetros:", error);
    process.exit(1);
}
async function executeMongoOperation(client, operation, dbName = "siasis_asuncion_8") {
    try {
        const db = client.db(dbName);
        const collection = db.collection(operation.collection);
        let result;
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
                result = await collection.updateOne(operation.filter || {}, operation.data, operation.options);
                return {
                    success: true,
                    result: {
                        matchedCount: result.matchedCount,
                        modifiedCount: result.modifiedCount,
                        upsertedCount: result.upsertedCount,
                    },
                };
            case "updateMany":
                result = await collection.updateMany(operation.filter || {}, operation.data, operation.options);
                return {
                    success: true,
                    result: {
                        matchedCount: result.matchedCount,
                        modifiedCount: result.modifiedCount,
                        upsertedCount: result.upsertedCount,
                    },
                };
            case "deleteOne":
                result = await collection.deleteOne(operation.filter || {}, operation.options);
                return {
                    success: true,
                    result: {
                        deletedCount: result.deletedCount,
                    },
                };
            case "deleteMany":
                result = await collection.deleteMany(operation.filter || {}, operation.options);
                return {
                    success: true,
                    result: {
                        deletedCount: result.deletedCount,
                    },
                };
            case "replaceOne":
                result = await collection.replaceOne(operation.filter || {}, operation.data, operation.options);
                return {
                    success: true,
                    result: {
                        matchedCount: result.matchedCount,
                        modifiedCount: result.modifiedCount,
                        upsertedCount: result.upsertedCount,
                    },
                };
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
                result = await collection.findOne(operation.filter || {}, operation.options);
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
                result = await collection.countDocuments(operation.filter || {}, operation.options);
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
    }
    catch (error) {
        return {
            success: false,
            error: error.message,
        };
    }
}
function getAffectedDocumentsCount(operationType, result) {
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
async function replicateToSingleInstance(instancia) {
    const dbUrl = RDP03_INSTANCES_DISTRIBUTION_1.RDP03_INSTANCES_DATABASE_URL_MAP.get(instancia);
    if (!dbUrl) {
        console.warn(`URL no disponible para instancia ${instancia}`);
        return {
            instancia,
            success: false,
            operacion: mongoOperation.operation,
            coleccion: mongoOperation.collection,
            error: "URL no configurada",
        };
    }
    console.log(`Replicando en instancia ${instancia}...`);
    const client = new mongodb_1.MongoClient(dbUrl, mongoOptions);
    try {
        await Promise.race([
            client.connect(),
            new Promise((_, reject) => setTimeout(() => reject(new Error("Timeout de conexión")), 10000))
        ]);
        const start = Date.now();
        const operationResult = await executeMongoOperation(client, mongoOperation);
        const duration = Date.now() - start;
        if (operationResult.success) {
            const documentosAfectados = getAffectedDocumentsCount(mongoOperation.operation, operationResult.result);
            console.log(`Operación completada en ${instancia}: ${documentosAfectados} documentos afectados en ${duration}ms`);
            return {
                instancia,
                success: true,
                operacion: mongoOperation.operation,
                coleccion: mongoOperation.collection,
                documentosAfectados,
                duracion: duration,
            };
        }
        else {
            console.error(`Error en instancia ${instancia}: ${operationResult.error}`);
            return {
                instancia,
                success: false,
                operacion: mongoOperation.operation,
                coleccion: mongoOperation.collection,
                error: operationResult.error,
                duracion: duration,
            };
        }
    }
    catch (error) {
        console.error(`Error de conexión en instancia ${instancia}:`, error.message);
        return {
            instancia,
            success: false,
            operacion: mongoOperation.operation,
            coleccion: mongoOperation.collection,
            error: `Error de conexión: ${error.message}`,
        };
    }
    finally {
        try {
            await client.close();
        }
        catch (closeError) {
            console.warn(`Error cerrando conexión para ${instancia}:`, closeError);
        }
    }
}
async function replicateToMongoDBInstances() {
    console.log("Iniciando replicación MongoDB EMCN01 - MODO PARALELO");
    console.log(`Timestamp de operación: ${timestamp}`);
    console.log(`Instancias a actualizar: ${instanciasAActualizar.length}`);
    console.log(`Operación a replicar: ${mongoOperation.operation} en colección ${mongoOperation.collection}`);
    if (process.env.ENTORNO === Entornos_1.Entorno.DESARROLLO) {
        console.log("Detalles de la operación:");
        console.log(`   - Operación: ${mongoOperation.operation}`);
        console.log(`   - Colección: ${mongoOperation.collection}`);
        if (mongoOperation.filter) {
            console.log(`   - Filtro: ${JSON.stringify(mongoOperation.filter)}`);
        }
        if (mongoOperation.data) {
            console.log(`   - Datos: ${JSON.stringify(mongoOperation.data).substring(0, 200)}${JSON.stringify(mongoOperation.data).length > 200 ? "..." : ""}`);
        }
        if (mongoOperation.options) {
            console.log(`   - Opciones: ${JSON.stringify(mongoOperation.options)}`);
        }
    }
    const startTime = Date.now();
    const maxConcurrentConnections = parseInt(process.env.MONGO_MAX_CONCURRENT_REPLICATIONS || "5", 10);
    console.log(`Ejecutando hasta ${maxConcurrentConnections} replicaciones simultáneas`);
    let results = [];
    if (instanciasAActualizar.length <= maxConcurrentConnections) {
        console.log("Ejecutando TODAS las replicaciones en paralelo completo");
        const promesasReplicacion = instanciasAActualizar.map(instancia => replicateToSingleInstance(instancia));
        results = await Promise.all(promesasReplicacion);
    }
    else {
        console.log(`Procesando en lotes de ${maxConcurrentConnections} instancias (hay ${instanciasAActualizar.length} instancias)`);
        for (let i = 0; i < instanciasAActualizar.length; i += maxConcurrentConnections) {
            const lote = instanciasAActualizar.slice(i, i + maxConcurrentConnections);
            console.log(`Procesando lote ${Math.floor(i / maxConcurrentConnections) + 1}/${Math.ceil(instanciasAActualizar.length / maxConcurrentConnections)}: [${lote.join(', ')}]`);
            const promesasLote = lote.map(instancia => replicateToSingleInstance(instancia));
            const resultadosLote = await Promise.all(promesasLote);
            results.push(...resultadosLote);
        }
    }
    const totalDuration = Date.now() - startTime;
    console.log("\nResumen de replicación MongoDB (PARALELA):");
    console.table(results.map((r) => ({
        Instancia: r.instancia,
        Estado: r.success ? "Éxito" : "Error",
        Operación: r.operacion,
        Colección: r.coleccion,
        "Docs Afectados": r.documentosAfectados || 0,
        "Duración (ms)": r.duracion || 0,
        Error: r.error || "N/A",
    })));
    const exitosos = results.filter((r) => r.success);
    const fallidos = results.filter((r) => !r.success);
    console.log(`\nEstadísticas finales (PARALELO):`);
    console.log(`   Exitosos: ${exitosos.length}/${results.length}`);
    console.log(`   Fallidos: ${fallidos.length}/${results.length}`);
    console.log(`   Tiempo total de replicación: ${totalDuration}ms`);
    if (exitosos.length > 0) {
        const totalDocumentos = exitosos.reduce((sum, r) => sum + (r.documentosAfectados || 0), 0);
        const promedioTiempoInstancia = exitosos.reduce((sum, r) => sum + (r.duracion || 0), 0) / exitosos.length;
        const tiempoMasLento = Math.max(...exitosos.map(r => r.duracion || 0));
        const tiempoMasRapido = Math.min(...exitosos.map(r => r.duracion || 0));
        console.log(`   Total documentos afectados: ${totalDocumentos}`);
        console.log(`   Tiempo promedio por instancia: ${Math.round(promedioTiempoInstancia)}ms`);
        console.log(`   Instancia más lenta: ${tiempoMasLento}ms`);
        console.log(`   Instancia más rápida: ${tiempoMasRapido}ms`);
        const tiempoSerieEstimado = exitosos.reduce((sum, r) => sum + (r.duracion || 0), 0);
        const mejoraRendimiento = Math.round(((tiempoSerieEstimado - totalDuration) / tiempoSerieEstimado) * 100);
        console.log(`   Mejora estimada vs serie: ${mejoraRendimiento > 0 ? '+' : ''}${mejoraRendimiento}%`);
    }
    if (fallidos.length > 0) {
        console.error(`\nSe encontraron ${fallidos.length} errores durante la replicación`);
        if (fallidos.length > results.length / 2) {
            console.error("Fallo crítico: Más del 50% de las instancias fallaron");
            process.exit(1);
        }
        else {
            console.warn("Replicación parcial: Algunas instancias fallaron");
        }
    }
}
replicateToMongoDBInstances()
    .then(() => {
    console.log("\nReplicación MongoDB PARALELA completada con éxito");
    process.exit(0);
})
    .catch((error) => {
    console.error("\nError fatal en replicación MongoDB PARALELA:", error);
    process.exit(1);
});
