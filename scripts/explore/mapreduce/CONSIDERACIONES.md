# Consideraciones: que NO hacer con MapReduce en este flujo

Este documento recoge las decisiones descartadas y los riesgos que deben evitarse al integrar MapReduce.

## 1. No encadenar MapReduce antes de Modify

**Razon:** `modify_puno.py` espera exactamente las 25 columnas originales del CSV crudo.
Si la salida de MapReduce (tabla ligera de 5 columnas) se usara como entrada de Modify,
el script fallaria con un error de columnas faltantes y romperia todo lo que sigue.

**Correcto:** Modify siempre lee desde `/user/hadoop/electropuno/raw/` o equivalente con CSV crudos.

## 2. No usar MapReduce como motor de modelado

**Razon:** MapReduce no tiene capacidades nativas de ML. Usarlo para entrenar regresion o KMeans
requeriria implementaciones manuales muy costosas de mantener.

**Correcto:** El modelado sigue en Spark ML, que ya esta funcionando y validado.

## 3. No reemplazar Spark por MapReduce en Explore

**Razon:** MapReduce no puede hacer `show()`, `describe()`, `distinct()` ni enriquecimiento con UBIGEO
de forma directa. Intentarlo duplicaria codigo sin ventaja real.

**Correcto:** MapReduce agrega previamente; Spark lee esa salida y hace el analisis descriptivo.

## 4. No agregar datanodes o nodemanagers extra para multi-mes

**Razon:** A escala de laboratorio con pocos meses, un datanode y un nodemanager son suficientes.
Agregar nodos extra solo complica el compose, consume mas RAM local y no aporta evidencia tecnica adicional.

**Correcto:** Escalar la infraestructura solo si el volumen real lo exige.

## 5. No usar el UBIGEO completo como clave numerica continua en Model

**Razon:** El UBIGEO es un codigo categorico jerarquico. Tratarlo como numero entero en regresion
produce relaciones falsas y degrada la calidad del modelo.

**Correcto:** Segmentar por prefijo de region (2 digitos) o provincia (4 digitos) y codificar con OHE,
tal como esta implementado actualmente en `model_regression_puno.py`.

## 6. No hacer Explore Spark directamente sobre los CSV crudos multi-mes sin MapReduce previo

**Razon:** Con `SPARK_WORKER_CORES=1` y `SPARK_WORKER_MEMORY=1g`, operaciones como `count()`,
`distinct()` por columna y `groupBy` completos sobre varios millones de filas pueden congelar el worker
o hacer que el job tarde decenas de minutos sin resultado util.

**Correcto:** Dejar que MapReduce reduzca primero y que Spark solo toque la tabla ligera resultante.
