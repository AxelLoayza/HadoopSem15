# Model

Aqui se ubicaran los scripts de modelado:
- `build_model_dataset_puno.py`
- `model_regression_puno.py`
- `model_kmeans_puno.py`
- `model_random_forest_puno.py`
- `assess_model_puno.py`

Flujo recomendado:
1. Construir el dataset agregado con `build_model_dataset_puno.py`.
2. Entrenar la regresion lineal como ruta principal.
3. Ejecutar KMeans como segmentacion exploratoria.
4. Probar Random Forest como benchmark.
5. Cerrar con `assess_model_puno.py`.

Salida esperada:
- dataset agregado en `hdfs://namenode:9000/user/hadoop/electropuno/model_dataset_puno`
- predicciones de regresion en `hdfs://namenode:9000/user/hadoop/electropuno/model_outputs/regression_predictions`
- predicciones de random forest en `hdfs://namenode:9000/user/hadoop/electropuno/model_outputs/random_forest_predictions`
- clusters de KMeans en `hdfs://namenode:9000/user/hadoop/electropuno/model_outputs/kmeans_predictions`

La prioridad del plan es regresion lineal; KMeans y Random Forest quedan como comparacion o benchmark.