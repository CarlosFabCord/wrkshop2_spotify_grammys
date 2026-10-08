# ETL Grammys + Spotify con Apache Airflow

Pipeline ETL que combina dos datasets (nominaciones de los premios Grammy y características de pistas de Spotify), valida la calidad de los datos con **pandera**, y orquesta todo con un DAG de **Apache Airflow** que ramifica según el resultado de la validación. El resultado se guarda en **SQLite** y se exporta a **CSV**.

Está pensado para ejecutarse en **Google Colab**.

## ¿Qué hace?

```
extrccn_grammy ──> validacion_grmmys ──(sin problemas)──────────────────────┐
                        │                                                   │
                        └─(nulos en nominee)─> trnsform_grammy_task         │
                                                      │                     │
                                              validacion_grmmys_silver      │
                                                      │                     ▼
extrccn_spotify ──────────────────────────────────────┴──────────> combinacion_tablas
                                                                            │
                                                              ┌─────────────┴─────────────┐
                                                              ▼                           ▼
                                                        carga_sqlite                exportar_csv
```
En la carpeta docs encontrará el diagrama arrojado por Airflow

| Tarea | Qué hace |
|---|---|
| `extrccn_spotify` | Lee el CSV de Spotify y comprueba que se carga bien. |
| `extrccn_grammy` | Crea las tablas `grammys` y `grammys_silver` en SQLite y carga el CSV de Grammy. |
| `validacion_grmmys` | **Rama.** Valida `grammys` con pandera. Si todo está bien salta a `combinacion_tablas`; si solo falla `nominee` (nulos) salta a la transformación; si falla otra cosa, la tarea falla. |
| `trnsform_grammy_task` | Elimina las filas con `nominee` nulo y guarda el resultado en `grammys_silver`. |
| `validacion_grmmys_silver` | Revalida `grammys_silver`. No ramifica: si falla, el DAG se detiene. |
| `combinacion_tablas` | Une Grammy con Spotify y deja el resultado en un archivo temporal. |
| `carga_sqlite` | Guarda el resultado en la tabla `grammys_spotify` de SQLite. |
| `exportar_csv` | Exporta el resultado a `grammys_spotify.csv`. |

## Estructura del repositorio

```
.
├── README.md
├── requirements.txt
├── .gitignore
├── dags/
│   └── dag_ramificado_grammy_spotify.py   # el DAG de Airflow
├── notebooks/
│   └── etl_grammys_spotify.ipynb          # notebook para ejecutarlo en Colab
└── data/
    └── README.md                          # de dónde descargar los CSV (los CSV no se suben)
```

## Datos de entrada

Los CSV no se incluyen en el repositorio. Colócalos en `/content/` de Colab (o súbelos desde el notebook):

| Archivo | Filas | Contenido |
|---|---|---|
| `the_grammy_awards.csv` | 4.810 | Nominaciones: `year`, `title`, `published_at`, `updated_at`, `category`, `nominee`, `artist`, `workers`, `img`, `winner` |
| `datasetSpotifi.csv` | 114.000 | Pistas con `track_id`, `artists`, `album_name`, `track_name`, `popularity` y características de audio (`danceability`, `energy`, `valence`, `tempo`, ...) |

> Completar: enlace de origen de cada dataset (por ejemplo Kaggle).

## Requisitos

- Cuenta de Google (Colab y Drive)
- Python 3.12 (el de Colab)
- `apache-airflow==2.10.5`, `pandas`, `numpy`, `pandera` (ver `requirements.txt`)

## Cómo ejecutarlo en Colab

1. Abre `notebooks/etl_grammys_spotify.ipynb` en Colab.
2. Ejecuta las celdas **en orden**:

| Celda | Qué hace |
|---|---|
| 1 | Instala las dependencias. Después: **Entorno de ejecución → Reiniciar sesión**. |
| 2 | Monta Google Drive. |
| 3 | Sube los dos CSV a `/content/` si no están. Debe imprimir `True` dos veces. |
| 4 | Configura Airflow (`AIRFLOW_HOME=/content/airflow`). |
| 5 | Escribe el DAG en `/content/airflow/dags/`. |
| 6 | Ejecuta el DAG con `airflow dags test`. |
| 7 | Muestra las tablas creadas y el resumen de coincidencias. |

3. Comprueba que en tu Drive (Mi unidad) aparecen `grammys_db.sqlite` y `grammys_spotify.csv`.

### Configuración de rutas

Están al inicio del DAG:

```python
ruta_base       = "/content/drive/My Drive/"
ruta_db_grmmy   = ruta_base + "grammys_db.sqlite"
ruta_spotify    = "/content/datasetSpotifi.csv"
ruta_grammy     = "/content/the_grammy_awards.csv"
ruta_intermedio = "/content/combinado_tmp.pkl"      # archivo temporal entre tareas
ruta_csv_final  = ruta_base + "grammys_spotify.csv"
```

Drive debe montarse **antes** de ejecutar Airflow, desde una celda del notebook. `drive.mount` no funciona dentro de una tarea de Airflow.

## Resultado

La tabla `grammys_spotify` (y el CSV) tiene **una fila por nominación** (4.804) y 41 columnas:

| Grupo | Columnas |
|---|---|
| Grammy | Las 10 originales más `grammy_id` |
| Álbum en Spotify | `album_popularity`, `album_danceability`, `album_energy`, `album_generos`, `album_n_pistas`, `album_coincidencia`, ... (15) |
| Pista en Spotify | Lo mismo con prefijo `track_` (15) |

Las columnas `album_coincidencia` y `track_coincidencia` indican la fiabilidad de la unión:

| Valor | Significado |
|---|---|
| `nombre+artista` | Coinciden el nombre de la obra y el artista. **Fiable.** |
| `nombre_sin_verificar` | Coincide el nombre, pero Grammy no trae artista, así que no se pudo verificar. |
| vacío | Sin coincidencia en Spotify. |

Para análisis conviene filtrar por `nombre+artista`.

## Decisiones de diseño

- **Un solo branch.** Solo `validacion_grmmys` ramifica. La revalidación de `silver` solo pasa o falla. Reenviar a la transformación crearía un ciclo, y un DAG no puede tener ciclos.
- **Las tareas están aisladas.** Cada una abre y cierra su propia conexión a SQLite. Los datos viajan entre tareas por SQLite o por un archivo temporal, no por variables de Python.
- **`trigger_rule="none_failed_min_one_success"`** en `combinacion_tablas`, porque siempre hay una rama saltada.
- **Una fila por nominación.** Spotify se reduce a una fila por (nombre, artista principal) antes de unir, para evitar el producto cruzado entre duplicados. `combinacion_tablas` falla si se multiplican filas.
- **Coincidencia exigiendo artista.** Unir solo por nombre daba falsas coincidencias (por ejemplo "Bad Guy" de Billie Eilish con otro artista).

## Limitaciones

- Solo coinciden unas 180 a 230 nominaciones verificadas de 4.804: los Grammys cubren desde 1958 y el dataset de Spotify es mayoritariamente de música reciente.
- La comparación de artistas es por texto y falla con variantes (`&` contra `featuring`).
- Las pistas se promedian por nombre y artista, así que los remixes mezclan su popularidad con la de la versión original.

## Problemas comunes

| Error | Solución |
|---|---|
| `ModuleNotFoundError: No module named 'numpy'` (o `pandas`, `pandera`) | Airflow usa un entorno distinto al de Colab. Instala en ese entorno: `!pip install -q uv` y luego `!uv pip install --python /content/airflow_venv/bin/python numpy pandas pandera "typing-extensions>=4.15"` |
| `Dag 'etl_con_flujo_grammys_spotify' could not be found` | El DAG no se pudo importar. Mira el error de importación unas líneas más arriba. |
| `FileNotFoundError` | Falta un CSV o Drive no está montado. Repite las celdas 2 y 3. |
| `disk I/O error` al escribir SQLite en Drive | Cambia `ruta_db_grmmy` a `/content/grammys_db.sqlite` y copia el archivo a Drive al final. |
| `SchemaErrors` en `validacion_grmmys` | Falla una columna distinta de `nominee`. Revisa la tabla `failure_cases` que imprime la tarea. |

## Posibles mejoras

- Decidir si `nominee` es álbum o canción según `category`.
- Mejorar la comparación de artistas (limpiar `featuring`, comparación aproximada).
- Usar el máximo de popularidad en lugar del promedio para pistas.

## Autor

> Completar: nombre, contacto y licencia.
