import streamlit as st
import pandas as pd
import pyarrow # o fastparquet
import matplotlib # Para interactuar con S3
# import matplotlib.pyplot as plt # Descomenta si vas a usar el histograma con matplotlib

# Configuración de la página (opcional)
st.set_page_config(layout="wide", page_title="Visualizador Parquet desde S3")

st.title("Graficas de la información recopilada de Open Bank")

# --- Inicializar session_state si no existe ---
if 'data_loaded' not in st.session_state:
    st.session_state.data_loaded = False
    st.session_state.df = None
    st.session_state.s3_path = ""

# --- Sección de Entrada de S3 ---
s3_bucket_input = "datasetproject3"
s3_key_input = "refined/"

# Botón para cargar los datos
load_button = st.button("Cargar y Visualizar Datos")

if load_button:
    if s3_bucket_input and s3_key_input:
        st.session_state.s3_path = f"s3://{s3_bucket_input}/{s3_key_input}"
        st.session_state.s3_bucket_input = s3_bucket_input # Guardar para repoblar el campo
        st.session_state.s3_key_input = s3_key_input     # Guardar para repoblar el campo
        st.info(f"Intentando cargar datos desde: {st.session_state.s3_path}")
        try:
            # Leer el archivo Parquet directamente desde S3
            st.session_state.df = pd.read_parquet(st.session_state.s3_path)
            st.session_state.data_loaded = True
            st.success(f"¡Archivo Parquet desde '{st.session_state.s3_path}' cargado y procesado exitosamente!")
            # Forzar un rerun para que los widgets de abajo se actualicen con los nuevos datos de session_state
            # st.experimental_rerun() # En versiones más nuevas st.rerun()
            st.rerun()
        except FileNotFoundError:
            st.session_state.data_loaded = False
            st.session_state.df = None
            st.error(f"Error: El archivo o bucket no fue encontrado en S3 ({st.session_state.s3_path}). Verifica el nombre del bucket y la ruta del archivo.")
            st.error("Asegúrate también de que tus credenciales de AWS estén configuradas correctamente y tengan permisos para acceder al bucket.")
        except pd.errors.EmptyDataError:
            st.session_state.data_loaded = False
            st.session_state.df = None
            st.error(f"Error: El archivo Parquet en {st.session_state.s3_path} está vacío.")
        except Exception as e:
            st.session_state.data_loaded = False
            st.session_state.df = None
            st.error(f"Error al leer o procesar el archivo Parquet desde S3: {e}")
            st.error("Verifica la ruta del archivo, los permisos de S3 y que el archivo sea un Parquet válido.")
    else:
        st.warning("Por favor, ingresa tanto el nombre del bucket S3 como la ruta del archivo Parquet.")

# --- Mostrar datos y visualizaciones SI los datos han sido cargados ---
if st.session_state.data_loaded and st.session_state.df is not None:
    df = st.session_state.df # Usar el DataFrame desde session_state

    # --- Mostrar Información del DataFrame ---
    st.subheader("Vista Previa de los Datos")
    st.dataframe(df.head())

    st.subheader("Información General del DataFrame")
    st.write(f"Número de filas: {df.shape[0]}")
    st.write(f"Número de columnas: {df.shape[1]}")

    with st.expander("Ver detalles de las columnas (info())"):
        from io import StringIO
        buffer = StringIO()
        df.info(buf=buffer)
        s = buffer.getvalue()
        st.text(s)

    with st.expander("Ver estadísticas descriptivas (describe())"):
        st.dataframe(df.describe(include='all'))

    # --- Sección de Visualización (Ejemplos Básicos) ---
    st.subheader("Visualización de Datos")

    all_columns = df.columns.tolist()
    numeric_columns = df.select_dtypes(include=['number']).columns.tolist()
    object_columns = df.select_dtypes(include=['object', 'category']).columns.tolist()

    if not numeric_columns:
        st.warning("No se encontraron columnas numéricas para graficar.")
    else:
        # Gráfico de Líneas
        st.markdown("#### Gráfico de Líneas")
        if len(numeric_columns) >= 1:
            # Usar un índice guardado en session_state o default 0 para el selectbox
            line_y_index = st.session_state.get('line_y_index', 0 if numeric_columns else None)
            if line_y_index is not None and line_y_index < len(numeric_columns):
                 selected_line_y = st.selectbox(
                    "Selecciona la columna Y para el gráfico de líneas:",
                    numeric_columns,
                    index=line_y_index, # Usar índice guardado
                    key="line_y_selector" # Clave única para el widget
                )
                 # Guardar el índice seleccionado para la próxima vez
                 st.session_state.line_y_index = numeric_columns.index(selected_line_y)

                 if df[selected_line_y].nunique() > 1:
                    st.line_chart(df[[selected_line_y]])
                 else:
                    st.info(f"La columna '{selected_line_y}' no tiene suficientes valores únicos para un gráfico de líneas.")
            else:
                 st.info("Índice de columna para gráfico de líneas no válido o no hay columnas numéricas.")

        else:
            st.info("Se necesita al menos una columna numérica para el gráfico de líneas.")

        # Gráfico de Barras
        st.markdown("#### Gráfico de Barras")
        # Opción 1: Para una sola columna numérica
        if len(numeric_columns) >= 1:
            # Usar un índice guardado en session_state o default 0 para el selectbox
            bar_single_y_index = st.session_state.get('bar_single_y_index', 0 if numeric_columns else None)
            if bar_single_y_index is not None and bar_single_y_index < len(numeric_columns):
                selected_bar_single = st.selectbox(
                    "Selecciona una columna numérica para el gráfico de barras:",
                    numeric_columns,
                    index=bar_single_y_index, # Usar índice guardado
                    key="bar_single_y_selector" # Clave única
                )
                # Guardar el índice seleccionado
                st.session_state.bar_single_y_index = numeric_columns.index(selected_bar_single)
                st.bar_chart(df[[selected_bar_single]])
            else:
                st.info("Índice de columna para gráfico de barras no válido o no hay columnas numéricas.")