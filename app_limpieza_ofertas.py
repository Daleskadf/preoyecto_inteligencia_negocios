import streamlit as st
import pandas as pd
import re
from datetime import datetime, timedelta
import os
import csv
import boto3
import io

# --- Constantes ---
CURRENT_YEAR = datetime.now().year

# --- Funciones de Limpieza (SIN CAMBIOS) ---
def parse_fecha(fecha_str):
    # ... (tu código)
    if pd.isna(fecha_str): return None
    fecha_str = str(fecha_str).lower().strip()
    if not fecha_str or fecha_str == 'nan': return None
    try:
        parsed_date = pd.to_datetime(fecha_str)
        return parsed_date.strftime('%Y-%m-%d')
    except Exception: pass
    if "hoy" in fecha_str: return datetime.now().strftime('%Y-%m-%d')
    if "ayer" in fecha_str: return (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    match_hace_dias = re.search(r'hace (\d+) días?', fecha_str)
    if match_hace_dias: return (datetime.now() - timedelta(days=int(match_hace_dias.group(1)))).strftime('%Y-%m-%d')
    match_hace_horas_min = re.search(r'hace (\d+) (horas?|minutos?)', fecha_str)
    if match_hace_horas_min: return datetime.now().strftime('%Y-%m-%d')
    meses_es = {"enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6, "julio": 7, "agosto": 8, "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12}
    match_dd_mes_yyyy = re.search(r'(\d{1,2})\s+de\s+([a-záéíóúñ]+)\s+de\s+(\d{4})', fecha_str)
    if match_dd_mes_yyyy:
        dia, mes_str, year = int(match_dd_mes_yyyy.group(1)), match_dd_mes_yyyy.group(2), int(match_dd_mes_yyyy.group(3))
        if mes_str in meses_es:
            try: return datetime(year, meses_es[mes_str], dia).strftime('%Y-%m-%d')
            except ValueError: return None
    match_dd_mes = re.search(r'(\d{1,2})\s+de\s+([a-záéíóúñ]+)', fecha_str)
    if match_dd_mes:
        dia, mes_str = int(match_dd_mes.group(1)), match_dd_mes.group(2)
        if mes_str in meses_es:
            year_a_usar = CURRENT_YEAR
            try:
                fecha_propuesta = datetime(year_a_usar, meses_es[mes_str], dia)
                if fecha_propuesta > datetime.now() + timedelta(days=60): year_a_usar -=1
                return datetime(year_a_usar, meses_es[mes_str], dia).strftime('%Y-%m-%d')
            except ValueError: return None
    return None

def limpiar_salario(monto_str, moneda_str_original, tipo_pago_str_original):
    # ... (tu código)
    monto_limpio, moneda_limpia, tipo_pago_limpio = None, None, None
    if pd.notna(monto_str):
        monto_str_lower = str(monto_str).lower().strip()
        if monto_str_lower not in ['no disponible', 'a convenir', 'según mercado', 'nan', '']:
            s = str(monto_str).replace('.', '').replace(',', '.')
            match_num = re.search(r'(\d+\.?\d*)', s)
            if match_num:
                try: monto_limpio = float(match_num.group(1))
                except ValueError: pass
    if monto_limpio is not None:
        if pd.notna(moneda_str_original):
            moneda_str_lower = str(moneda_str_original).lower().strip()
            if moneda_str_lower not in ['no disponible', 'nan', '']:
                moneda_limpia = str(moneda_str_original).upper().strip()
                if moneda_limpia == "S/.": moneda_limpia = "PEN"
        if moneda_limpia is None: moneda_limpia = "PEN"
        if pd.notna(tipo_pago_str_original):
            tipo_pago_lower = str(tipo_pago_str_original).lower().strip()
            if tipo_pago_lower not in ['no disponible', 'nan', '']:
                tipo_pago_limpio = str(tipo_pago_str_original).strip().capitalize()
        if tipo_pago_limpio is None and monto_limpio > 200: tipo_pago_limpio = "Mensual"
        elif tipo_pago_limpio is None: tipo_pago_limpio = "No especificado"
    return monto_limpio, moneda_limpia, tipo_pago_limpio

def limpiar_edad(edad_str):
    # ... (tu código)
    if pd.isna(edad_str) or str(edad_str).lower().strip() in ['no disponible', 'nan', '']: return None
    match = re.search(r'(\d+)', str(edad_str))
    if match:
        try: return int(match.group(1))
        except ValueError: return None
    return None

def capitalizar_texto(texto):
    # ... (tu código)
    if pd.isna(texto) or str(texto).lower().strip() in ['no disponible', 'nan', '']: return None
    return str(texto).strip().capitalize()

def limpiar_lista_delimitada(texto_lista, delimitador=','):
    # ... (tu código)
    if pd.isna(texto_lista) or str(texto_lista).lower().strip() in ['no disponible', 'nan', '']: return None
    items = [item.strip().capitalize() for item in str(texto_lista).split(delimitador) if item.strip()]
    return delimitador.join(items) if items else None

# --- Lógica Principal de Procesamiento (SIN CAMBIOS) ---
def procesar_dataframe(df_input):
    # ... (tu código existente para procesar_dataframe) ...
    st.write("Iniciando limpieza y transformación de datos...")
    df = df_input.copy()
    progress_bar = st.progress(0)
    total_steps = 7
    # 1. Fechas
    df['Fecha_Publicacion_Limpia'] = df['Fecha_Publicacion'].apply(parse_fecha) if 'Fecha_Publicacion' in df.columns else None
    progress_bar.progress(1/total_steps)
    # 2. Salarios
    salario_cols_exist = all(col in df.columns for col in ['Salario_Monto', 'Salario_Moneda', 'Salario_Tipo_Pago'])
    if salario_cols_exist:
        salarios_limpios = df.apply(lambda r: limpiar_salario(r['Salario_Monto'], r['Salario_Moneda'], r['Salario_Tipo_Pago']), axis=1)
        df[['Salario_Monto_Limpio', 'Salario_Moneda_Limpia', 'Salario_Tipo_Pago_Limpio']] = pd.DataFrame(salarios_limpios.tolist(), index=df.index)
    else:
        st.warning("Columnas de salario ('Salario_Monto', 'Salario_Moneda', 'Salario_Tipo_Pago') no encontradas. Se crearán vacías.")
        df['Salario_Monto_Limpio'], df['Salario_Moneda_Limpia'], df['Salario_Tipo_Pago_Limpio'] = None, None, None
    progress_bar.progress(2/total_steps)
    # 3. Edades
    df['Edad_minima_Limpia'] = df['Edad_minima'].apply(limpiar_edad) if 'Edad_minima' in df.columns else None
    df['Edad_maxima_Limpia'] = df['Edad_maxima'].apply(limpiar_edad) if 'Edad_maxima' in df.columns else None
    progress_bar.progress(3/total_steps)
    # 4. Estandarización de Texto
    columnas_texto_a_capitalizar = ['Título', 'Ciudad', 'Region_Departamento', 'Tipo_Contrato', 'Tipo_Jornada', 'Modalidad_Trabajo', 'nivel_ingles', 'nivel_educacion', 'NombreEmpresa', 'Categoría']
    for col in columnas_texto_a_capitalizar:
        df[col + '_Limpio'] = df[col].apply(capitalizar_texto) if col in df.columns else None
    progress_bar.progress(4/total_steps)
    # 5. Limpieza de Listas Delimitadas
    columnas_lista_a_limpiar = ['Lenguajes', 'Frameworks', 'gestores_db', 'Herramientas', 'Conocimientos_Adicionales']
    for col in columnas_lista_a_limpiar:
        df[col + '_Lista_Limpia'] = df[col].apply(lambda x: limpiar_lista_delimitada(x, delimitador=',')) if col in df.columns else None
    progress_bar.progress(5/total_steps)
    # 6. Años de Experiencia
    if 'Anos_Experiencia' in df.columns: df['Anos_Experiencia_Limpio'] = pd.to_numeric(df['Anos_Experiencia'], errors='coerce')
    else: df['Anos_Experiencia_Limpio'] = None
    progress_bar.progress(6/total_steps)
    # 7. Seleccionar y renombrar columnas finales
    columnas_finales_map = {
        'ID_Oferta': 'ID_Oferta', 'Título_Limpio': 'Titulo_Oferta', 'Ciudad_Limpio': 'Ciudad',
        'Region_Departamento_Limpio': 'Region_Departamento', 'Fecha_Publicacion_Limpia': 'Fecha_Publicacion',
        'Tipo_Contrato_Limpio': 'Tipo_Contrato', 'Tipo_Jornada_Limpio': 'Tipo_Jornada',
        'Modalidad_Trabajo_Limpio': 'Modalidad_Trabajo', 'Salario_Monto_Limpio': 'Salario_Monto',
        'Salario_Moneda_Limpia': 'Salario_Moneda', 'Salario_Tipo_Pago_Limpio': 'Salario_Tipo_Pago',
        'Descripcion_Oferta_Raw': 'Descripcion_Oferta_Raw',
        'Lenguajes_Lista_Limpia': 'Lenguajes_Lista', 'Frameworks_Lista_Limpia': 'Frameworks_Lista',
        'gestores_db_Lista_Limpia': 'Bases_Datos_Lista', 'Herramientas_Lista_Limpia': 'Herramientas_Lista',
        'nivel_ingles_Limpio': 'Nivel_Ingles', 'nivel_educacion_Limpio': 'Nivel_Educacion',
        'Anos_Experiencia_Limpio': 'Anos_Experiencia',
        'Conocimientos_Adicionales_Lista_Limpia': 'Conocimientos_Adicionales_Lista',
        'Edad_minima_Limpia': 'Edad_Minima', 'Edad_maxima_Limpia': 'Edad_Maxima',
        'NombreEmpresa_Limpio': 'Nombre_Empresa', 'DescripciónEmpresa': 'Descripcion_Empresa_Raw',
        'Enlace_Oferta': 'Enlace_Oferta', 'Categoría_Limpio': 'Categoria_Puesto'
    }
    df_final = pd.DataFrame()
    for processed_col_key, final_col_name in columnas_finales_map.items():
        col_to_use_from_df_processed = None
        if processed_col_key in df.columns:
            col_to_use_from_df_processed = processed_col_key
        elif processed_col_key.replace('_Limpio', '').replace('_Lista_Limpia', '') in df.columns:
            original_col_name = processed_col_key.replace('_Limpio', '').replace('_Lista_Limpia', '')
            if original_col_name + '_Limpio' in df.columns:
                 col_to_use_from_df_processed = original_col_name + '_Limpio'
            elif original_col_name + '_Lista_Limpia' in df.columns:
                 col_to_use_from_df_processed = original_col_name + '_Lista_Limpia'
            else:
                col_to_use_from_df_processed = original_col_name
        if col_to_use_from_df_processed and col_to_use_from_df_processed in df.columns:
            df_final[final_col_name] = df[col_to_use_from_df_processed]
        elif processed_col_key in df.columns:
             df_final[final_col_name] = df[processed_col_key]
        else:
            df_final[final_col_name] = pd.Series([None] * len(df), dtype='object')
    progress_bar.progress(7/total_steps)
    return df_final

# --- Función para convertir a CSV para descarga local (SIN CAMBIOS) ---
@st.cache_data
def convert_df_to_csv_for_download(df_to_convert):
    # ... (tu código)
    df_copy = df_to_convert.copy()
    cols_texto_largo = ['Descripcion_Oferta_Raw', 'Descripcion_Empresa_Raw']
    for col_name in cols_texto_largo:
        if col_name in df_copy.columns and df_copy[col_name].notna().any():
            df_copy[col_name] = df_copy[col_name].astype(str).str.replace('\r\n', ' ', regex=False).str.replace('\n', ' ', regex=False)
            df_copy[col_name] = df_copy[col_name].str.replace(r'\s+', ' ', regex=True).str.strip()
    try:
        csv_output = df_copy.to_csv(index=False, encoding='utf-8-sig', sep=',', na_rep='', quoting=csv.QUOTE_MINIMAL)
        return csv_output.encode('utf-8-sig')
    except Exception as e:
        st.error(f"Error durante la conversión a CSV para descarga: {e}")
        return None

# --- Función para subir DataFrame a S3 (SIN CAMBIOS) ---
def upload_df_to_s3(df_to_upload, bucket_name, s3_object_key_name, format_type="csv"):
    # ... (tu código)
    st.write(f"Intentando subir DataFrame a S3: s3://{bucket_name}/{s3_object_key_name}")
    try:
        s3_resource = boto3.resource('s3')
        if format_type.lower() == "csv":
            csv_buffer = io.StringIO()
            df_for_csv = df_to_upload.astype(str).copy()
            df_for_csv.replace('None', '', inplace=True)
            df_for_csv.replace('nan', '', inplace=True)
            cols_texto_largo = ['Descripcion_Oferta_Raw', 'Descripcion_Empresa_Raw']
            for col_name in cols_texto_largo:
                if col_name in df_for_csv.columns:
                    df_for_csv[col_name] = df_for_csv[col_name].str.replace('\r\n', ' ', regex=False).str.replace('\n', ' ', regex=False)
                    df_for_csv[col_name] = df_for_csv[col_name].str.replace(r'\s+', ' ', regex=True).str.strip()
            df_for_csv.to_csv(csv_buffer, index=False, encoding='utf-8-sig', sep=',', quoting=csv.QUOTE_MINIMAL, na_rep='')
            s3_resource.Object(bucket_name, s3_object_key_name).put(Body=csv_buffer.getvalue().encode('utf-8-sig'))
            st.success(f"DataFrame subido como CSV exitosamente a s3://{bucket_name}/{s3_object_key_name}")
        elif format_type.lower() == "parquet":
            parquet_buffer = io.BytesIO()
            df_to_upload.to_parquet(parquet_buffer, index=False)
            s3_resource.Object(bucket_name, s3_object_key_name).put(Body=parquet_buffer.getvalue())
            st.success(f"DataFrame subido como Parquet exitosamente a s3://{bucket_name}/{s3_object_key_name}")
        else:
            st.error(f"Formato '{format_type}' no soportado para subida a S3.")
            return False
        return True
    except Exception as e:
        st.error(f"Error al subir DataFrame a S3 (s3://{bucket_name}/{s3_object_key_name}): {e}")
        st.exception(e)
        return False

# --- Interfaz de Streamlit ---
st.set_page_config(page_title="Limpiador CSV Ofertas vS3", layout="wide")
# st.image("ruta/a/tu/logo.png", width=200) # Descomenta y ajusta si tienes un logo
st.title("🧹 Limpiador y Estandarizador de CSV de Ofertas Laborales (con Subida a S3)")
st.markdown("""
Sube un archivo CSV con datos de ofertas laborales (delimitado por **punto y coma ;** y codificado en **UTF-8**) 
para limpiarlo, estandarizarlo y prepararlo para análisis. El archivo limpio se subirá a Amazon S3.
""")

# --- Leer configuración de S3 directamente de los secretos ---
# Estos son los NOMBRES DE LOS SECRETOS que debes definir en Streamlit Community Cloud
# Si un secreto no está definido, se usará el valor por defecto o un string vacío.
AWS_ACCESS_KEY_ID_LOADED = st.secrets.get("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY_LOADED = st.secrets.get("AWS_SECRET_ACCESS_KEY", "")
# AWS_DEFAULT_REGION_LOADED = st.secrets.get("AWS_DEFAULT_REGION", "us-east-1") # Boto3 lo toma de las vars de entorno

S3_BUCKET_NAME_FROM_SECRET = st.secrets.get("S3_PROCESSED_BUCKET", "")
S3_OBJECT_PREFIX_FROM_SECRET = st.secrets.get("S3_OBJECT_PREFIX", "ofertas_limpias/") # Valor por defecto si no está en secretos
S3_FILE_FORMAT_FROM_SECRET = st.secrets.get("S3_FILE_FORMAT", "csv") # Valor por defecto si no está en secretos

# Ya no mostramos estos en la barra lateral, se usan directamente.
# Si quieres que el usuario PUEDA sobrescribirlos, entonces sí los pones en la sidebar como antes.
# Pero si quieres que SIEMPRE se usen los de los secretos, entonces no los expongas en la UI.

# Mostrar un mensaje si la configuración de S3 no está completa en los secretos
if not S3_BUCKET_NAME_FROM_SECRET:
    st.sidebar.warning("⚠️ Nombre del Bucket S3 no configurado en los secretos de Streamlit.")
    st.sidebar.info("La aplicación intentará usar 'tu-bucket-s3-aqui'. "
                    "Para un funcionamiento correcto, configura `S3_PROCESSED_BUCKET` en los secretos.")
    s3_bucket_to_use = "tu-bucket-s3-aqui" # Fallback si no hay secreto
else:
    s3_bucket_to_use = S3_BUCKET_NAME_FROM_SECRET
#    st.sidebar.success(f"✔️ Usando Bucket S3: {s3_bucket_to_use}")


# (Opcional) Mostrar el prefijo y formato si quieres que el usuario los vea, pero no los edite
# st.sidebar.markdown("---")
# st.sidebar.markdown(f"**Prefijo en S3:** `{S3_OBJECT_PREFIX_FROM_SECRET}`")
# st.sidebar.markdown(f"**Formato de archivo para S3:** `{S3_FILE_FORMAT_FROM_SECRET}`")
# st.sidebar.markdown("*(Estos valores se toman de los secretos de la aplicación)*")


uploaded_file = st.file_uploader("📂 Elige un archivo CSV (delimitado por punto y coma)", type="csv")

if uploaded_file is not None:
    st.write("---")
    st.subheader("📄 Previsualización del CSV Original (primeras 5 filas):")
    try:
        df_original = pd.read_csv(uploaded_file, sep=';', encoding='utf-8-sig', dtype=str, keep_default_na=False, na_values=[''])
        df_original = df_original.fillna('')
        st.dataframe(df_original.head())

        if st.button("🚀 Procesar, Limpiar y Subir a S3"):
            if not s3_bucket_to_use or s3_bucket_to_use == "tu-bucket-s3-aqui":
                st.error("El nombre del Bucket S3 no está configurado correctamente. "
                         "Por favor, configura el secreto `S3_PROCESSED_BUCKET` en Streamlit Cloud.")
            elif not AWS_ACCESS_KEY_ID_LOADED or not AWS_SECRET_ACCESS_KEY_LOADED:
                 st.error("Las credenciales de AWS (AWS_ACCESS_KEY_ID o AWS_SECRET_ACCESS_KEY) "
                          "no están configuradas en los secretos de Streamlit. La subida a S3 fallará.")
            else:
                with st.spinner('Procesando archivo... Esto puede tardar unos segundos.'):
                    df_limpio = procesar_dataframe(df_original.copy())
                
                st.subheader("📊 Previsualización del CSV Limpio (DataFrame interno, primeras 5 filas):")
                st.dataframe(df_limpio.head())
                st.success("¡Procesamiento interno completado exitosamente!")

                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                base_filename_original = "ofertas_procesadas"
                if hasattr(uploaded_file, 'name') and uploaded_file.name:
                    base, _ = os.path.splitext(uploaded_file.name)
                    base_filename_original = f"{base}_limpio"
                
                final_prefix = ""
                if S3_OBJECT_PREFIX_FROM_SECRET: # Usar el prefijo del secreto
                    final_prefix = S3_OBJECT_PREFIX_FROM_SECRET.strip()
                    if not final_prefix.endswith('/'):
                        final_prefix += '/'
                
                s3_object_name_final = f"{final_prefix}{base_filename_original}_{timestamp}.{S3_FILE_FORMAT_FROM_SECRET}" # Usar el formato del secreto
                
                upload_successful = upload_df_to_s3(df_limpio, s3_bucket_to_use, s3_object_name_final, format_type=S3_FILE_FORMAT_FROM_SECRET)

                if upload_successful:
                    st.markdown("---")
                    st.info("El archivo también está disponible para descarga local.")
                    csv_limpio_bytes_local = convert_df_to_csv_for_download(df_limpio)
                    if csv_limpio_bytes_local:
                        nombre_archivo_descarga = f"{base_filename_original}_{timestamp}_descarga.csv"
                        st.download_button(
                            label="📥 Descargar CSV Limpio Localmente (delimitado por comas)",
                            data=csv_limpio_bytes_local,
                            file_name=nombre_archivo_descarga,
                            mime="text/csv",
                        )
                    else:
                        st.warning("No se pudo generar el archivo para descarga local.")
            
    except pd.errors.ParserError as pe:
        st.error(f"Error de Pandas al parsear el CSV de ENTRADA: {pe}")
        # ... (tu manejo de errores)
    except Exception as e:
        st.error(f"Ocurrió un error inesperado durante el procesamiento: {e}")
        # ... (tu manejo de errores)
else:
    st.info("👋 ¡Bienvenido! Por favor, sube un archivo CSV para comenzar.")

st.write("---")
st.markdown("© 2024 Proyecto Observatorio Laboral TI")