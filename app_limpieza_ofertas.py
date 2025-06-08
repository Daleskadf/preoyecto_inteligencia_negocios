import streamlit as st
import pandas as pd
import re
from datetime import datetime, timedelta
import os
import csv # Importar csv para las constantes de quoting
import boto3 # Para S3
import io    # Para buffers en memoria

# --- Constantes ---
CURRENT_YEAR = datetime.now().year

# --- Funciones de Limpieza (tus funciones, importante la salida de limpiar_salario) ---
def parse_fecha(fecha_str):
    if pd.isna(fecha_str) or not str(fecha_str).strip() or str(fecha_str).lower().strip() == 'nan': return None
    fecha_str = str(fecha_str).lower().strip()
    try:
        parsed_date = pd.to_datetime(fecha_str, errors='coerce')
        if pd.notna(parsed_date): return parsed_date.strftime('%Y-%m-%d')
    except Exception: pass
    if "hoy" in fecha_str: return datetime.now().strftime('%Y-%m-%d')
    if "ayer" in fecha_str: return (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    match_hace_dias = re.search(r'hace (\d+) días?', fecha_str)
    if match_hace_dias: return (datetime.now() - timedelta(days=int(match_hace_dias.group(1)))).strftime('%Y-%m-%d')
    match_hace_horas_min = re.search(r'hace (\d+) (horas?|minutos?)', fecha_str)
    if match_hace_horas_min: return datetime.now().strftime('%Y-%m-%d')
    meses_es = {"enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6, "julio": 7, "agosto": 8, "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12}
    match_dd_mes_yyyy = re.search(r'(\d{1,2})\s+de\s+([a-záéíóúñ]+)\s+de\s+(\d{4})', fecha_str, re.IGNORECASE)
    if match_dd_mes_yyyy:
        dia, mes_str, year = int(match_dd_mes_yyyy.group(1)), match_dd_mes_yyyy.group(2).lower(), int(match_dd_mes_yyyy.group(3))
        if mes_str in meses_es:
            try: return datetime(year, meses_es[mes_str], dia).strftime('%Y-%m-%d')
            except ValueError: return None
    match_dd_mes = re.search(r'(\d{1,2})\s+de\s+([a-záéíóúñ]+)', fecha_str, re.IGNORECASE)
    if match_dd_mes:
        dia, mes_str = int(match_dd_mes.group(1)), match_dd_mes.group(2).lower()
        if mes_str in meses_es:
            year_a_usar = CURRENT_YEAR
            try:
                fecha_propuesta = datetime(year_a_usar, meses_es[mes_str], dia)
                if fecha_propuesta > datetime.now() + timedelta(days=60): year_a_usar -=1
                return datetime(year_a_usar, meses_es[mes_str], dia).strftime('%Y-%m-%d')
            except ValueError: return None
    return None

def limpiar_salario(monto_str, moneda_str_original, tipo_pago_str_original):
    monto_limpio, moneda_limpia, tipo_pago_limpio = None, None, None # EMPIEZAN COMO None
    if pd.notna(monto_str) and str(monto_str).strip():
        monto_str_lower = str(monto_str).lower().strip()
        if monto_str_lower not in ['no disponible', 'a convenir', 'según mercado', 'nan', 'acordar', 'negociable']:
            s = str(monto_str).replace('S/.', '').replace('USD', '').replace('EUR', '')
            # Ajusta esto según el formato de miles/decimales de tu CSV de ENTRADA
            s = s.replace('.', '').replace(',', '.') # Asume punto miles, coma decimal -> convierte a punto decimal
            # Si tu entrada es 3500.0 (punto decimal), entonces solo necesitas quitar comas de miles si las hay:
            # s = s.replace(',', '') 
            match_num = re.search(r'(\d+\.?\d*)', s)
            if match_num:
                try: monto_limpio = float(match_num.group(1)) # monto_limpio es float o None
                except ValueError: pass # monto_limpio sigue siendo None
    
    if monto_limpio is not None: # Solo procesar moneda y tipo si hay monto
        if pd.notna(moneda_str_original) and str(moneda_str_original).strip():
            # ... (tu lógica de moneda) ...
            moneda_str_lower = str(moneda_str_original).lower().strip()
            if moneda_str_lower not in ['no disponible', 'nan']:
                moneda_limpia = str(moneda_str_original).upper().strip()
                if "S/." in moneda_limpia or "SOL" in moneda_limpia or "PEN" in moneda_limpia: moneda_limpia = "PEN"
                elif "$" in moneda_limpia or "USD" in moneda_limpia: moneda_limpia = "USD"
        if moneda_limpia is None: # Si no se pudo determinar por string, intentar inferir del monto_str
            if pd.notna(monto_str) and str(monto_str).strip():
                if 'S/.' in str(monto_str) : moneda_limpia = "PEN"
                elif '$' in str(monto_str) : moneda_limpia = "USD" # Asumir USD si hay $
        if moneda_limpia is None : moneda_limpia = "PEN" # Default a PEN si todo falla

        if pd.notna(tipo_pago_str_original) and str(tipo_pago_str_original).strip():
            # ... (tu lógica de tipo_pago) ...
            tipo_pago_lower = str(tipo_pago_str_original).lower().strip()
            if tipo_pago_lower not in ['no disponible', 'nan', 'acordar', 'negociable']:
                tipo_pago_limpio = str(tipo_pago_str_original).strip().capitalize()
        if tipo_pago_limpio is None and monto_limpio > 200: # Heurística: si el monto es > 200, probable mensual
             tipo_pago_limpio = "Mensual"
        elif tipo_pago_limpio is None:
             tipo_pago_limpio = "No especificado"
             
    return monto_limpio, moneda_limpia, tipo_pago_limpio # monto_limpio es float o None

# ... (resto de tus funciones de limpieza: limpiar_edad, capitalizar_texto, limpiar_lista_delimitada) ...
# Asegúrate que limpiar_edad y cualquier otra que devuelva números también devuelva None si no es convertible.
def limpiar_edad(edad_str):
    if pd.isna(edad_str) or str(edad_str).lower().strip() in ['no disponible', 'nan', '']: return None
    try: return int(float(str(edad_str))) # Convertir a float primero por si es "25.0"
    except ValueError:
        match = re.search(r'(\d+)', str(edad_str))
        if match:
            try: return int(match.group(1))
            except ValueError: return None
    return None

# --- Lógica Principal de Procesamiento (como la tenías, con el orden correcto) ---
def procesar_dataframe(df_input):
    # ... (tu código de procesar_dataframe, asegurándote que las columnas _Limpio para salario, edad, años_exp
    #      realmente contengan los números (float/int) o None de Python) ...
    # ... el df_final_ordenado se genera al final ...
    st.write("Iniciando limpieza y transformación de datos...")
    df = df_input.copy()
    columnas_esperadas_del_csv_original = ['ID_Oferta', 'Título', 'Ciudad', 'Region_Departamento', 'Fecha_Publicacion', 'Tipo_Contrato', 'Tipo_Jornada', 'Modalidad_Trabajo', 'Salario_Monto', 'Salario_Moneda', 'Salario_Tipo_Pago', 'Descripcion_Oferta_Raw', 'Lenguajes', 'Frameworks', 'gestores_db', 'Herramientas', 'nivel_ingles', 'nivel_educacion', 'Anos_Experiencia', 'Conocimientos_Adicionales', 'Edad_minima', 'Edad_maxima', 'NombreEmpresa', 'DescripciónEmpresa', 'Enlace_Oferta', 'Categoría']
    for col_esperada in columnas_esperadas_del_csv_original:
        if col_esperada not in df.columns:
            df[col_esperada] = pd.NA
    df['Fecha_Publicacion_Limpia'] = df['Fecha_Publicacion'].apply(parse_fecha)
    salarios_limpios = df.apply(lambda r: limpiar_salario(r['Salario_Monto'], r['Salario_Moneda'], r['Salario_Tipo_Pago']), axis=1)
    df[['Salario_Monto_Limpio', 'Salario_Moneda_Limpia', 'Salario_Tipo_Pago_Limpio']] = pd.DataFrame(salarios_limpios.tolist(), index=df.index)
    df['Edad_minima_Limpia'] = df['Edad_minima'].apply(limpiar_edad)
    df['Edad_maxima_Limpia'] = df['Edad_maxima'].apply(limpiar_edad)
    columnas_texto_a_capitalizar = ['Título', 'Ciudad', 'Region_Departamento', 'Tipo_Contrato', 'Tipo_Jornada', 'Modalidad_Trabajo', 'nivel_ingles', 'nivel_educacion', 'NombreEmpresa', 'Categoría']
    for col in columnas_texto_a_capitalizar: df[col + '_Limpio'] = df[col].apply(capitalizar_texto)
    columnas_lista_a_limpiar = ['Lenguajes', 'Frameworks', 'gestores_db', 'Herramientas', 'Conocimientos_Adicionales']
    for col in columnas_lista_a_limpiar: df[col + '_Lista_Limpia'] = df[col].apply(lambda x: limpiar_lista_delimitada(x, delimitador=','))
    # Para Anos_Experiencia_Limpio, la conversión a numérico debe devolver float o Int64 que pueda manejar NaNs
    df['Anos_Experiencia_Limpio_Temp'] = pd.to_numeric(df['Anos_Experiencia'], errors='coerce')
    df['Anos_Experiencia_Limpio'] = df['Anos_Experiencia_Limpio_Temp'].astype('Int64', errors='ignore')


    mapa_a_nombres_finales = {
        'ID_Oferta': 'ID_Oferta', 'Título_Limpio': 'Titulo_Oferta', 'Ciudad_Limpio': 'Ciudad',
        'Region_Departamento_Limpio': 'Region_Departamento', 'Fecha_Publicacion_Limpia': 'Fecha_Publicacion',
        'Tipo_Contrato_Limpio': 'Tipo_Contrato', 'Tipo_Jornada_Limpio': 'Tipo_Jornada',
        'Modalidad_Trabajo_Limpio': 'Modalidad_Trabajo', 'Salario_Monto_Limpio': 'Salario_Monto',
        'Salario_Moneda_Limpia': 'Salario_Moneda', 'Salario_Tipo_Pago_Limpio': 'Salario_Tipo_Pago',
        'Lenguajes_Lista_Limpia': 'Lenguajes_Lista', 'Frameworks_Lista_Limpia': 'Frameworks_Lista',
        'gestores_db_Lista_Limpia': 'Bases_Datos_Lista', 'Herramientas_Lista_Limpia': 'Herramientas_Lista',
        'nivel_ingles_Limpio': 'Nivel_Ingles', 'nivel_educacion_Limpio': 'Nivel_Educacion',
        'Anos_Experiencia_Limpio': 'Anos_Experiencia', 
        'Conocimientos_Adicionales_Lista_Limpia': 'Conocimientos_Adicionales_Lista',
        'Edad_minima_Limpia': 'Edad_Minima', 'Edad_maxima_Limpia': 'Edad_Maxima',
        'Categoría_Limpio': 'Categoria_Puesto', 'NombreEmpresa_Limpio': 'Nombre_Empresa',
        'DescripciónEmpresa': 'Contenido_Descripcion_Empresa',
        'Enlace_Oferta': 'Enlace_Oferta',
        'Descripcion_Oferta_Raw': 'Contenido_Descripcion_Oferta'
    }
    df_renombrado = pd.DataFrame()
    for key_en_df_intermedio, nombre_columna_final in mapa_a_nombres_finales.items():
        if key_en_df_intermedio in df.columns: df_renombrado[nombre_columna_final] = df[key_en_df_intermedio]
        else:
            original_key = key_en_df_intermedio.replace('_Limpio', '').replace('_Lista_Limpia', '')
            if original_key in df.columns: df_renombrado[nombre_columna_final] = df[original_key]
            else:
                st.warning(f"Columna '{key_en_df_intermedio}' (ni '{original_key}') no encontrada para '{nombre_columna_final}'.")
                df_renombrado[nombre_columna_final] = pd.NA
    orden_final_columnas_csv = [
        'ID_Oferta', 'Titulo_Oferta', 'Ciudad', 'Region_Departamento', 'Fecha_Publicacion',
        'Tipo_Contrato', 'Tipo_Jornada', 'Modalidad_Trabajo', 'Salario_Monto',
        'Salario_Moneda', 'Salario_Tipo_Pago', 'Lenguajes_Lista', 'Frameworks_Lista',
        'Bases_Datos_Lista', 'Herramientas_Lista', 'Nivel_Ingles', 'Nivel_Educacion',
        'Anos_Experiencia', 'Conocimientos_Adicionales_Lista', 'Edad_Minima', 'Edad_Maxima',
        'Categoria_Puesto', 'Nombre_Empresa', 'Contenido_Descripcion_Empresa',
        'Enlace_Oferta', 'Contenido_Descripcion_Oferta'
    ]
    for col_orden in orden_final_columnas_csv:
        if col_orden not in df_renombrado.columns:
            st.error(f"Error: Columna '{col_orden}' en 'orden_final_columnas_csv' no está en df_renombrado.")
            return pd.DataFrame(columns=orden_final_columnas_csv)
    df_final_ordenado = df_renombrado[orden_final_columnas_csv]
    st.write("Limpieza y transformación completadas.")
    return df_final_ordenado

# --- Función para convertir a CSV para descarga local ---
@st.cache_data
def convert_df_to_csv_for_download(df_to_convert):
    df_copy = df_to_convert.copy()
    cols_texto_largo = ['Contenido_Descripcion_Oferta', 'Contenido_Descripcion_Empresa']
    for col_name in cols_texto_largo:
        if col_name in df_copy.columns and df_copy[col_name].notna().any():
            df_copy[col_name] = df_copy[col_name].astype(str).str.replace('\r\n', ' ', regex=False).str.replace('\n', ' ', regex=False).str.replace('\r', ' ', regex=False)
            df_copy[col_name] = df_copy[col_name].str.replace(r'\s+', ' ', regex=True).str.strip()
    try:
        # CAMBIO AQUÍ: na_rep='\\N'
        csv_output = df_copy.to_csv(index=False, encoding='utf-8-sig', sep=',', na_rep='\\N', quoting=csv.QUOTE_ALL, escapechar='"')
        return csv_output.encode('utf-8-sig')
    except Exception as e:
        st.error(f"Error durante la conversión a CSV para descarga: {e}")
        return None

# --- Función para subir DataFrame a S3 ---
def upload_df_to_s3(df_to_upload, bucket_name, s3_object_key_name, format_type="csv"):
    st.write(f"Intentando subir DataFrame a S3: s3://{bucket_name}/{s3_object_key_name}")
    try:
        s3_resource = boto3.resource('s3')
        df_for_s3 = df_to_upload.copy()

        if format_type.lower() == "csv":
            cols_texto_largo = ['Contenido_Descripcion_Oferta', 'Contenido_Descripcion_Empresa']
            for col_name in cols_texto_largo:
                if col_name in df_for_s3.columns and df_for_s3[col_name].notna().any():
                    df_for_s3[col_name] = df_for_s3[col_name].astype(str).str.replace('\r\n', ' ', regex=False).str.replace('\n', ' ', regex=False).str.replace('\r', ' ', regex=False)
                    df_for_s3[col_name] = df_for_s3[col_name].str.replace(r'\s+', ' ', regex=True).str.strip()
            
            csv_buffer = io.StringIO()
            # CAMBIO AQUÍ: na_rep='\\N'
            df_for_s3.to_csv(csv_buffer, index=False, encoding='utf-8-sig', sep=',', quoting=csv.QUOTE_ALL, escapechar='"', na_rep='\\N')
            s3_resource.Object(bucket_name, s3_object_key_name).put(Body=csv_buffer.getvalue().encode('utf-8-sig'))
            st.success(f"DataFrame subido como CSV exitosamente a s3://{bucket_name}/{s3_object_key_name}")

        elif format_type.lower() == "parquet":
            parquet_buffer = io.BytesIO()
            df_for_s3.to_parquet(parquet_buffer, index=False, engine='pyarrow')
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

# --- Interfaz de Streamlit (sin cambios aquí) ---
st.set_page_config(page_title="Limpiador CSV Ofertas vS3", layout="wide")
st.title("🧹 Limpiador y Estandarizador de CSV de Ofertas Laborales (con Subida a S3)")
st.markdown("Sube un archivo CSV (delimitado por **punto y coma ;** y codificado en **UTF-8**) para limpiarlo, estandarizarlo y subirlo a S3.")

AWS_ACCESS_KEY_ID_LOADED = st.secrets.get("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY_LOADED = st.secrets.get("AWS_SECRET_ACCESS_KEY", "")
S3_BUCKET_NAME_FROM_SECRET = st.secrets.get("S3_PROCESSED_BUCKET", "")
S3_OBJECT_PREFIX_FROM_SECRET = st.secrets.get("S3_OBJECT_PREFIX", "ofertas_limpias/")
S3_FILE_FORMAT_FROM_SECRET = st.secrets.get("S3_FILE_FORMAT", "csv")

s3_bucket_to_use = S3_BUCKET_NAME_FROM_SECRET
if not s3_bucket_to_use:
    st.sidebar.error("⚠️ Nombre del Bucket S3 (`S3_PROCESSED_BUCKET`) no configurado en los secretos.")
    st.stop()
else:
    st.sidebar.success(f"✔️ Usando Bucket S3: {s3_bucket_to_use}")
    st.sidebar.info(f"Prefijo S3: {S3_OBJECT_PREFIX_FROM_SECRET}")
    st.sidebar.info(f"Formato S3: {S3_FILE_FORMAT_FROM_SECRET.upper()}")

uploaded_file = st.file_uploader("📂 Elige un archivo CSV (delimitado por ;)", type="csv")

if uploaded_file is not None:
    st.write("---")
    st.subheader("📄 Previsualización del CSV Original (primeras 5 filas):")
    try:
        df_original = pd.read_csv(uploaded_file, sep=';', encoding='utf-8-sig', dtype=str, keep_default_na=False, na_values=[''])
        df_original = df_original.fillna('')
        st.dataframe(df_original.head())

        if st.button("🚀 Procesar, Limpiar y Subir a S3"):
            if not AWS_ACCESS_KEY_ID_LOADED or not AWS_SECRET_ACCESS_KEY_LOADED:
                 st.error("Credenciales AWS (`AWS_ACCESS_KEY_ID` o `AWS_SECRET_ACCESS_KEY`) no configuradas en los secretos.")
            else:
                with st.spinner('Procesando archivo...'):
                    df_procesado_y_ordenado = procesar_dataframe(df_original.copy())
                
                if not df_procesado_y_ordenado.empty:
                    st.subheader("📊 Previsualización del DataFrame Limpio y Ordenado (listo para CSV):")
                    st.dataframe(df_procesado_y_ordenado.head())
                    st.success("¡Procesamiento interno completado!")

                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    base_filename_original = "ofertas_procesadas"
                    if hasattr(uploaded_file, 'name') and uploaded_file.name:
                        base, _ = os.path.splitext(uploaded_file.name)
                        base_filename_original = f"{base}_limpio"
                    
                    final_prefix = S3_OBJECT_PREFIX_FROM_SECRET.strip()
                    if final_prefix and not final_prefix.endswith('/'): final_prefix += '/'
                    s3_object_name_final = f"{final_prefix}{base_filename_original}_{timestamp}.{S3_FILE_FORMAT_FROM_SECRET}"
                    
                    upload_successful = upload_df_to_s3(df_procesado_y_ordenado, s3_bucket_to_use, s3_object_name_final, format_type=S3_FILE_FORMAT_FROM_SECRET)

                    if upload_successful:
                        st.markdown("---")
                        st.info("Opción de descarga local:")
                        csv_limpio_bytes_local = convert_df_to_csv_for_download(df_procesado_y_ordenado)
                        if csv_limpio_bytes_local:
                            nombre_archivo_descarga = f"{base_filename_original}_{timestamp}_descarga.csv"
                            st.download_button(
                                label="📥 Descargar CSV Limpio Localmente",
                                data=csv_limpio_bytes_local,
                                file_name=nombre_archivo_descarga,
                                mime="text/csv",
                            )
                else:
                    st.error("El procesamiento del DataFrame resultó en un DataFrame vacío o hubo un error.")
    except Exception as e:
        st.error(f"Ocurrió un error al procesar el archivo: {e}")
        st.exception(e)
else:
    st.info("👋 ¡Bienvenido! Sube un archivo CSV para comenzar.")

st.write("---")
st.markdown("© 2024 Proyecto Observatorio Laboral TI")