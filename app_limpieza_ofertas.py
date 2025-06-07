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

# --- Funciones de Limpieza (Asumo que estas ya están bien definidas por ti) ---
def parse_fecha(fecha_str):
    if pd.isna(fecha_str): return None
    fecha_str = str(fecha_str).lower().strip()
    if not fecha_str or fecha_str == 'nan': return None
    try:
        # Intentar parsear formatos comunes directamente si es posible
        # Esto podría mejorarse con dateutil.parser si los formatos son muy variados
        parsed_date = pd.to_datetime(fecha_str, errors='coerce')
        if pd.notna(parsed_date):
            return parsed_date.strftime('%Y-%m-%d')
    except Exception:
        pass # Continuar con las reglas regex si pd.to_datetime falla o no es el formato esperado
    
    if "hoy" in fecha_str: return datetime.now().strftime('%Y-%m-%d')
    if "ayer" in fecha_str: return (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    match_hace_dias = re.search(r'hace (\d+) días?', fecha_str)
    if match_hace_dias: return (datetime.now() - timedelta(days=int(match_hace_dias.group(1)))).strftime('%Y-%m-%d')
    
    match_hace_horas_min = re.search(r'hace (\d+) (horas?|minutos?)', fecha_str)
    if match_hace_horas_min: return datetime.now().strftime('%Y-%m-%d') # Fecha de hoy para "hace horas/minutos"
    
    meses_es = {
        "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
        "julio": 7, "agosto": 8, "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12
    }
    # Formato: DD de MES_NOMBRE de YYYY
    match_dd_mes_yyyy = re.search(r'(\d{1,2})\s+de\s+([a-záéíóúñ]+)\s+de\s+(\d{4})', fecha_str, re.IGNORECASE)
    if match_dd_mes_yyyy:
        dia, mes_str, year = int(match_dd_mes_yyyy.group(1)), match_dd_mes_yyyy.group(2).lower(), int(match_dd_mes_yyyy.group(3))
        if mes_str in meses_es:
            mes = meses_es[mes_str]
            try: return datetime(year, mes, dia).strftime('%Y-%m-%d')
            except ValueError: return None

    # Formato: DD de MES_NOMBRE (asumir año actual o anterior)
    match_dd_mes = re.search(r'(\d{1,2})\s+de\s+([a-záéíóúñ]+)', fecha_str, re.IGNORECASE)
    if match_dd_mes:
        dia, mes_str = int(match_dd_mes.group(1)), match_dd_mes.group(2).lower()
        if mes_str in meses_es:
            mes = meses_es[mes_str]
            year_a_usar = CURRENT_YEAR
            try:
                # Heurística: si la fecha propuesta es muy en el futuro, usar el año anterior
                fecha_propuesta = datetime(year_a_usar, mes, dia)
                if fecha_propuesta > datetime.now() + timedelta(days=60): # Si la fecha es más de 2 meses en el futuro
                    year_a_usar -=1
                return datetime(year_a_usar, mes, dia).strftime('%Y-%m-%d')
            except ValueError: return None
    return None # Si ningún formato coincide

def limpiar_salario(monto_str, moneda_str_original, tipo_pago_str_original):
    monto_limpio, moneda_limpia, tipo_pago_limpio = None, None, None
    if pd.notna(monto_str):
        monto_str_lower = str(monto_str).lower().strip()
        # Expandir lista de no disponibilidad
        if monto_str_lower not in ['no disponible', 'a convenir', 'según mercado', 'nan', '', 'acordar', 'negociable']:
            s = str(monto_str).replace('S/.', '').replace('USD', '').replace('EUR', '').replace('.', '').replace(',', '.') # Quitar símbolos de moneda y separadores de miles
            match_num = re.search(r'(\d+\.?\d*)', s) # Buscar el primer grupo de números
            if match_num:
                try: monto_limpio = float(match_num.group(1))
                except ValueError: pass
    
    if monto_limpio is not None:
        # Moneda
        if pd.notna(moneda_str_original):
            moneda_str_lower = str(moneda_str_original).lower().strip()
            if moneda_str_lower not in ['no disponible', 'nan', '']:
                moneda_limpia = str(moneda_str_original).upper().strip()
                if "S/." in moneda_limpia or "SOL" in moneda_limpia or "PEN" in moneda_limpia: moneda_limpia = "PEN"
                elif "$" in moneda_limpia or "USD" in moneda_limpia: moneda_limpia = "USD"
                # Añadir más monedas si es necesario
        if moneda_limpia is None: # Si no se pudo determinar por string, intentar inferir del monto_str
            if pd.notna(monto_str):
                if 'S/.' in str(monto_str) : moneda_limpia = "PEN"
                elif '$' in str(monto_str) : moneda_limpia = "USD" # Asumir USD si hay $
        if moneda_limpia is None : moneda_limpia = "PEN" # Default a PEN si todo falla

        # Tipo de Pago
        if pd.notna(tipo_pago_str_original):
            tipo_pago_lower = str(tipo_pago_str_original).lower().strip()
            if tipo_pago_lower not in ['no disponible', 'nan', '', 'acordar', 'negociable']:
                tipo_pago_limpio = str(tipo_pago_str_original).strip().capitalize()
        if tipo_pago_limpio is None and monto_limpio > 200: # Heurística: si el monto es > 200, probable mensual
             tipo_pago_limpio = "Mensual"
        elif tipo_pago_limpio is None:
             tipo_pago_limpio = "No especificado"
             
    return monto_limpio, moneda_limpia, tipo_pago_limpio

def limpiar_edad(edad_str):
    if pd.isna(edad_str) or str(edad_str).lower().strip() in ['no disponible', 'nan', '']: return None
    # Intentar convertir directamente a float y luego a int para manejar "25.0"
    try:
        return int(float(str(edad_str)))
    except ValueError:
        # Si falla, intentar con regex por si hay texto extra
        match = re.search(r'(\d+)', str(edad_str))
        if match:
            try: return int(match.group(1))
            except ValueError: return None
    return None

def capitalizar_texto(texto):
    if pd.isna(texto) or str(texto).strip() == '': return None # Considerar string vacío como nulo aquí
    texto_lower = str(texto).lower().strip()
    if texto_lower in ['no disponible', 'nan']: return None
    return str(texto).strip().capitalize()

def limpiar_lista_delimitada(texto_lista, delimitador=','):
    if pd.isna(texto_lista) or str(texto_lista).strip() == '': return None
    texto_lower = str(texto_lista).lower().strip()
    if texto_lower in ['no disponible', 'nan', 'llena nomas xd']: return None # Añadido 'llena nomas xd'
    
    # Quitar comillas dobles al inicio/final de cada item si Pandas las añadió
    items = [item.strip().strip('"').strip().capitalize() for item in str(texto_lista).split(delimitador) if item.strip().strip('"').strip()]
    return delimitador.join(items) if items else None

# --- Lógica Principal de Procesamiento ---
def procesar_dataframe(df_input):
    st.write("Iniciando limpieza y transformación de datos...")
    df = df_input.copy()
    progress_bar = st.progress(0)
    total_steps = 7 # Ajusta si añades más pasos de procesamiento

    # Paso 0: Asegurar que todas las columnas esperadas existan, aunque sea con Nones,
    # para evitar KeyErrors más adelante si el CSV de entrada es incompleto.
    columnas_esperadas_entrada = [
        'ID_Oferta', 'Título', 'Ciudad', 'Region_Departamento', 'Fecha_Publicacion',
        'Tipo_Contrato', 'Tipo_Jornada', 'Modalidad_Trabajo', 'Salario_Monto',
        'Salario_Moneda', 'Salario_Tipo_Pago', 'Descripcion_Oferta_Raw', 'Lenguajes',
        'Frameworks', 'gestores_db', 'Herramientas', 'nivel_ingles', 'nivel_educacion',
        'Anos_Experiencia', 'Conocimientos_Adicionales', 'Edad_minima', 'Edad_maxima',
        'NombreEmpresa', 'DescripciónEmpresa', 'Enlace_Oferta', 'Categoría'
    ]
    for col_esperada in columnas_esperadas_entrada:
        if col_esperada not in df.columns:
            df[col_esperada] = None # O pd.NA

    # 1. Fechas
    df['Fecha_Publicacion_Limpia'] = df['Fecha_Publicacion'].apply(parse_fecha)
    progress_bar.progress(1/total_steps)

    # 2. Salarios
    df['Salario_Monto_Input'] = df['Salario_Monto'] # Guardar el original para la función
    df['Salario_Moneda_Input'] = df['Salario_Moneda']
    df['Salario_Tipo_Pago_Input'] = df['Salario_Tipo_Pago']
    salarios_limpios = df.apply(lambda r: limpiar_salario(r['Salario_Monto_Input'], r['Salario_Moneda_Input'], r['Salario_Tipo_Pago_Input']), axis=1)
    df[['Salario_Monto_Limpio', 'Salario_Moneda_Limpia', 'Salario_Tipo_Pago_Limpio']] = pd.DataFrame(salarios_limpios.tolist(), index=df.index)
    progress_bar.progress(2/total_steps)

    # 3. Edades
    df['Edad_minima_Limpia'] = df['Edad_minima'].apply(limpiar_edad)
    df['Edad_maxima_Limpia'] = df['Edad_maxima'].apply(limpiar_edad)
    progress_bar.progress(3/total_steps)
    
    # 4. Estandarización de Texto (Capitalizar)
    columnas_texto_a_capitalizar = ['Título', 'Ciudad', 'Region_Departamento', 'Tipo_Contrato', 
                                   'Tipo_Jornada', 'Modalidad_Trabajo', 'nivel_ingles', 
                                   'nivel_educacion', 'NombreEmpresa', 'Categoría']
    for col in columnas_texto_a_capitalizar:
        df[col + '_Limpio'] = df[col].apply(capitalizar_texto)
    progress_bar.progress(4/total_steps)

    # 5. Limpieza de Listas Delimitadas
    columnas_lista_a_limpiar = ['Lenguajes', 'Frameworks', 'gestores_db', 'Herramientas', 'Conocimientos_Adicionales']
    for col in columnas_lista_a_limpiar:
        df[col + '_Lista_Limpia'] = df[col].apply(lambda x: limpiar_lista_delimitada(x, delimitador=','))
    progress_bar.progress(5/total_steps)
            
    # 6. Años de Experiencia
    df['Anos_Experiencia_Limpio'] = pd.to_numeric(df['Anos_Experiencia'], errors='coerce').astype('Int64', errors='ignore') # Usar Int64 para permitir <NA>
    progress_bar.progress(6/total_steps)

    # 7. Seleccionar y renombrar columnas finales según el orden deseado
    #    Las CLAVES son las columnas PROCESADAS en df (ej. 'Título_Limpio')
    #    Los VALORES son los nombres FINALES que quieres en tu CSV/Athena (ej. 'Titulo_Oferta')
    columnas_finales_map_ordenado = {
        # Clave (columna en df procesado) : Valor (nombre final en CSV)
        'ID_Oferta': 'ID_Oferta',
        'Título_Limpio': 'Titulo_Oferta',
        'Ciudad_Limpio': 'Ciudad',
        'Region_Departamento_Limpio': 'Region_Departamento',
        'Fecha_Publicacion_Limpia': 'Fecha_Publicacion',
        'Tipo_Contrato_Limpio': 'Tipo_Contrato',
        'Tipo_Jornada_Limpio': 'Tipo_Jornada',
        'Modalidad_Trabajo_Limpio': 'Modalidad_Trabajo',
        'Salario_Monto_Limpio': 'Salario_Monto',
        'Salario_Moneda_Limpia': 'Salario_Moneda',
        'Salario_Tipo_Pago_Limpio': 'Salario_Tipo_Pago',
        # Aquí el orden que especificaste
        'Lenguajes_Lista_Limpia': 'Lenguajes_Lista',
        'Frameworks_Lista_Limpia': 'Frameworks_Lista',
        'gestores_db_Lista_Limpia': 'Bases_Datos_Lista', # La clave es 'gestores_db_Lista_Limpia'
        'Herramientas_Lista_Limpia': 'Herramientas_Lista',
        'nivel_ingles_Limpio': 'Nivel_Ingles',
        'nivel_educacion_Limpio': 'Nivel_Educacion',
        'Anos_Experiencia_Limpio': 'Anos_Experiencia',
        'Conocimientos_Adicionales_Lista_Limpia': 'Conocimientos_Adicionales_Lista',
        'Edad_minima_Limpia': 'Edad_Minima',
        'Edad_maxima_Limpia': 'Edad_Maxima',
        'Categoría_Limpio': 'Categoria_Puesto',
        'NombreEmpresa_Limpio': 'Nombre_Empresa',
        'DescripciónEmpresa': 'Contenido_Descripcion_Empresa', # Clave original 'DescripciónEmpresa', nombre final único
        'Enlace_Oferta': 'Enlace_Oferta',
        'Descripcion_Oferta_Raw': 'Contenido_Descripcion_Oferta' # Clave original 'Descripcion_Oferta_Raw', nombre final único
    }
    
    df_final = pd.DataFrame()
    for processed_col_key, final_col_name in columnas_finales_map_ordenado.items():
        if processed_col_key in df.columns:
            df_final[final_col_name] = df[processed_col_key]
        else:
            # Si la clave procesada no existe, intenta con la clave original (sin sufijo _Limpio o _Lista_Limpia)
            # Esto es un fallback, idealmente todas las claves procesadas deberían existir.
            original_key_candidate = processed_col_key.replace('_Limpio', '').replace('_Lista_Limpia', '')
            if original_key_candidate in df.columns:
                st.warning(f"Usando columna original '{original_key_candidate}' para '{final_col_name}' porque '{processed_col_key}' no se encontró.")
                df_final[final_col_name] = df[original_key_candidate]
            else:
                st.warning(f"Columna fuente '{processed_col_key}' (ni '{original_key_candidate}') no encontrada para el nombre final '{final_col_name}'. Se creará como vacía.")
                df_final[final_col_name] = pd.Series([None] * len(df), dtype='object')
            
    progress_bar.progress(7/total_steps)
    return df_final

# --- Función para convertir a CSV para descarga local ---
@st.cache_data
def convert_df_to_csv_for_download(df_to_convert):
    df_copy = df_to_convert.copy()
    # Columnas con texto largo que necesitan manejo especial de saltos de línea para CSV
    cols_texto_largo = ['Contenido_Descripcion_Oferta', 'Contenido_Descripcion_Empresa'] # Usar nombres finales
    for col_name in cols_texto_largo:
        if col_name in df_copy.columns and df_copy[col_name].notna().any():
            df_copy[col_name] = df_copy[col_name].astype(str).str.replace('\r\n', ' ', regex=False).str.replace('\n', ' ', regex=False).str.replace('\r', ' ', regex=False)
            df_copy[col_name] = df_copy[col_name].str.replace(r'\s+', ' ', regex=True).str.strip()
    try:
        # Usar QUOTE_ALL para máxima robustez, especialmente con descripciones largas
        csv_output = df_copy.to_csv(index=False, encoding='utf-8-sig', sep=',', na_rep='', quoting=csv.QUOTE_ALL)
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
            # Pre-procesamiento para CSV robusto
            cols_texto_largo = ['Contenido_Descripcion_Oferta', 'Contenido_Descripcion_Empresa'] # Usar nombres finales
            for col_name in cols_texto_largo:
                if col_name in df_for_s3.columns and df_for_s3[col_name].notna().any():
                    df_for_s3[col_name] = df_for_s3[col_name].astype(str).str.replace('\r\n', ' ', regex=False).str.replace('\n', ' ', regex=False).str.replace('\r', ' ', regex=False)
                    df_for_s3[col_name] = df_for_s3[col_name].str.replace(r'\s+', ' ', regex=True).str.strip()
            
            csv_buffer = io.StringIO()
            # Usar QUOTE_ALL para asegurar que todos los campos estén entrecomillados
            df_for_s3.to_csv(csv_buffer, index=False, encoding='utf-8-sig', sep=',', quoting=csv.QUOTE_ALL, na_rep='')
            s3_resource.Object(bucket_name, s3_object_key_name).put(Body=csv_buffer.getvalue().encode('utf-8-sig'))
            st.success(f"DataFrame subido como CSV exitosamente a s3://{bucket_name}/{s3_object_key_name}")

        elif format_type.lower() == "parquet":
            parquet_buffer = io.BytesIO()
            # Para Parquet, es mejor que los Nones de Python sean NaNs de Pandas o tipos nullable.
            # El to_parquet maneja bien los tipos de Pandas.
            df_for_s3.to_parquet(parquet_buffer, index=False)
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
# ... (el resto de tu código de interfaz de Streamlit se mantiene igual,
#      asegurándote de que S3_BUCKET_NAME_FROM_SECRET, S3_OBJECT_PREFIX_FROM_SECRET,
#      y S3_FILE_FORMAT_FROM_SECRET se lean correctamente de los secretos
#      y se usen en la llamada a upload_df_to_s3) ...

# Ejemplo de cómo se usa en la parte principal:
st.set_page_config(page_title="Limpiador CSV Ofertas vS3", layout="wide")
st.title("🧹 Limpiador y Estandarizador de CSV de Ofertas Laborales (con Subida a S3)")
# ... (markdown) ...

AWS_ACCESS_KEY_ID_LOADED = st.secrets.get("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY_LOADED = st.secrets.get("AWS_SECRET_ACCESS_KEY", "")
S3_BUCKET_NAME_FROM_SECRET = st.secrets.get("S3_PROCESSED_BUCKET", "")
S3_OBJECT_PREFIX_FROM_SECRET = st.secrets.get("S3_OBJECT_PREFIX", "ofertas_limpias/")
S3_FILE_FORMAT_FROM_SECRET = st.secrets.get("S3_FILE_FORMAT", "csv")

if not S3_BUCKET_NAME_FROM_SECRET:
    st.sidebar.warning("⚠️ Nombre del Bucket S3 no configurado en los secretos de Streamlit.")
    s3_bucket_to_use = "tu-bucket-s3-aqui-por-defecto"
else:
    s3_bucket_to_use = S3_BUCKET_NAME_FROM_SECRET

uploaded_file = st.file_uploader("📂 Elige un archivo CSV (delimitado por punto y coma)", type="csv")

if uploaded_file is not None:
    st.write("---")
    st.subheader("📄 Previsualización del CSV Original (primeras 5 filas):")
    try:
        df_original = pd.read_csv(uploaded_file, sep=';', encoding='utf-8-sig', dtype=str, keep_default_na=False, na_values=[''])
        df_original = df_original.fillna('')
        st.dataframe(df_original.head())

        if st.button("🚀 Procesar, Limpiar y Subir a S3"):
            if not s3_bucket_to_use or s3_bucket_to_use == "tu-bucket-s3-aqui-por-defecto":
                st.error("El nombre del Bucket S3 no está configurado correctamente.")
            elif not AWS_ACCESS_KEY_ID_LOADED or not AWS_SECRET_ACCESS_KEY_LOADED:
                 st.error("Credenciales AWS no configuradas en los secretos.")
            else:
                with st.spinner('Procesando archivo...'):
                    df_limpio = procesar_dataframe(df_original.copy())
                
                st.subheader("📊 Previsualización del CSV Limpio (DataFrame interno):")
                st.dataframe(df_limpio.head())
                st.success("¡Procesamiento interno completado!")

                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                base_filename_original = "ofertas_procesadas"
                if hasattr(uploaded_file, 'name') and uploaded_file.name:
                    base, _ = os.path.splitext(uploaded_file.name)
                    base_filename_original = f"{base}_limpio"
                
                final_prefix = S3_OBJECT_PREFIX_FROM_SECRET.strip()
                if final_prefix and not final_prefix.endswith('/'):
                    final_prefix += '/'
                
                s3_object_name_final = f"{final_prefix}{base_filename_original}_{timestamp}.{S3_FILE_FORMAT_FROM_SECRET}"
                
                upload_successful = upload_df_to_s3(df_limpio, s3_bucket_to_use, s3_object_name_final, format_type=S3_FILE_FORMAT_FROM_SECRET)

                if upload_successful:
                    st.markdown("---")
                    st.info("Opción de descarga local:")
                    csv_limpio_bytes_local = convert_df_to_csv_for_download(df_limpio) # Usa la función correcta
                    if csv_limpio_bytes_local:
                        nombre_archivo_descarga = f"{base_filename_original}_{timestamp}_descarga.csv"
                        st.download_button(
                            label="📥 Descargar CSV Limpio Localmente",
                            data=csv_limpio_bytes_local,
                            file_name=nombre_archivo_descarga,
                            mime="text/csv",
                        )
    except Exception as e:
        st.error(f"Ocurrió un error: {e}")
        st.exception(e)
else:
    st.info("👋 ¡Bienvenido! Sube un archivo CSV para comenzar.")

st.write("---")
st.markdown("© 2024 Proyecto Observatorio Laboral TI")