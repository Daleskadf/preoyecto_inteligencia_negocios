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

# --- Funciones de Limpieza (TU LÓGICA - SIN CAMBIOS) ---
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
    monto_limpio, moneda_limpia, tipo_pago_limpio = None, None, None
    if pd.notna(monto_str) and str(monto_str).strip():
        monto_str_lower = str(monto_str).lower().strip()
        invalidos_salario = ['no disponible', 'a convenir', 'según mercado', 'nan', 'acordar', 'negociable', '']
        if monto_str_lower not in invalidos_salario:
            s = str(monto_str).replace('S/.', '').replace('USD', '').replace('EUR', '')
            s = s.replace(',', '') # Asumiendo que las comas son separadores de miles para la entrada
            match_num = re.search(r'(\d+\.?\d*)', s) # Punto como decimal
            if match_num:
                try: monto_limpio = float(match_num.group(1))
                except ValueError: pass
    if monto_limpio is not None:
        if pd.notna(moneda_str_original) and str(moneda_str_original).strip():
            moneda_str_lower = str(moneda_str_original).lower().strip()
            if moneda_str_lower not in ['no disponible', 'nan', '']:
                moneda_limpia = str(moneda_str_original).upper().strip()
                if "S/." in moneda_limpia or "SOL" in moneda_limpia or "PEN" in moneda_limpia: moneda_limpia = "PEN"
                elif "$" in moneda_limpia or "USD" in moneda_limpia: moneda_limpia = "USD"
        if moneda_limpia is None:
            if pd.notna(monto_str) and str(monto_str).strip():
                if 'S/.' in str(monto_str): moneda_limpia = "PEN"
                elif '$' in str(monto_str): moneda_limpia = "USD"
        if moneda_limpia is None : moneda_limpia = "PEN"
        if pd.notna(tipo_pago_str_original) and str(tipo_pago_str_original).strip():
            tipo_pago_lower = str(tipo_pago_str_original).lower().strip()
            if tipo_pago_lower not in ['no disponible', 'nan', 'acordar', 'negociable']:
                tipo_pago_limpio = str(tipo_pago_str_original).strip().capitalize()
        if tipo_pago_limpio is None and monto_limpio > 200: tipo_pago_limpio = "Mensual"
        elif tipo_pago_limpio is None: tipo_pago_limpio = "No especificado"
    return monto_limpio, moneda_limpia, tipo_pago_limpio

def limpiar_edad(edad_str):
    if pd.isna(edad_str) or str(edad_str).lower().strip() in ['no disponible', 'nan', '']: return None
    try: return int(float(str(edad_str)))
    except ValueError:
        match = re.search(r'(\d+)', str(edad_str))
        if match:
            try: return int(match.group(1))
            except ValueError: return None
    return None

def capitalizar_texto(texto):
    if pd.isna(texto) or str(texto).strip() == '': return None
    texto_lower = str(texto).lower().strip()
    if texto_lower in ['no disponible', 'nan']: return None
    return str(texto).strip().capitalize()

def limpiar_lista_delimitada(texto_lista, delimitador=','):
    if pd.isna(texto_lista) or str(texto_lista).strip() == '': return None
    texto_lower = str(texto_lista).lower().strip()
    if texto_lower in ['no disponible', 'nan', 'llena nomas xd']: return None
    items = [item.strip().strip('"').strip().capitalize() for item in str(texto_lista).split(delimitador) if item.strip().strip('"').strip()]
    return delimitador.join(items) if items else None

# --- Lógica Principal de Procesamiento (CON ORDEN FINAL Y NOMBRES ÚNICOS) ---
def procesar_dataframe(df_input):
    st.write("Iniciando proceso de estandarización y preparación de datos...") # Texto Modificado
    df = df_input.copy()
    
    columnas_esperadas_del_csv_original = [
        'ID_Oferta', 'Título', 'Ciudad', 'Region_Departamento', 'Fecha_Publicacion',
        'Tipo_Contrato', 'Tipo_Jornada', 'Modalidad_Trabajo', 'Salario_Monto',
        'Salario_Moneda', 'Salario_Tipo_Pago', 'Descripcion_Oferta_Raw', 'Lenguajes',
        'Frameworks', 'gestores_db', 'Herramientas', 'nivel_ingles', 'nivel_educacion',
        'Anos_Experiencia', 'Conocimientos_Adicionales', 'Edad_minima', 'Edad_maxima',
        'NombreEmpresa', 'DescripciónEmpresa', 'Enlace_Oferta', 'Categoría'
    ]
    for col_esperada in columnas_esperadas_del_csv_original:
        if col_esperada not in df.columns:
            st.warning(f"Advertencia: Columna '{col_esperada}' no encontrada en el archivo CSV subido. Se intentará continuar.")
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
                st.warning(f"Advertencia: Columna fuente '{key_en_df_intermedio}' (ni '{original_key}') no encontrada para el nombre final '{nombre_columna_final}'. Se creará como Nula.")
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
    try:
        df_final_ordenado = df_renombrado[orden_final_columnas_csv]
    except KeyError as e:
        st.error(f"Error crítico: Una columna esperada para el orden final no existe en el DataFrame procesado: {e}")
        st.error(f"Columnas disponibles después del renombrado: {df_renombrado.columns.tolist()}")
        return pd.DataFrame(columns=orden_final_columnas_csv)

    st.write("Datos preparados y ordenados exitosamente.") # Texto Modificado
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
        csv_output = df_copy.to_csv(index=False, encoding='utf-8-sig', sep=',', na_rep='\\N', quoting=csv.QUOTE_ALL, escapechar='"')
        return csv_output.encode('utf-8-sig')
    except Exception as e:
        st.error(f"Error durante la conversión a CSV para descarga: {e}")
        return None

# --- Función para subir DataFrame a S3 ---
def upload_df_to_s3(df_to_upload, bucket_name, s3_object_key_name, format_type="csv"):
    st.write(f"Subiendo datos procesados a Amazon S3: s3://{bucket_name}/{s3_object_key_name}") # Texto Modificado
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
            df_for_s3.to_csv(csv_buffer, index=False, encoding='utf-8-sig', sep=',', quoting=csv.QUOTE_ALL, escapechar='"', na_rep='\\N')
            s3_resource.Object(bucket_name, s3_object_key_name).put(Body=csv_buffer.getvalue().encode('utf-8-sig'))
            st.success(f"¡Éxito! Datos subidos a: s3://{bucket_name}/{s3_object_key_name}") # Texto Modificado
        elif format_type.lower() == "parquet":
            parquet_buffer = io.BytesIO()
            df_for_s3.to_parquet(parquet_buffer, index=False, engine='pyarrow')
            s3_resource.Object(bucket_name, s3_object_key_name).put(Body=parquet_buffer.getvalue())
            st.success(f"¡Éxito! Datos subidos como Parquet a: s3://{bucket_name}/{s3_object_key_name}") # Texto Modificado
        else:
            st.error(f"Formato '{format_type}' no soportado para subida a S3.")
            return False
        return True
    except Exception as e:
        st.error(f"Fallo al subir datos a S3 (s3://{bucket_name}/{s3_object_key_name}): {e}") # Texto Modificado
        st.exception(e)
        return False

# --- Interfaz de Streamlit (TEXTOS MODIFICADOS) ---
st.set_page_config(page_title="Carga de Datos - Plataforma de Análisis del Mercado Laboral Tecnológico", layout="wide", initial_sidebar_state="expanded")

# Puedes añadir un logo si quieres:
# st.image("tu_logo.png", width=150) 

st.title("📊 Plataforma de Análisis del Mercado Laboral Tecnológico")
st.header("Módulo de Carga y Preparación de Datos de Ofertas")

st.markdown("""
Bienvenido al módulo para alimentar nuestra **Plataforma de Análisis del Mercado Laboral Tecnológico**. 
Aquí puedes subir tu archivo CSV con las últimas ofertas laborales. 
Nosotros nos encargaremos de procesarlo, estandarizarlo y prepararlo para que 
puedas descubrir insights valiosos sobre el mercado TI.
""")
st.markdown("---")


AWS_ACCESS_KEY_ID_LOADED = st.secrets.get("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY_LOADED = st.secrets.get("AWS_SECRET_ACCESS_KEY", "")
S3_BUCKET_NAME_FROM_SECRET = st.secrets.get("S3_PROCESSED_BUCKET", "")
S3_OBJECT_PREFIX_FROM_SECRET = st.secrets.get("S3_OBJECT_PREFIX", "ofertas_limpias/")
S3_FILE_FORMAT_FROM_SECRET = st.secrets.get("S3_FILE_FORMAT", "csv")

s3_bucket_to_use = S3_BUCKET_NAME_FROM_SECRET
if not s3_bucket_to_use:
    st.sidebar.error("⚠️ **Configuración Incompleta:** El nombre del Bucket S3 (`S3_PROCESSED_BUCKET`) no está definido en los secretos de la aplicación. La subida de datos no funcionará.")
    st.stop()
else:
    st.sidebar.success(f"✔️ Bucket S3 Destino: **{s3_bucket_to_use}**")
    st.sidebar.markdown(f"📁 **Prefijo en S3:** `{S3_OBJECT_PREFIX_FROM_SECRET}`")
    st.sidebar.markdown(f"📄 **Formato de Salida S3:** `{S3_FILE_FORMAT_FROM_SECRET.upper()}`")

uploaded_file = st.file_uploader(
    "📂 **Paso 1:** Sube tu archivo CSV de Ofertas Laborales", 
    type="csv",
    help="El archivo debe estar delimitado por punto y coma (;) y codificado en UTF-8."
)

if uploaded_file is not None:
    st.write("---")
    st.subheader("📄 Previsualización del Archivo Original (primeras 5 filas):")
    try:
        df_original = pd.read_csv(uploaded_file, sep=';', encoding='utf-8-sig', dtype=str, keep_default_na=False, na_values=[''])
        df_original = df_original.fillna('')
        st.dataframe(df_original.head())

        st.write("---")
        if st.button("🚀 **Paso 2:** Procesar Datos y Enviar a la Plataforma", help="Esto limpiará los datos y los subirá a Amazon S3 para análisis"):
            if not AWS_ACCESS_KEY_ID_LOADED or not AWS_SECRET_ACCESS_KEY_LOADED:
                 st.error("❌ **Error de Configuración:** Credenciales de AWS (`AWS_ACCESS_KEY_ID` o `AWS_SECRET_ACCESS_KEY`) no encontradas en los secretos de la aplicación. No se puede subir a S3.")
            else:
                with st.spinner('⚙️ Procesando y preparando tus datos... Por favor, espera.'):
                    df_procesado_y_ordenado = procesar_dataframe(df_original.copy())
                
                if not df_procesado_y_ordenado.empty and not (df_procesado_y_ordenado.isnull().all().all() if isinstance(df_procesado_y_ordenado, pd.DataFrame) else True) :
                    st.subheader("📊 Vista Previa de Datos Procesados y Ordenados (primeras 5 filas):")
                    st.dataframe(df_procesado_y_ordenado.head())
                    st.success("✅ ¡Datos procesados y listos para ser enviados!")

                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    base_filename_original = "datos_ofertas" # Nombre base más genérico
                    if hasattr(uploaded_file, 'name') and uploaded_file.name:
                        base, _ = os.path.splitext(uploaded_file.name)
                        base_filename_original = f"{base}_procesado"
                    
                    final_prefix = S3_OBJECT_PREFIX_FROM_SECRET.strip()
                    if final_prefix and not final_prefix.endswith('/'): final_prefix += '/'
                    s3_object_name_final = f"{final_prefix}{base_filename_original}_{timestamp}.{S3_FILE_FORMAT_FROM_SECRET}"
                    
                    upload_successful = upload_df_to_s3(df_procesado_y_ordenado, s3_bucket_to_use, s3_object_name_final, format_type=S3_FILE_FORMAT_FROM_SECRET)

                    if upload_successful:
                        st.balloons()
                        st.markdown("---")
                        st.info("ℹ️ Como opción, también puedes descargar una copia local del archivo procesado:")
                        csv_limpio_bytes_local = convert_df_to_csv_for_download(df_procesado_y_ordenado)
                        if csv_limpio_bytes_local:
                            nombre_archivo_descarga = f"{base_filename_original}_{timestamp}_descarga_local.csv"
                            st.download_button(
                                label="📥 Descargar Copia Local (CSV Procesado)",
                                data=csv_limpio_bytes_local,
                                file_name=nombre_archivo_descarga,
                                mime="text/csv",
                            )
                else:
                    st.error("❌ El procesamiento del DataFrame resultó en datos vacíos o hubo un error. Revisa las advertencias si las hubo y el formato de tu archivo de entrada.")
            
    except Exception as e:
        st.error(f"❌ Ocurrió un error inesperado al manejar el archivo: {e}")
        st.exception(e)
else:
    st.info("👋 **¡Bienvenido!** Para comenzar, por favor sube un archivo CSV con los datos de ofertas laborales.")

st.write("---")
st.markdown(f"<div style='text-align: center; color: grey;'>© {datetime.now().year} Plataforma de Análisis del Mercado Laboral Tecnológico</div>", unsafe_allow_html=True)