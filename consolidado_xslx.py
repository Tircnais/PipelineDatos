import os
import sys
import pandas as pd
import logging
import warnings
import glob
from datetime import datetime
from pathlib import Path

# Oculta advertencias lectura, ocasionados por "No tener formatos visuales"
warnings.filterwarnings("ignore", category=UserWarning)
# Crear el objeto logger que usarás en todo el script
logger = logging.getLogger(__name__)

# Configuración básica para consola y archivo
def configurar_logging(ruta_logs):
    if not os.path.exists(ruta_logs):
        os.makedirs(ruta_logs)
        
    fecha_hoy = datetime.now().strftime('%Y%m%d')
    archivo_log = os.path.join(ruta_logs, f"registro_{fecha_hoy}.log")

    logging.basicConfig(
        level=logging.INFO,
        # format='%(asctime)s\t[%(levelname)s]\t%(message)s',
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(archivo_log), # Guarda en archivo
            logging.StreamHandler(sys.stdout) # Muestra en consola
        ]
    )
    

def normalizar_columnas(df):
    df = df.rename(columns={
        "id": "id",
        "ID": "id",
        "identificador": "id",
        "nombre": "nombres",
        "nombres": "nombres",
        "apellido": "apellidos",
        "apellidos": "apellidos",
        "telefono": "telefono",
        "telef": "telefono",
        "teléfono": "telefono",
        "dirección": "direccion",
        "direc": "direccion",
        "fecha gestion": "fecha_gestion"
    })
    logger.debug(f"Fin de normalizado de columnas")
    return df

# 1. Extraccion. Funciones de utilidad y lógica
def cargar_archivos(ruta):
    """Carga de archivos.
    Uso de la ruta para buscar archivos Excel que cumplan un determinado nombre y extension

    Args:
        ruta (string): Directorio o ruta de archivos a explorar

    Returns:
        array: Lista de archivos EXCEL encontrados con el patron 'Reporte_Cartera_' y con la extension 'xlsx'.
    """
    # Validar existencia del directorio
    if not os.path.exists(ruta):
        logger.error(f"La ruta no existe o es inaccesible: {ruta}")
        return None
    logger.info(f"Escaneando directorio: {ruta}")
    # Definir el patrón de búsqueda
    patron = os.path.join(ruta, "Reporte_Cartera_*.xlsx")
    archivos = glob.glob(patron)
    if not archivos:
        logger.warning(f"No se encontraron archivos que coincidan con 'Reporte_Cartera_*.xlsx' en {ruta}")
    else:
        logger.info(f"Se encontraron {len(archivos)} archivos para procesar.")
    return archivos

# 2. Transformacion de datos
def procesar_datos(archivos):
    """Procesa y transformacion de datos
    Manejo y limpieza de datos.
    
    Args:
        archivos (array): Lista de archivos

    Returns:
        dataframe: Datos manejados con pandas
    """
    dataframes = []
    for f in archivos:
        # A. Usar calamine para mayor velocidad de lectura
        # B. Usar usecols si no necesitas todas las columnas del Excel
        df_temp = pd.read_excel(f, engine="calamine", dtype=str)
        df_temp = normalizar_columnas(df_temp)
        # Reemplazo de print por logger.info
        logger.info(f"Leído: {Path(f).name} | Registros: {len(df_temp)}")
        dataframes.append(df_temp)
    # C. Concatenar primero
    df = pd.concat(dataframes, ignore_index=True)
    # D. Optimización de memoria: Eliminar duplicados ANTES de ordenar 
    df.drop_duplicates(inplace=True)
    # Convertir a datetime solo para ordenar si es necesario, 
    # o asegurar que el sort_values no sea sobre strings pesados
    df.sort_values("fecha_gestion", inplace=True)
    logger.info(f"✅ {len(archivos)} archivos. Consolidación finalizada: {len(df)} filas únicas.")
    return df

# 3. Carga
def crear_archivos(dataFrame):
    """_summary_
    Retorna un diccionario con el estado y detalles del proceso
    
    Args:
        dataFrame (dataframe): Datos que van a crear el archivo.

    Returns:
        dict: Diccionario para manejo de respuestas
    """
    # Inicializar diccionario vacío con los atributos
    salida = {'estado': '', 'mensaje': ''}
    # print(f"DataFrame tiene: {len(dataFrame)} filas y {len(dataFrame.columns)} columnas")
    if len(dataFrame) > 0:
        fechaActual = datetime.now().strftime('%Y%m%d%H%M%S')
        nombre_archivo = f"Reporte_ConsolidadoCarteras_{fechaActual}.xlsx"
        
        try:
            # Guardar el dataframe
            # dataFrame.to_excel(nombre_archivo, index=False)
            # Eliminamos 'constant_memory' para asegurar integridad de todas las columnas
            with pd.ExcelWriter(nombre_archivo, engine='xlsxwriter') as writer:
                dataFrame.to_excel(writer, index=False, sheet_name='Consolidado')
            
            logger.info(f"✅ {nombre_archivo} Archivo consolidado con éxito usando xlsxwriter.")
            # Verificar si realmente existe
            if os.path.exists(nombre_archivo):
                ubicacionArch: str = os.path.abspath(nombre_archivo)
                tamArch = os.path.getsize(nombre_archivo)
                logger.info(f"📁 Ubicación: {ubicacionArch}")
                logger.info(f"📊 Tamaño: {tamArch} bytes")
                salida['estado'] = 'Ok'
                salida['mensaje'] = 'Archivo creado exitosamente'
                salida['archivo'] = nombre_archivo
                salida['ruta'] = ubicacionArch
                salida['tamaño_bytes'] = tamArch
                salida['filas'] = len(dataFrame)
                salida['columnas'] = len(dataFrame.columns)
            else:
                salida['estado'] = 'Error'
                salida['mensaje'] = 'El archivo no se encuentra después de guardar'
                salida['archivo'] = nombre_archivo
                logger.error(f"❌ El archivo no se encuentra después de guardar")
        except Exception as e:
            salida['estado'] = "Error"
            salida['mensaje'] = f'Error al guardar con xlsxwriter: {str(e)}'
            logger.error(f"{salida['mensaje']}")
    else:
        salida['estado'] = f"❌ No hay datos para guardar"
        salida['mensaje'] = "Dataframe sin datos para guardar"
        salida['filas'] = len(dataFrame)
        logger.error(salida['estado'])
    return salida

def main():
    # Inicializar el sistema de logs
    configurar_logging('logs')
    logger.info("--- Iniciando seguimiento de consolidación ---")
    
    directorio = r"E:\Documentos\Trabajo\ReportesCarteras"
    
    # 4. Lógica del negocio
    listaArchivos = cargar_archivos(directorio)
    
    # Inicializar diccionario vacío con los atributos
    diccionario = {
        'estado': '',
        'mensaje': ''
    }
    
    if listaArchivos:
        df = procesar_datos(listaArchivos)
        resultado = crear_archivos(df)
        logger.info(f"Reporte por creacion del archivo.")
        # Usando conversión explícita
        logger.info(str(resultado))
        if resultado['estado'] == 'Ok':
            diccionario['estado'] = resultado['estado']
            diccionario['mensaje'] = resultado['mensaje']
            logger.info(f"✅ ÉXITO {resultado['mensaje']} 📁 Archivo: {resultado['archivo']}")
        elif resultado['estado'] == 'Error':
            diccionario['estado'] = resultado['estado']
            diccionario['mensaje'] = resultado['mensaje']
            logger.error(f"❌ Error {resultado['mensaje']}")
        else:
            diccionario['estado'] = resultado['estado']
            diccionario['mensaje'] = resultado['mensaje']
            logger.error(f"❌ FALLO: {resultado['mensaje']}")
    else:
        diccionario['estado'] = "sin_datos"
        diccionario['mensaje'] = "No se encontró archivos Excel."
        logger.warning(f"⚠️ No se encontraron archivos en la ruta: {directorio}")        
    
if __name__ == "__main__":
    main()
