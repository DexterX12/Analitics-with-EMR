#!/usr/bin/python3
import requests
from datetime import datetime
import subprocess
import os
import boto3 # Necesario para S3
from botocore.exceptions import NoCredentialsError, ClientError
from cluster_steps import run_steps

DATA_URL = "https://databank.worldbank.org/data/download/WDI_CSV.zip"

PATH_TO_DOWNLOADED_FILE = "/home/ubuntu/saved_data/data.zip"

# Configuración de S3
S3_BUCKET_NAME = "datasetproject3"
S3_DESTINATION_PREFIX = "raw/OpenBank/world_development_indicator/"

def download_data(url, file_name_with_path):
    """Descarga un archivo."""
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{current_time}] Iniciando descarga...")

    save_directory = os.path.dirname(file_name_with_path)
    try:
        if save_directory:
            os.makedirs(save_directory, exist_ok=True)
            print(f"[{current_time}] Directorio '{save_directory}' asegurado/creado.")
        else:
            save_directory = "." 
            print(f"[{current_time}] No se especificó directorio, se usará CWD: '{os.getcwd()}'")
    except OSError as e:
        print(f"[{current_time}] Error al crear directorio '{save_directory}': {e.strerror}")
        return False

    try:
        print(f"[{current_time}] Descargando desde: {url} -> {file_name_with_path}")
        response = requests.get(url, stream=True, timeout=180) 
        response.raise_for_status()
        with open(file_name_with_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=81920):
                if chunk:
                    f.write(chunk)
        print(f"[{current_time}] Archivo descargado exitosamente como '{file_name_with_path}'")
        return True
    except requests.exceptions.RequestException as err:
        print(f"[{current_time}] Error durante la descarga: {err}")
    except IOError as e:
        print(f"[{current_time}] Error de E/S al guardar el archivo: {e}")
    return False

def unzip_downloaded_file(zip_filepath):
    """Descomprime el archivo ZIP en el mismo directorio donde se encuentra."""
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    extraction_directory = os.path.dirname(zip_filepath)
    if not extraction_directory:
        extraction_directory = "."

    print(f"[{current_time}] Intentando descomprimir '{zip_filepath}' en '{extraction_directory}'...")
    
    comando_unzip = ['unzip', '-o', zip_filepath, '-d', extraction_directory]
    
    try:
        print(f"[{current_time}] Ejecutando comando: {' '.join(comando_unzip)}")
        proceso = subprocess.run(comando_unzip, capture_output=True, text=True, check=False)

        if proceso.returncode == 0 or proceso.returncode == 1: # unzip puede retornar 1 con advertencias
            print(f"[{current_time}] Archivo ZIP procesado en '{extraction_directory}'.")
            return True
        else:
            print(f"[{current_time}] Error al descomprimir. Código: {proceso.returncode}")
            if proceso.stderr: print(f"Error de unzip (stderr):\n{proceso.stderr}")
            if proceso.stdout: print(f"Salida de unzip (stdout):\n{proceso.stdout}")
            return False
    except FileNotFoundError:
         print(f"[{current_time}] Error: 'unzip' no encontrado. Instálalo (sudo apt install unzip).")
    except Exception as e:
        print(f"[{current_time}] Error inesperado durante descompresión: {e}")
    return False

def upload_directory_to_s3(local_directory, bucket_name, s3_prefix):
    """Sube el contenido de un directorio local a S3."""
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{current_time}] Iniciando subida a S3: {local_directory} -> s3://{bucket_name}/{s3_prefix}")
    
    if not os.path.isdir(local_directory):
        print(f"[{current_time}] Error: Directorio local '{local_directory}' no existe.")
        return False

    try:
        s3_client = boto3.client('s3')
    except Exception as e:
        print(f"[{current_time}] Error al inicializar cliente S3 Boto3: {e}. Verifica rol IAM y Boto3.")
        return False

    successful_uploads = 0
    failed_uploads = 0

    for root, _, files in os.walk(local_directory):
        for filename in files:
            local_path = os.path.join(root, filename)
            relative_path = os.path.relpath(local_path, local_directory)
            s3_key = os.path.join(s3_prefix, relative_path).replace("\\", "/")

            # Evitar subir el propio archivo ZIP descargado si está en el mismo directorio de extracción
            if local_path == os.path.join(local_directory, os.path.basename(PATH_TO_DOWNLOADED_FILE)): # Compara con la ruta completa del zip
                 print(f"[{current_time}] Omitiendo subida del archivo ZIP original: '{local_path}'")
                 continue

            try:
                print(f"[{current_time}] Subiendo '{local_path}' a 's3://{bucket_name}/{s3_key}'...")
                s3_client.upload_file(local_path, bucket_name, s3_key)
                successful_uploads += 1
            except Exception as e:
                print(f"[{current_time}] Error al subir '{local_path}': {e}")
                failed_uploads += 1
    
    print(f"[{current_time}] Subida a S3 finalizada. Subidos: {successful_uploads}, Fallos: {failed_uploads}")
    return failed_uploads == 0

if __name__ == "__main__":
    main_start_time = datetime.now()
    print(f"[{main_start_time.strftime('%Y-%m-%d %H:%M:%S')}] === INICIO DEL SCRIPT ===")

    # 1. Descargar
    if download_data(DATA_URL, PATH_TO_DOWNLOADED_FILE):
        # 2. Descomprimir
        if unzip_downloaded_file(PATH_TO_DOWNLOADED_FILE):
            extraction_dir = os.path.dirname(PATH_TO_DOWNLOADED_FILE)
            if not extraction_dir: extraction_dir = "." # Por si acaso

            # 3. Subir a S3 el contenido del directorio de extracción
            if upload_directory_to_s3(extraction_dir, S3_BUCKET_NAME, S3_DESTINATION_PREFIX):
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Proceso completo: Descarga, descompresión y subida a S3 exitosos.")
            else:
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Falló la subida a S3.")

        else:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Falló la descompresión.")
    else:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Falló la descarga.")
    

    run_steps() # Crea el cluster si este ya no existe y ejecuta los scripts

    main_end_time = datetime.now()
    print(f"[{main_end_time.strftime('%Y-%m-%d %H:%M:%S')}] === FIN DEL SCRIPT (Duración: {main_end_time - main_start_time}) ===")