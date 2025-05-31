from flask import Flask, jsonify, request
import pandas as pd

app = Flask(__name__)

S3_BUCKET_NAME = "datasetproject3"
S3_OBJECT_KEY = "refined/"
COUNTRY_COLUMN_NAME = "Country"

# Variable global para el DataFrame (o cargar bajo demanda con caché)
df_full = None

def load_data():
    global df_full
    if df_full is None:
        s3_path = f"s3://{S3_BUCKET_NAME}/{S3_OBJECT_KEY}"
        print(f"Cargando datos desde: {s3_path}")
        try:
            df_full = pd.read_parquet(s3_path)
            if COUNTRY_COLUMN_NAME not in df_full.columns:
                print(f"Error: La columna '{COUNTRY_COLUMN_NAME}' no se encuentra en el Parquet.")
                df_full = None # Marcar como no cargado si hay error
                return False
            print("Datos cargados exitosamente en memoria.")
            return True
        except Exception as e:
            print(f"Error al cargar datos desde S3: {e}")
            df_full = None
            return False
    return True # Ya estaba cargado

@app.route('/', methods=["GET"])
def say_hi():
    return jsonify({"message": "hello"}), 200

@app.route('default/available-data-columns', methods=['GET'])
def get_available_data_columns():
    if not load_data() or df_full is None:
        return jsonify({"error": "No se pudieron cargar los datos fuente."}), 500

    all_columns = df_full.columns.tolist()
    data_column_names = [col for col in all_columns if col != COUNTRY_COLUMN_NAME]
    return jsonify({'available_data_columns': data_column_names})

@app.route('/default/all-countries-data-column', methods=['GET'])
def get_all_countries_data_column():
    if not load_data() or df_full is None:
        return jsonify({"error": "No se pudieron cargar los datos fuente."}), 500

    requested_data_column = request.args.get('column')
    if not requested_data_column:
        return jsonify({'error': 'El parámetro "column" es obligatorio.'}), 400

    available_columns = df_full.columns.tolist()
    matched_data_column = None
    for col_name_in_df in available_columns:
        if col_name_in_df.strip().lower() == requested_data_column.strip().lower():
            if col_name_in_df == COUNTRY_COLUMN_NAME:
                return jsonify({'error': f"No especifique '{COUNTRY_COLUMN_NAME}' como columna de datos."}), 400
            matched_data_column = col_name_in_df
            break

    if not matched_data_column:
        data_cols_available = [col for col in available_columns if col != COUNTRY_COLUMN_NAME]
        return jsonify({
            'error': f"La columna de datos '{requested_data_column}' no fue encontrada.",
            'available_data_columns': data_cols_available
        }), 404

    df_to_return = df_full[[COUNTRY_COLUMN_NAME, matched_data_column]]
    result_data = df_to_return.to_dict(orient='records') # to_dict es más directo para Flask
    return jsonify({
        'requested_data_column': matched_data_column,
        'data': result_data
    })

if __name__ == '__main__':
    # Para desarrollo local en EC2 (accede por IP_EC2:5000)
    app.run(host='0.0.0.0', port=80, debug=True)
    # Para producción, usarías un servidor WSGI como Gunicorn:
    # gunicorn --bind 0.0.0.0:8000 app_ec2:app
    pass