import joblib
import os
import pandas as pd

# Se define cómo cargar el modelo
def model_fn(model_dir):
    model_path = os.path.join(model_dir, "random_forest_model_trained.pkl")
    model = joblib.load(model_path)
    return model

# Se define cómo se recibe la entrada
def input_fn(request_body, request_content_type):
    if request_content_type == 'text/csv':
        return pd.read_csv(io.StringIO(request_body))
    else:
        raise ValueError("Unsupported content type: {}".format(request_content_type))

# Se define cómo se generan las predicciones
def predict_fn(input_data, model):
    return model.predict(input_data)

# Se define cómo se devuelve la respuesta
def output_fn(prediction, content_type):
    prediction_str = ','.join(str(x) for x in prediction)
    return prediction_str

