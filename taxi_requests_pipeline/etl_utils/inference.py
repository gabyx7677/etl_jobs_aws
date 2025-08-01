# Se importa la librería estándar de SageMaker para scikit-learn
from sklearn.ensemble import RandomForestRegressor
import joblib
import pandas as pd
import os
import json

# Se define la ruta del modelo guardado en el contenedor
MODEL_PATH = os.path.join('/opt/ml/model', 'random_forest_model_trained.pkl')


# Se carga el modelo previamente guardado
def model_fn(model_dir):
  model = joblib.load(os.path.join(model_dir, 'random_forest_model_trained.pkl'))
  return model


# Se convierte el cuerpo del request en un DataFrame de pandas
def input_fn(request_body, request_content_type):
  if request_content_type == 'application/json':
    input_data = json.loads(request_body)
    df = pd.DataFrame(input_data)
    return df
  else:
    raise ValueError(f"Tipo de contenido no compatible: {request_content_type}")


# Se realiza la predicción con el modelo cargado
def predict_fn(input_data, model):
  return model.predict(input_data)


# Se serializa la respuesta para que sea devuelta como JSON
def output_fn(prediction, response_content_type):
  if response_content_type == 'application/json':
    return json.dumps(prediction.tolist())
  else:
    raise ValueError(f"Tipo de contenido no compatible: {response_content_type}")
