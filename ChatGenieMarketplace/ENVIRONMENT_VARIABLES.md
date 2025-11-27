# Variables de Entorno Requeridas

Este documento describe las variables de entorno necesarias para ejecutar la aplicación Conversational Agent.

## Variables Requeridas

### DATABRICKS_SERVICE_TOKEN
- **Descripción**: Token de servicio de Databricks (Personal Access Token o Service Principal Token)
- **Requerido**: Sí
- **Ejemplo**: `dapi2326d2c11660089b065059bfca600c45`
- **Cómo obtenerlo**:
  - Para PAT: User Settings → Access Tokens → Generate New Token
  - Para Service Principal: Crear un Service Principal y generar un token
- **Permisos necesarios**: 
  - Acceso al Genie Space (al menos `CAN_RUN`)
  - Acceso a Unity Catalog si se consultan tablas
  - Acceso al Serving Endpoint (al menos `CAN_QUERY`)

### DATABRICKS_HOST
- **Descripción**: Host de tu workspace de Databricks
- **Requerido**: Sí
- **Formato**: `adb-{workspace-id}.{deployment-number}.azuredatabricks.net` o `{workspace-name}.cloud.databricks.com`
- **Ejemplo**: `adb-984752964297111.11.azuredatabricks.net`
- **Cómo obtenerlo**: Se encuentra en la URL de tu workspace de Databricks

### GENIE_SPACE
- **Descripción**: ID del Genie Space que se utilizará para las conversaciones
- **Requerido**: Sí (para funcionalidad completa)
- **Ejemplo**: `01f0b5d1fb9c1a6195e96c5a74c6c78b`
- **Cómo obtenerlo**: 
  - Navega a tu Genie Space en Databricks
  - El ID está en la URL o en la configuración del espacio

### SERVING_ENDPOINT_NAME
- **Descripción**: Nombre del serving endpoint utilizado para generar insights
- **Requerido**: No (opcional, solo necesario para funcionalidad de insights)
- **Ejemplo**: `databricks-gpt-5`
- **Cómo obtenerlo**: 
  - Navega a Serving Endpoints en Databricks
  - Copia el nombre del endpoint que deseas usar

## Configuración en app.yaml

Para desplegar la aplicación en Databricks Apps, configura estas variables en el archivo `app.yaml`:

```yaml
env:
- name: "DATABRICKS_SERVICE_TOKEN"
  value: "TU_TOKEN_AQUI"
- name: "DATABRICKS_HOST"
  value: "TU_HOST_AQUI"
- name: "GENIE_SPACE"
  value: "TU_GENIE_SPACE_ID_AQUI"
- name: "SERVING_ENDPOINT_NAME"
  value: "TU_SERVING_ENDPOINT_AQUI"
```

## Configuración Local (.env)

Para desarrollo local, crea un archivo `.env` en la raíz del proyecto:

```bash
DATABRICKS_SERVICE_TOKEN=tu_token_aqui
DATABRICKS_HOST=tu_host_aqui
GENIE_SPACE=tu_genie_space_id_aqui
SERVING_ENDPOINT_NAME=tu_serving_endpoint_aqui
```

**Importante**: El archivo `.env` está en `.gitignore` y no debe subirse al repositorio.

## Configuración en Databricks Apps UI

Alternativamente, puedes configurar las variables de entorno directamente en la interfaz de Databricks Apps:

1. Ve a la página de detalles de tu aplicación
2. Haz clic en "Edit" o "Configuration"
3. Navega a la sección "Environment Variables"
4. Agrega cada variable con su valor correspondiente

## Verificación

Para verificar que las variables están configuradas correctamente, puedes ejecutar:

```python
import os
from dotenv import load_dotenv

load_dotenv()

print(f"DATABRICKS_HOST: {os.environ.get('DATABRICKS_HOST')}")
print(f"GENIE_SPACE: {os.environ.get('GENIE_SPACE')}")
print(f"SERVING_ENDPOINT_NAME: {os.environ.get('SERVING_ENDPOINT_NAME')}")
print(f"DATABRICKS_SERVICE_TOKEN: {'***' if os.environ.get('DATABRICKS_SERVICE_TOKEN') else 'NO CONFIGURADO'}")
```

