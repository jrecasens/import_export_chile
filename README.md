<p align="center">
<br>
<img src="img/logo_datos.JPG" width= "10%" height= "10%" alt="logo">
<br>
<strong> Sistema de extraccion de datos de Importacion/Exportacion Chilenas </strong>
</p>

Programa en Python y SQL para extraer los registros de importación y exportación desde el Servicio Nacional de Aduanas en <a href="https://datos.gob.cl/organization/servicio_nacional_de_aduanas"> datos.gob.cl </a> y Codigos en <a href="http://comext.aduana.cl:7001/codigos/buscar.do"> aduana.cl </a>.

## Testing

Before deployment to Azure App Service, creating a virtual enviroment is recommended for a succesful local execution test:

### Create Enviroment (named .venv)
    python -m venv .venv

### Activate enviroment
    .venv\scripts\activate

### Install dependencies in .venv
    pip install -r requirements.txt

### Run Flask App
    cd C:/Github/arlo_azure_service
    $env:FLASK_APP = "execute:app"

### Deactivate and Delete (optional)
    deactivate
    rm -r .venv
