<p align="center">
<br>
<img src="img/logo_datos.JPG" width= "10%" height= "10%" alt="logo">
<br>
<strong> Sistema de extraccion de datos de Importacion/Exportacion Chilenas </strong>
</p>

Programa en Python y SQL para extraer los registros de importación y exportación desde el Servicio Nacional de Aduanas en <a href="https://datos.gob.cl/organization/servicio_nacional_de_aduanas"> datos.gob.cl </a> y Codigos en <a href="http://comext.aduana.cl:7001/codigos/buscar.do"> aduana.cl </a>.


## Prerequisitos
The project code utilizes the following library:
* [Python](https://www.python.org/) v3.8.6
* [PostgreSQL](https://www.enterprisedb.com/downloads/postgres-postgresql-downloads) v13.3





* [Flask](https://flask.palletsprojects.com/en/2.0.x/) v2.0.1
* [pyaarlo](https://github.com/twrecked/pyaarlo) v0.8.0a6
* [azure.storage.blob](https://docs.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-python) v12.8.1
* [pyodbc](https://pypi.org/project/pyodbc/) v4.0.31

## Code
This project is based on

https://docs.microsoft.com/en-us/azure/developer/python/tutorial-deploy-app-service-on-linux-01


Deploy Python apps to Azure App Service on Linux from Visual Studio Code



3.- Change the password of the username 'postgres' in the postgres DB with ```ALTER USER postgres WITH PASSWORD 'abc123';```. Create a username called 'opexapp' (with password 'abc123') in Login/Group Roles. Give all possible privileges.

The following are some of the arguments used by `master_db.py`:



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
