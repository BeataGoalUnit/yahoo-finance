# YAHOO FINANCE REAL TIME

## HEJ CALLE ヾ( ˃ᴗ˂ )◞ • *✰
* **dags_yahoo-finance-pipeline.py** är den filen som ska till Airflow
* De delar jag är osäker på eller som behöver åtgärd (vad jag vet) har **TODO** kommentarer
* local_dag fungerar mot lokal databas (connection genom local_db_connection). Skapar nya tables om de inte finns (ev. lyfta ut denna funktionalitet).
* dags_cloudsqlmigration_postgres är filen från dagsfolder som har db connection, la in en ny metod där man kan köra queries och DDLs.