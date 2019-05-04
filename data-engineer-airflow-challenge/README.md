# tempus_challenge_dags

Data pipeline that fetches data from News API, transforms the data into a 
tabular structure, and stores the transformed data on Amazon S3. 


===========================
DESIGN OVERVIEW
===========================
Application makes use of News API and corresponding calls to gather all 
English news sources, retrieve each source's top headlines, and craft an 
aggregated csv file on a per-source basis. These csv's are then uploaded 
to an S3 bucket (per source) for storage.

While a class object was not exactly necessary for the current objectives, 
it was chosen for convenient extensibility and potential for customization 
in output. It also provides easy centralized state management.

Native Python logging was added for convenient debugging. 

Virtualenv coupled with Docker was utilized for containerization.


===========================
PROGRAM LANGUAGE
===========================
Application was written in Python as speed is not a major concern and 
no need for low-level functionality.

OOP is well-suited for data structure design and ETL, especially where 
strict typing is not of drastic concern.

Selected language allowed for a reduced dev time and greater readability 
and maintainability.


===========================
INSTALLATION
===========================
Unzip codebase and cd to "tempus" directory.
Instantiate virtualenv by calling "source tempus/bin/activate".
Requires Python 3.6.
No compilation necessary and third party dependencies present in requirements.txt
so as to be containerized.


===========================
EXECUTE
===========================
From within the "tempus/challenges/data-engineer-airflow-challenge" directory:

To init Airflow app, run "make run".
To manually trigger jobs, simply press the "execute" button on the desired Airflow job.
To execute unittests, run "python -m unittest discover tests"
