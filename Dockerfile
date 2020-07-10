FROM supervisely/base-py:6

RUN pip install xlrd 
RUN pip install google-cloud-storage

RUN python -m pip install git+https://github.com/supervisely/supervisely.git