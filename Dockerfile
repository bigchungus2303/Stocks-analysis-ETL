FROM quay.io/astronomer/astro-runtime:11.3.0

# USER root
# COPY ./dbt_project ./dbt_project
# COPY --chown=astro:0 . .

# install dbt into a virtual environment
RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir -r dbt_project/dbt-requirements.txt && \
    pip install --no-cache-dir pyiceberg && \
    cd dbt_project && dbt deps && cd .. && \
    deactivate
