FROM quay.io/astronomer/astro-runtime:11.3.0

# USER root
# COPY ./dbt_project ./dbt_project
# COPY --chown=astro:0 . .

# install dbt into a virtual environment
RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir -r capstone-dbt_2/dbt-requirements.txt && \
    pip install --no-cache-dir pyiceberg && \
    cd capstone-dbt_2 && dbt deps && cd .. && \
    deactivate
