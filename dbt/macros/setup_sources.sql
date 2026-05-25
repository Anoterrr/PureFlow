{% macro create_external_sources() %}
    {% if execute %}
        {% do log("🚀 [dbt] Registering external sources...", info=True) %}
        
        {# 1. Ensure S3 credentials and extensions are set GLOBALly in this session #}
        {% set s3_endpoint = env_var('S3_ENDPOINT', 'minio:9000').replace('http://', '').replace('https://', '') %}
        {% if 'localhost' in s3_endpoint %}
            {% set s3_endpoint = s3_endpoint.replace('localhost', '127.0.0.1') %}
        {% endif %}
        
        {% set s3_user = env_var('STORAGE_USER', 'admin') %}
        {% set s3_pass = env_var('STORAGE_PASSWORD', 'strongpassword123') %}

        {% set setup_sql %}
            INSTALL httpfs; LOAD httpfs;
            INSTALL json; LOAD json;
            INSTALL delta; LOAD delta;
            
            -- Use modern SECRET syntax for reliable extension discovery
            CREATE OR REPLACE SECRET minio_secret (
                TYPE S3,
                KEY_ID '{{ s3_user }}',
                SECRET '{{ s3_pass }}',
                ENDPOINT '{{ s3_endpoint }}',
                URL_STYLE 'path',
                USE_SSL false,
                REGION 'us-east-1'
            );
        {% endset %}
        
        {% do log("🚀 [dbt] Configuring S3 Endpoint: " ~ s3_endpoint, info=True) %}
        {% do run_query(setup_sql) %}

        {# 2. Register Sources as Views #}
        {% for source in graph.sources.values() %}
            {% set ext_loc = None %}
            {% if source.meta and source.meta.get('s3_path') %}
                {% set ext_loc = source.meta.get('s3_path') %}
            {% endif %}
            
            {% if ext_loc %}
                {% set schema_name = source.schema %}
                {% set table_name = source.name %}
                
                {# Properly render the location string (evaluates any Jinja tags inside) #}
                {% set loc = render(ext_loc) %}
                {% set is_delta = 'dt=' in loc or 'delta' in loc %}
                
                {# Delta paths often require trailing slashes for listing #}
                {% if is_delta and not loc.endswith('/') %}
                    {% set loc = loc ~ '/' %}
                {% endif %}

                {% set read_func = "delta_scan" if is_delta else "read_parquet" %}

                {% set registration_sql %}
                    CREATE SCHEMA IF NOT EXISTS {{ schema_name }};
                    CREATE OR REPLACE VIEW {{ schema_name }}.{{ table_name }} AS 
                    SELECT * FROM {{ read_func }}('{{ loc }}');
                {% endset %}
                
                {% do log("📦 [dbt] Registering source: " ~ schema_name ~ "." ~ table_name ~ " at " ~ loc, info=True) %}
                {% do run_query(registration_sql) %}
            {% endif %}
        {% endfor %}
    {% endif %}
{% endmacro %}
