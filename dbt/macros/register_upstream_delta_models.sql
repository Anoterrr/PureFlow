{#-
    Each medallion layer is a separate dbt invocation (see src/orchestration.py),
    and every invocation gets a fresh in-memory DuckDB, so a ref() to the previous
    layer finds nothing in the catalog. This registers every upstream model that
    is not part of the current selection as a view over its Delta table, so the
    next layer reads exactly the table its post-write gate validated.

    Same idea as dbt-duckdb's register_upstream_external_models(), which reads
    the staged Parquet file instead of the Delta table.
-#}
{% macro register_upstream_delta_models() %}
    {% if execute %}
        {% set registered = [] %}
        {% for node_id in selected_resources %}
            {% set node = graph.nodes.get(node_id) %}
            {% for upstream_id in (node.depends_on.nodes if node else []) %}
                {% set upstream = graph.nodes.get(upstream_id) %}
                {% if upstream
                    and upstream.resource_type == 'model'
                    and upstream_id not in selected_resources
                    and upstream_id not in registered
                    and upstream.config.get('delta_table_path') %}
                    {% do registered.append(upstream_id) %}
                    {% set rel = api.Relation.create(
                        database=upstream.database,
                        schema=upstream.schema,
                        identifier=upstream.alias
                    ) %}
                    {% call statement('register_' ~ upstream.name) %}
                        create schema if not exists {{ rel.without_identifier() }};
                        create or replace view {{ rel }} as
                        select * from delta_scan('{{ upstream.config.delta_table_path }}');
                    {% endcall %}
                {% endif %}
            {% endfor %}
        {% endfor %}
        {#- The hook's transaction is rolled back unless committed here. -#}
        {% if registered %}
            {% do adapter.commit() %}
        {% endif %}
    {% endif %}
{% endmacro %}
