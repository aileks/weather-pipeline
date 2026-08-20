{#
  Standardized value; a zero standard deviation yields null (no row) rather
  than an infinity (docs/anomaly-detection.md).
#}
{% macro z_score(value, mean, stddev) %}
({{ value }} - {{ mean }}) / nullif({{ stddev }}, 0)
{% endmacro %}
