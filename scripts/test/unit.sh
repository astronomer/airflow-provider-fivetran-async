pytest \
    -vv \
    --cov-report=xml \
    -m "not (integration)" \
  --ignore=tests/integrations
